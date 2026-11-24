package driver

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"time"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// Snapshot is a consistent copy of the database at a given capture position.
type Snapshot struct {
	// Path is the file the snapshot was written to.
	Path string
	// Bytes is the size of the file.
	Bytes int64
	// Pos is the capture position the snapshot is bound to. The increments
	// that follow this position bring the database up to date.
	Pos Position
}

// baseline describes the state of the database file at a point in time.
type baseline struct {
	// changeCounter is the change counter of page 1.
	changeCounter uint32
	// size is the size of the file.
	size int64
	// modTime is the modification time of the file.
	modTime time.Time
}

// Snapshot writes a consistent copy of the database to dst.
//
// The snapshot is the state of the database at the capture position it
// returns. The increments that follow that position bring the database up to
// date without a gap and without a repeat.
//
// The snapshot does not block the business writes. It copies the database
// file and overlays the frames of the WAL that are not in the file yet.
func (d *Driver) Snapshot(ctx context.Context, dst string) (*Snapshot, error) {
	var err error
	for attempt := 0; attempt < d.cfg.SnapshotRetries; attempt++ {
		var snap *Snapshot
		snap, err = d.snapshotOnce(ctx, dst)
		if err == nil {
			return snap, nil
		}
		d.recordSnapshotError(err)
	}
	return nil, fmt.Errorf("driver: snapshot failed after %d attempts: %w", d.cfg.SnapshotRetries, err)
}

// snapshotOnce assembles a single snapshot.
func (d *Driver) snapshotOnce(ctx context.Context, dst string) (*Snapshot, error) {
	// Stop the checkpoint manager. While it is stopped the database file is
	// not written back, so the copy cannot be torn by a checkpoint.
	release := d.pauseCheckpoints()
	defer release()

	// The frames of the WAL and the boundary of the snapshot.
	//
	// The whole WAL is read, not only the frames that are not in the
	// database file yet. Writing a frame that is already in the database
	// file puts the same content back, so the result does not depend on
	// where the backfill stands.
	reader, err := wal.Open(d.walPath)
	if err != nil {
		return nil, fmt.Errorf("driver: open WAL: %w", err)
	}
	defer reader.Close()
	scan, err := reader.Scan()
	if err != nil {
		return nil, fmt.Errorf("driver: read WAL: %w", err)
	}
	boundary := scan.CommitFrame

	// The boundary has to agree with the frame count SQLite reports. A
	// boundary that is ahead of the frame count means the WAL was reset
	// while it was read. A boundary that is behind it means a transaction
	// was committed after the WAL was read, which is harmless: the
	// increments continue from the boundary of the snapshot.
	if log, err := d.walFrameCount(ctx); err == nil && log > 0 {
		if boundary > uint64(log) {
			return nil, fmt.Errorf("driver: the WAL holds %d committed frames, SQLite reports %d", boundary, log)
		}
		if boundary == 0 {
			boundary = uint64(log)
		}
	}
	// The header on disk has to be the one the reader worked with.
	onDisk, err := wal.ReadHeader(d.walPath)
	if err != nil && !errors.Is(err, wal.ErrNoWAL) {
		return nil, err
	}
	if err == nil && (onDisk.Salt1 != reader.Header().Salt1 || onDisk.Salt2 != reader.Header().Salt2) {
		return nil, fmt.Errorf("driver: the WAL was reset while it was read")
	}

	// The pages that are not in the database file yet. A page that the WAL
	// wrote more than once keeps its last version.
	pages := map[uint32][]byte{}
	var commitPages uint32
	for _, tx := range scan.Transactions {
		if tx.CommitFrame > boundary {
			continue
		}
		commitPages = tx.CommitPages
		maps.Copy(pages, pagesOfTransaction(tx))
	}

	// Copy the database file and make sure it did not move while it was
	// copied.
	before, err := readBaseline(d.dbPath)
	if err != nil {
		return nil, err
	}
	if err := copyFile(d.dbPath, dst); err != nil {
		return nil, err
	}
	after, err := readBaseline(d.dbPath)
	if err != nil {
		return nil, err
	}
	if before != after {
		return nil, fmt.Errorf("driver: the database file changed while it was copied")
	}

	// Overlay the pages of the WAL and cut the file to the size the
	// database has after the commit.
	if commitPages == 0 {
		commitPages, err = dbPageCount(dst)
		if err != nil {
			return nil, err
		}
	}
	if err := applyPages(dst, d.dbPath, pages, commitPages, d.pageSize); err != nil {
		return nil, err
	}

	// The snapshot has to be a valid database.
	if err := checkIntegrity(dst); err != nil {
		return nil, err
	}

	info, err := os.Stat(dst)
	if err != nil {
		return nil, err
	}

	pos := Position{
		Salt1:     reader.Header().Salt1,
		Salt2:     reader.Header().Salt2,
		Frame:     boundary,
		WALOffset: walOffset(boundary, d.pageSize),
	}
	d.advanceCursorTo(pos)

	return &Snapshot{Path: dst, Bytes: info.Size(), Pos: pos}, nil
}

// pauseCheckpoints stops the checkpoint manager until the returned function
// is called.
func (d *Driver) pauseCheckpoints() func() {
	d.ckptMu.Lock()
	defer d.ckptMu.Unlock()
	d.ckptPaused = true
	return func() {
		d.ckptMu.Lock()
		defer d.ckptMu.Unlock()
		d.ckptPaused = false
	}
}

// recordSnapshotError remembers why a snapshot attempt failed.
func (d *Driver) recordSnapshotError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastErr = err
}

// readBaseline reads the state of the database file that a copy has to match.
func readBaseline(path string) (baseline, error) {
	info, err := os.Stat(path)
	if err != nil {
		return baseline{}, fmt.Errorf("driver: stat database: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return baseline{}, fmt.Errorf("driver: open database: %w", err)
	}
	defer f.Close()
	// The change counter of page 1 sits at offset 24.
	var head [28]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		return baseline{}, fmt.Errorf("driver: read database header: %w", err)
	}
	return baseline{
		changeCounter: binary.BigEndian.Uint32(head[24:28]),
		size:          info.Size(),
		modTime:       info.ModTime(),
	}, nil
}

// copyFile copies a file.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("driver: open %s: %w", src, err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("driver: create %s: %w", dst, err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("driver: copy %s: %w", src, err)
	}
	return out.Sync()
}

// applyPages writes the pages of the WAL over the copy of the database file
// and cuts the file to the size of the database.
//
// A page that the database grew into and that is not in the WAL is taken
// from the database file itself.
func applyPages(path, source string, pages map[uint32][]byte, commitPages uint32, pageSize int) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("driver: open snapshot: %w", err)
	}
	defer f.Close()
	for pgno, data := range pages {
		offset := int64(pgno-1) * int64(pageSize)
		if _, err := f.WriteAt(data, offset); err != nil {
			return fmt.Errorf("driver: write page %d: %w", pgno, err)
		}
	}
	// The database has commitPages pages after the commit. Pages beyond that
	// belong to a database that shrank.
	size := int64(commitPages) * int64(pageSize)
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("driver: stat snapshot: %w", err)
	}
	if info.Size() < size {
		if err := copyMissingPages(f, source, info.Size(), size, pageSize); err != nil {
			return err
		}
	}
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("driver: cut snapshot: %w", err)
	}
	return f.Sync()
}

// copyMissingPages copies the pages the snapshot is missing from the
// database file.
func copyMissingPages(dst *os.File, source string, from, to int64, pageSize int) error {
	src, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("driver: open %s: %w", source, err)
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return fmt.Errorf("driver: stat %s: %w", source, err)
	}
	if info.Size() <= from {
		// The database file does not hold the pages either, the snapshot
		// gets zeroed pages and the integrity check reports the problem.
		return nil
	}
	buf := make([]byte, pageSize)
	for offset := from; offset < to; offset += int64(pageSize) {
		n, err := src.ReadAt(buf, offset)
		if n == 0 {
			if err != nil {
				return fmt.Errorf("driver: read %s: %w", source, err)
			}
			break
		}
		if _, err := dst.WriteAt(buf[:n], offset); err != nil {
			return fmt.Errorf("driver: write snapshot: %w", err)
		}
	}
	return nil
}

// dbPageCount returns the number of pages the database file holds, as the
// header of page 1 reports it.
func dbPageCount(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("driver: open %s: %w", path, err)
	}
	defer f.Close()
	var head [32]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		return 0, fmt.Errorf("driver: read %s: %w", path, err)
	}
	pages := binary.BigEndian.Uint32(head[28:32])
	if pages == 0 {
		// The header of an empty database reports no pages. Its size is
		// one page.
		return 1, nil
	}
	return pages, nil
}

// checkIntegrity verifies that a snapshot is a valid database.
func checkIntegrity(path string) error {
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000")
	if err != nil {
		return fmt.Errorf("driver: open snapshot: %w", err)
	}
	defer db.Close()
	var result string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("driver: integrity_check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("driver: integrity_check reported %q", result)
	}
	return nil
}
