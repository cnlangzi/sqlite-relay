package backup

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// Errors of the package.
var (
	// ErrSnapshotRequired reports that the backup has no baseline and
	// that it needs a snapshot of the primary database.
	ErrSnapshotRequired = errors.New("backup: a snapshot is required")
	// ErrClosed reports that the file of the backup database is closed.
	ErrClosed = errors.New("backup: the backup database is closed")
	// ErrPageSize reports that the pages of a transaction do not fit the
	// backup database.
	ErrPageSize = errors.New("backup: the page size does not fit the backup database")
)

// The offsets of the header of page 1 of a SQLite database.
const (
	// headerWriteVersion is the offset of the write version of the file.
	headerWriteVersion = 18
	// headerReadVersion is the offset of the read version of the file.
	headerReadVersion = 19
	// headerChangeCounter is the offset of the change counter of the file.
	headerChangeCounter = 24
	// headerPageCount is the offset of the size of the database in pages.
	headerPageCount = 28
	// headerVersionValidFor is the offset of the number that tells whether
	// the size of the database in the header is valid.
	headerVersionValidFor = 92
	// headerSize is the size of the header of page 1.
	headerSize = 100
)

// applier applies the increments of the chain to the backup database.
//
// While the backup is in sync the applier holds no connection to the backup
// database: it writes the pages of the transactions into the file of the
// backup and it makes the file durable at the boundary of a segment. The
// applied sequence number is persisted with the file, so that an
// acknowledgement is only sent when both of them are durable.
type applier struct {
	// mu guards the state of the applier.
	mu sync.Mutex

	dir       string
	dbPath    string
	statePath string
	verify    bool

	// st is the state of the backup.
	st *state

	// file is the file of the backup database. It is nil while the
	// backup is paused or while it has no baseline.
	file *os.File

	// applied is the sequence number that is applied and persisted.
	applied uint64
	// pending is the sequence number of the last applied transaction. It
	// is persisted with the next flush.
	pending uint64
	// dirty reports that pages were written that are not durable yet.
	dirty bool
	// idle is closed when the applier has nothing to flush. It is nil
	// when the applier is idle.
	idle chan struct{}
	// paused reports that the applier does not write to the backup
	// database.
	paused bool
	// resume is closed when the applier is resumed.
	resume chan struct{}
	// needsSnapshot reports that the backup needs a new baseline.
	needsSnapshot bool
	// epoch is the generation of the chain the source is part of. It is
	// zero while the peer does not know it.
	epoch uint64
	// snap is the snapshot that is on its way to the backup.
	snap *snapshotInstall
	// snapshotAt is the time of the last snapshot that was installed.
	snapshotAt time.Time
	// snapshotBytes is the size of the last snapshot that was installed.
	snapshotBytes int64
	// snapshots is the number of the snapshots that were installed.
	snapshots int
}

// open opens the file of the backup database.
//
// The caller holds the lock of the applier.
func (a *applier) open() error {
	f, err := os.OpenFile(a.dbPath, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("backup: open the backup database: %w", err)
	}
	a.file = f
	return nil
}

// check verifies that the file of the backup database belongs to the backup
// and that nobody else holds it.
//
// A journal, a write-ahead log or a shared memory file next to the backup
// database means that another process has the database open. The relay
// writes the file of the backup database while it is in sync, a process that
// reads it at the same time sees a torn page.
func (a *applier) check() error {
	for _, name := range []string{"-journal", "-wal", "-shm"} {
		if _, err := os.Stat(a.dbPath + name); err == nil {
			return fmt.Errorf("backup: the backup database has a %s file, the connections of the business have to be closed before the relay writes the backup database", name)
		}
	}
	f, err := os.Open(a.dbPath)
	if err != nil {
		return fmt.Errorf("backup: open the backup database: %w", err)
	}
	defer func() { _ = f.Close() }()
	return checkSize(f, a.st.PageSize)
}

// snapshotStats returns the time, the size and the number of the snapshots
// that were installed.
func (a *applier) snapshotStats() (time.Time, int64, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snapshotAt, a.snapshotBytes, a.snapshots
}

// Apply applies a transaction of the chain.
//
// The pages of the transaction are written into the file of the backup
// database. The transaction is durable when the applier is flushed, the
// protocol flushes it at the boundary of a segment.
//
// The applier does not write while it is paused: the call waits for the
// resume.
func (a *applier) Apply(ctx context.Context, tx segment.Tx) error {
	for {
		a.mu.Lock()
		if !a.paused {
			err := a.applyTx(tx)
			a.mu.Unlock()
			return err
		}
		if err := a.waitResumed(ctx); err != nil {
			return err
		}
	}
}

// waitResumed waits until the applier is resumed.
//
// The caller holds the lock of the applier, the lock is released while the
// applier waits. The lock is not taken again: the caller takes it when it
// looks at the state of the applier.
func (a *applier) waitResumed(ctx context.Context) error {
	resume := a.resume
	a.mu.Unlock()
	select {
	case <-resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// applyTx writes the pages of a transaction into the backup database.
//
// The caller holds the lock of the applier.
func (a *applier) applyTx(tx segment.Tx) error {
	if a.file == nil {
		return ErrClosed
	}
	if len(tx.Pages) == 0 {
		return fmt.Errorf("backup: the transaction %d holds no page", tx.Seq)
	}
	size := uint32(len(tx.Pages[0].Data))
	if a.st.PageSize == 0 {
		a.st.PageSize = size
	}
	if a.st.PageSize != size {
		return fmt.Errorf("%w: the pages of the transaction are %d bytes long, the backup holds pages of %d bytes", ErrPageSize, size, a.st.PageSize)
	}
	if tx.CommitPages == 0 {
		return fmt.Errorf("backup: the transaction %d holds no page count", tx.Seq)
	}
	for _, p := range tx.Pages {
		if uint32(len(p.Data)) != size {
			return fmt.Errorf("%w: the page %d holds %d bytes, the page size is %d", ErrPageSize, p.Pgno, len(p.Data), size)
		}
		if p.Pgno == 0 || p.Pgno > tx.CommitPages {
			return fmt.Errorf("backup: the transaction %d holds the page %d, the database has %d pages", tx.Seq, p.Pgno, tx.CommitPages)
		}
		offset := int64(p.Pgno-1) * int64(size)
		if _, err := a.file.WriteAt(p.Data, offset); err != nil {
			return fmt.Errorf("backup: write the page %d: %w", p.Pgno, err)
		}
	}
	if err := a.file.Truncate(int64(tx.CommitPages) * int64(size)); err != nil {
		return fmt.Errorf("backup: cut the backup database: %w", err)
	}
	if err := normalizeHeader(a.file, tx.CommitPages); err != nil {
		return err
	}
	a.pending = tx.Seq
	a.dirty = true
	if a.idle == nil {
		a.idle = make(chan struct{})
	}
	return nil
}

// Flush makes the applied transactions durable.
//
// The file of the backup database is synced and the applied sequence number
// is written to the state file. The state file is the memory of the backup:
// a transaction that is not persisted is applied again after a restart.
func (a *applier) Flush(ctx context.Context) error {
	for {
		a.mu.Lock()
		if !a.paused {
			err := a.flush()
			a.mu.Unlock()
			return err
		}
		if err := a.waitResumed(ctx); err != nil {
			return err
		}
	}
}

// flush makes the applied transactions durable.
//
// The caller holds the lock of the applier.
func (a *applier) flush() error {
	if !a.dirty {
		return nil
	}
	if a.file != nil {
		if err := a.file.Sync(); err != nil {
			return fmt.Errorf("backup: sync the backup database: %w", err)
		}
	}
	a.st.AppliedSeq = a.pending
	if err := writeState(a.statePath, a.st); err != nil {
		return err
	}
	a.applied = a.pending
	a.dirty = false
	if a.idle != nil {
		close(a.idle)
		a.idle = nil
	}
	return nil
}

// AppliedSeq returns the sequence number the backup applied and persisted.
func (a *applier) AppliedSeq(ctx context.Context) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.applied, nil
}

// Epoch returns the generation of the chain the backup is part of.
func (a *applier) Epoch() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.st.Epoch
}

// NeedsSnapshot reports whether the backup needs a baseline.
//
// A paused backup does not ask for a snapshot: the business reads the
// backup database and the file may not be replaced under its feet. The
// backup asks for the snapshot when it is resumed.
func (a *applier) NeedsSnapshot() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.paused {
		return false
	}
	return a.needsSnapshot || a.st.Phase != PhaseReady
}

// Pause stops the applier at the boundary of a segment.
//
// The applier waits for the segment that is applied to end, it makes the
// applied sequence number durable and it closes the file of the backup
// database. The file is not touched any more until the applier is resumed.
//
// The state of the backup is written before the file is closed: a backup
// that is killed in between does not open the database again.
func (a *applier) Pause(ctx context.Context) error {
	for {
		a.mu.Lock()
		if a.idle == nil {
			// Nothing is in flight, the file is durable.
			a.paused = true
			if a.resume == nil {
				a.resume = make(chan struct{})
			}
			a.st.Paused = true
			if err := writeState(a.statePath, a.st); err != nil {
				a.mu.Unlock()
				return err
			}
			err := a.closeFile()
			a.mu.Unlock()
			return err
		}
		idle := a.idle
		a.mu.Unlock()
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Resume opens the file of the backup database again.
//
// The applier checks that the file belongs to the backup: the generation of
// the chain it is part of and the size of the file have to match the state
// of the backup. A business program that still holds the backup database
// leaves a journal behind, the resume refuses to continue then.
//
// The state of the backup is cleared before the file is opened: a backup
// that is killed in between opens the database again after a restart.
func (a *applier) Resume(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		if a.st.Phase != PhaseReady {
			return ErrSnapshotRequired
		}
		if a.epoch != 0 && a.st.Epoch != a.epoch {
			return fmt.Errorf("backup: the epoch of the backup is %d, the epoch of the source is %d", a.st.Epoch, a.epoch)
		}
		if err := a.check(); err != nil {
			return err
		}
		next := *a.st
		next.Paused = false
		if err := writeState(a.statePath, &next); err != nil {
			return err
		}
		a.st.Paused = false
		f, err := os.OpenFile(a.dbPath, os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("backup: open the backup database: %w", err)
		}
		a.file = f
	}
	if a.paused {
		a.paused = false
		close(a.resume)
		a.resume = nil
	}
	return nil
}

// expectEpoch tells the applier the generation of the chain the source is
// part of.
//
// The applier refuses to open the database of the backup when the backup
// belongs to another generation of the chain.
func (a *applier) expectEpoch(epoch uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.epoch = epoch
}

// closeFile closes the file of the backup database.
//
// The caller holds the lock of the applier.
func (a *applier) closeFile() error {
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	if err != nil {
		return fmt.Errorf("backup: close the backup database: %w", err)
	}
	return nil
}

// requireSnapshot tells the applier that its baseline is not valid any more.
//
// The chain of the source is not continuous any more, only a new snapshot
// brings the backup back.
func (a *applier) requireSnapshot() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.needsSnapshot = true
}

// normalizeHeader rewrites the header of page 1 of the backup database.
//
// The pages of the primary database carry the flags of the write-ahead log.
// The backup database is a plain database with a rollback journal: it leaves
// no -wal and no -shm file behind and a reader of the backup does not
// interfere with the writes of the relay. The size of the database in the
// header has to match the size of the file.
func normalizeHeader(f *os.File, pages uint32) error {
	var head [headerSize]byte
	if _, err := f.ReadAt(head[:], 0); err != nil {
		return fmt.Errorf("backup: read the header of the backup database: %w", err)
	}
	head[headerWriteVersion] = 1
	head[headerReadVersion] = 1
	binary.BigEndian.PutUint32(head[headerPageCount:headerPageCount+4], pages)
	// The size of the database in the header is only read when the change
	// counter and the number that tells it is valid are the same.
	binary.BigEndian.PutUint32(head[headerVersionValidFor:headerVersionValidFor+4],
		binary.BigEndian.Uint32(head[headerChangeCounter:headerChangeCounter+4]))
	if _, err := f.WriteAt(head[:], 0); err != nil {
		return fmt.Errorf("backup: write the header of the backup database: %w", err)
	}
	return nil
}

// checkSize checks that the file of the backup database holds the pages the
// header of the database names.
func checkSize(f *os.File, pageSize uint32) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("backup: stat the backup database: %w", err)
	}
	if pageSize == 0 {
		return nil
	}
	if info.Size()%int64(pageSize) != 0 {
		return fmt.Errorf("backup: the backup database is %d bytes long, the page size is %d", info.Size(), pageSize)
	}
	var head [headerSize]byte
	if _, err := f.ReadAt(head[:], 0); err != nil {
		return fmt.Errorf("backup: read the header of the backup database: %w", err)
	}
	pages := binary.BigEndian.Uint32(head[headerPageCount : headerPageCount+4])
	if uint64(pages) != uint64(info.Size()/int64(pageSize)) {
		return fmt.Errorf("backup: the header names %d pages, the file holds %d", pages, info.Size()/int64(pageSize))
	}
	return nil
}
