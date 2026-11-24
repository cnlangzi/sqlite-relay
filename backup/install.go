package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"time"

	"github.com/cnlangzi/sqlite-relay/protocol"

	_ "github.com/mattn/go-sqlite3"
)

// snapshotInstall is a snapshot that is on its way to the backup.
type snapshotInstall struct {
	// file is the file that receives the bytes of the snapshot.
	file *os.File
	// path is the path of the file.
	path string
	// meta is the manifest of the snapshot.
	meta protocol.SnapshotMeta
	// hash is the checksum of the bytes that arrived.
	hash hash.Hash
	// bytes is the number of bytes that arrived.
	bytes uint64
}

// BeginSnapshot starts the installation of a snapshot.
//
// The bytes of the snapshot are written into a temporary file next to the
// backup database. The file replaces the backup database when the snapshot
// is committed.
func (a *applier) BeginSnapshot(ctx context.Context, meta protocol.SnapshotMeta) (io.Writer, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.snap != nil {
		return nil, errors.New("backup: a snapshot is already on its way")
	}
	path := a.dbPath + ".new"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("backup: create %s: %w", path, err)
	}
	a.snap = &snapshotInstall{
		file: f,
		path: path,
		meta: meta,
		hash: sha256.New(),
	}
	return &snapshotWriter{app: a}, nil
}

// CommitSnapshot finishes the installation of a snapshot.
//
// The file of the snapshot is checked against the manifest: the size and the
// checksum have to match. The file is checked for its integrity when the
// backup asks for it. The state of the backup is written before the file
// replaces the backup database and after it, so that a backup that was
// killed during the installation knows what it holds.
func (a *applier) CommitSnapshot(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.snap
	if s == nil {
		return errors.New("backup: no snapshot was received")
	}
	a.snap = nil
	defer s.file.Close()

	if s.bytes != s.meta.Bytes {
		os.Remove(s.path)
		return fmt.Errorf("backup: the snapshot holds %d bytes, the manifest says %d", s.bytes, s.meta.Bytes)
	}
	if !bytes.Equal(s.hash.Sum(nil), s.meta.Checksum[:]) {
		os.Remove(s.path)
		return errors.New("backup: the checksum of the snapshot does not match the manifest")
	}
	if s.meta.PageSize == 0 {
		os.Remove(s.path)
		return fmt.Errorf("%w: the manifest of the snapshot names no page size", ErrPageSize)
	}
	info, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("backup: stat %s: %w", s.path, err)
	}
	if info.Size()%int64(s.meta.PageSize) != 0 {
		os.Remove(s.path)
		return fmt.Errorf("%w: the snapshot is %d bytes long, the page size is %d", ErrPageSize, info.Size(), s.meta.PageSize)
	}
	pages := uint32(info.Size() / int64(s.meta.PageSize))
	if err := normalizeHeader(s.file, pages); err != nil {
		os.Remove(s.path)
		return err
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("backup: sync %s: %w", s.path, err)
	}
	if a.verify {
		if err := checkIntegrity(s.path); err != nil {
			os.Remove(s.path)
			return err
		}
	}

	// The state of the backup is written before the file replaces the
	// backup database: a backup that was killed during the installation
	// finds the state and finishes it.
	st := *a.st
	st.Phase = PhaseInstalling
	st.StreamID = s.meta.StreamID
	st.Epoch = s.meta.Epoch
	st.AppliedSeq = s.meta.BaselineSeq
	st.PageSize = s.meta.PageSize
	st.SnapshotBytes = s.meta.Bytes
	st.SnapshotSHA256 = hex.EncodeToString(s.meta.Checksum[:])
	if err := writeState(a.statePath, &st); err != nil {
		return err
	}
	if err := os.Rename(s.path, a.dbPath); err != nil {
		return fmt.Errorf("backup: install the snapshot: %w", err)
	}
	if err := syncDir(a.dir); err != nil {
		return err
	}
	st.Phase = PhaseReady
	if err := writeState(a.statePath, &st); err != nil {
		return err
	}
	a.st = &st
	a.applied = st.AppliedSeq
	a.pending = st.AppliedSeq
	a.needsSnapshot = false
	a.snapshotAt = time.Now()
	a.snapshotBytes = int64(s.meta.Bytes)
	a.dirty = false
	if a.idle != nil {
		close(a.idle)
		a.idle = nil
	}

	if a.paused {
		// The backup is paused, the file of the backup database stays
		// closed until the resume.
		return nil
	}
	f, err := os.OpenFile(a.dbPath, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("backup: open the backup database: %w", err)
	}
	a.file = f
	return nil
}

// snapshotWriter writes the bytes of a snapshot into the temporary file.
type snapshotWriter struct {
	app *applier
}

// Write writes a part of the file of the snapshot.
func (w *snapshotWriter) Write(p []byte) (int, error) {
	w.app.mu.Lock()
	defer w.app.mu.Unlock()
	s := w.app.snap
	if s == nil {
		return 0, errors.New("backup: the snapshot is not open")
	}
	if s.bytes+uint64(len(p)) > s.meta.Bytes {
		return 0, fmt.Errorf("backup: the snapshot holds more than %d bytes", s.meta.Bytes)
	}
	n, err := s.file.Write(p)
	s.bytes += uint64(n)
	s.hash.Write(p[:n])
	return n, err
}

// recover finishes an installation that was interrupted.
//
// The state of the backup is the memory of the installation: it says that a
// snapshot was on its way. The temporary file of the snapshot is the new
// database when it is still there, the backup database is the new one when
// the temporary file is gone.
func recover(dir, dbPath, statePath string, st *state) error {
	if st.Phase != PhaseInstalling {
		// The temporary file of an installation that was not started is
		// left over, it is not a database.
		os.Remove(dbPath + ".new")
		return nil
	}
	tmp := dbPath + ".new"
	if _, err := os.Stat(tmp); err == nil {
		// The rename did not happen, the temporary file is the new
		// database.
		if err := os.Rename(tmp, dbPath); err != nil {
			return fmt.Errorf("backup: install the snapshot: %w", err)
		}
		if err := syncDir(dir); err != nil {
			return err
		}
		st.Phase = PhaseReady
		return writeState(statePath, st)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("backup: stat %s: %w", tmp, err)
	}
	// The rename happened, the backup database is the new one. Its
	// checksum has to match the state, otherwise the installation did not
	// finish.
	sum, err := fileChecksum(dbPath)
	if err != nil {
		return err
	}
	if hex.EncodeToString(sum) != st.SnapshotSHA256 {
		return fmt.Errorf("%w: the installation did not finish", ErrSnapshotRequired)
	}
	st.Phase = PhaseReady
	return writeState(statePath, st)
}

// fileChecksum returns the SHA-256 of a file.
func fileChecksum(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("backup: open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, fmt.Errorf("backup: read %s: %w", path, err)
	}
	return h.Sum(nil), nil
}

// checkIntegrity verifies that a file is a valid database.
func checkIntegrity(path string) error {
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000")
	if err != nil {
		return fmt.Errorf("backup: open the backup database: %w", err)
	}
	defer db.Close()
	var result string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("backup: integrity_check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("backup: integrity_check reported %q", result)
	}
	return nil
}
