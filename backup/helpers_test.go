package backup

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/segment"

	_ "github.com/mattn/go-sqlite3"
)

// testToken is the token of the tests.
const testToken = "the shared token of the tests"

// testPageSize is the size of the pages of the test databases.
const testPageSize = 4096

// newTestDatabase creates a SQLite database with a table and one row.
//
// The database runs in the mode of the write-ahead log when wal is set, the
// pages of a database in that mode carry the flags of the log.
func newTestDatabase(t *testing.T, dir, name string, wal bool) string {
	t.Helper()
	path := filepath.Join(dir, name)
	dsn := path
	if wal {
		dsn += "?_journal_mode=WAL"
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open the database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create the table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t(v) VALUES('first')"); err != nil {
		t.Fatalf("insert the row: %v", err)
	}
	return path
}

// readPages returns the pages of a database file.
func readPages(t *testing.T, path string) [][]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the database: %v", err)
	}
	if len(b)%testPageSize != 0 {
		t.Fatalf("the database is %d bytes long, the page size is %d", len(b), testPageSize)
	}
	pages := make([][]byte, 0, len(b)/testPageSize)
	for off := 0; off < len(b); off += testPageSize {
		pages = append(pages, b[off:off+testPageSize])
	}
	return pages
}

// transaction returns a transaction that holds the pages of a database.
//
// The transaction is the one the chain of the source would hold after the
// database was changed.
func transaction(t *testing.T, path string, seq uint64) segment.Tx {
	t.Helper()
	pages := readPages(t, path)
	tx := segment.Tx{Seq: seq, CommitPages: uint32(len(pages))}
	for i, p := range pages {
		tx.Pages = append(tx.Pages, segment.Page{Pgno: uint32(i + 1), Data: append([]byte(nil), p...)})
	}
	return tx
}

// query reads the rows of a table of a database.
func query(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open the database: %v", err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT v FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query the database: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// write inserts a row into a database.
func write(t *testing.T, path, v string) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open the database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("INSERT INTO t(v) VALUES(?)", v); err != nil {
		t.Fatalf("insert the row: %v", err)
	}
}

// snapshotMeta returns the manifest of a snapshot of a database file.
func snapshotMeta(t *testing.T, path, databaseID string, epoch, baseline uint64) protocol.SnapshotMeta {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the database: %v", err)
	}
	return protocol.SnapshotMeta{
		FormatVersion: 1,
		DatabaseID:    segment.DatabaseID(databaseID),
		StreamID:      1,
		Epoch:         epoch,
		BaselineSeq:   baseline,
		PageSize:      testPageSize,
		Bytes:         uint64(len(b)),
		Checksum:      sha256.Sum256(b),
	}
}

// installSnapshot installs a snapshot of a database file in a backup.
//
// It returns the applier of the backup.
func installSnapshot(t *testing.T, app *applier, path, databaseID string, epoch, baseline uint64) {
	t.Helper()
	meta := snapshotMeta(t, path, databaseID, epoch, baseline)
	w, err := app.BeginSnapshot(context.Background(), meta)
	if err != nil {
		t.Fatalf("BeginSnapshot: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the database: %v", err)
	}
	if _, err := w.Write(b); err != nil {
		t.Fatalf("write the snapshot: %v", err)
	}
	if err := app.CommitSnapshot(context.Background()); err != nil {
		t.Fatalf("CommitSnapshot: %v", err)
	}
}

// newTestApplier opens an applier of a backup in a directory.
func newTestApplier(t *testing.T, dir, databaseID string) *applier {
	t.Helper()
	st := &state{
		Version:    stateVersion,
		Phase:      PhaseEmpty,
		DatabaseID: databaseID,
		StreamID:   1,
		DBPath:     filepath.Join(dir, dbName),
	}
	if err := writeState(filepath.Join(dir, stateName), st); err != nil {
		t.Fatalf("writeState: %v", err)
	}
	return &applier{
		dir:       dir,
		dbPath:    filepath.Join(dir, dbName),
		statePath: filepath.Join(dir, stateName),
		st:        st,
	}
}

// fakeSource provides the segments of the chain to the source of the
// protocol.
type fakeSource struct {
	mu       sync.Mutex
	epoch    uint64
	segments []protocol.Segment
}

// Epoch returns the generation of the chain.
func (f *fakeSource) Epoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.epoch
}

// OldestSeq returns the lowest sequence number the source holds.
func (f *fakeSource) OldestSeq() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.segments) == 0 {
		return 0
	}
	return f.segments[0].SeqStart
}

// SegmentsAfter returns the segments that hold a transaction after seq.
func (f *fakeSource) SegmentsAfter(ctx context.Context, seq uint64, limit int64) ([]protocol.Segment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []protocol.Segment
	for _, s := range f.segments {
		if s.SeqEnd <= seq {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// add puts segments into the source.
func (f *fakeSource) add(segs ...protocol.Segment) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.segments = append(f.segments, segs...)
}

// fakeProvider produces the snapshot of the source of the protocol.
type fakeProvider struct {
	mu   sync.Mutex
	meta protocol.SnapshotMeta
	data []byte
}

// Snapshot writes a consistent snapshot of the database.
func (p *fakeProvider) Snapshot(ctx context.Context) (protocol.SnapshotMeta, io.ReadCloser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	meta := p.meta
	meta.Bytes = uint64(len(p.data))
	meta.Checksum = sha256.Sum256(p.data)
	return meta, io.NopCloser(bytesReader(p.data)), nil
}

// bytesReader returns a reader of the bytes.
func bytesReader(b []byte) io.Reader {
	return &sliceReader{b: b}
}

// sliceReader reads a slice of bytes.
type sliceReader struct {
	b []byte
}

// Read reads the bytes of the slice.
func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

// testSegment returns the bytes of a segment that holds the given
// transaction.
func testSegment(t *testing.T, databaseID string, epoch, seqStart uint64, tx segment.Tx) []byte {
	t.Helper()
	var buf sliceWriter
	h := segment.Header{
		Version:    segment.Version,
		DatabaseID: segment.DatabaseID(databaseID),
		Epoch:      epoch,
		SeqStart:   seqStart,
		PageSize:   testPageSize,
	}
	w, err := segment.NewWriter(&buf, h)
	if err != nil {
		t.Fatalf("segment.NewWriter: %v", err)
	}
	if err := w.WriteTx(tx); err != nil {
		t.Fatalf("segment.WriteTx: %v", err)
	}
	if _, err := w.Close(segment.Pos{Salt1: 1, Salt2: 2, Frame: seqStart, WALOffset: 4096}); err != nil {
		t.Fatalf("segment.Close: %v", err)
	}
	return buf.b
}

// sliceWriter collects the bytes of a segment.
type sliceWriter struct {
	b []byte
}

// Write appends the bytes to the slice.
func (w *sliceWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

// testConfig returns the configuration of a backup of the tests.
func testConfig(t *testing.T, root string) Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatalf("write the token: %v", err)
	}
	return Config{
		Root: root,
		Protocol: protocol.Config{
			TokenFile:  path,
			DatabaseID: "app.db",
			StreamID:   1,
			BackupID:   "backup-1",
		},
	}
}

// checksum returns the SHA-256 of a file as a string.
func checksum(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// mustRead returns the bytes of a file.
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

// copyFile copies a file.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b := mustRead(t, src)
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

// waitFor waits until the condition holds or the timeout is over.
func waitFor(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
