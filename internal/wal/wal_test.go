package wal

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const testPageSize = 1024

// builder assembles a WAL file byte by byte so that tests can inject the
// exact corruption they want to exercise.
type builder struct {
	pageSize int
	salt1    uint32
	salt2    uint32
	ckptSeq  uint32
	sum      Checksum
	buf      []byte
}

func newBuilder() *builder {
	b := &builder{pageSize: testPageSize, salt1: 0x11223344, salt2: 0x55667788}
	b.writeHeader()
	return b
}

func (b *builder) writeHeader() {
	h := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(h[0:], MagicLittleEndian)
	binary.BigEndian.PutUint32(h[4:], FormatVersion)
	binary.BigEndian.PutUint32(h[8:], uint32(b.pageSize))
	binary.BigEndian.PutUint32(h[12:], b.ckptSeq)
	binary.BigEndian.PutUint32(h[16:], b.salt1)
	binary.BigEndian.PutUint32(h[20:], b.salt2)
	sum := Checksum{}.update(littleEndianWords, h[:24])
	binary.BigEndian.PutUint32(h[24:], sum.S1)
	binary.BigEndian.PutUint32(h[28:], sum.S2)
	b.sum = sum
	b.buf = h
}

// frame appends a frame with a valid checksum.
func (b *builder) frame(pgno, commitPages uint32, fill byte) {
	data := make([]byte, b.pageSize)
	for i := range data {
		data[i] = fill
	}
	b.frameWith(pgno, commitPages, data)
}

func (b *builder) frameWith(pgno, commitPages uint32, data []byte) {
	fh := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint32(fh[0:], pgno)
	binary.BigEndian.PutUint32(fh[4:], commitPages)
	binary.BigEndian.PutUint32(fh[8:], b.salt1)
	binary.BigEndian.PutUint32(fh[12:], b.salt2)
	b.sum = b.sum.update(littleEndianWords, fh[:8])
	b.sum = b.sum.update(littleEndianWords, data)
	binary.BigEndian.PutUint32(fh[16:], b.sum.S1)
	binary.BigEndian.PutUint32(fh[20:], b.sum.S2)
	b.buf = append(b.buf, fh...)
	b.buf = append(b.buf, data...)
}

// corrupt flips a byte in the last frame so that its checksum no longer
// matches.
func (b *builder) corrupt() {
	b.buf[len(b.buf)-1] ^= 0xff
}

func (b *builder) write(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "test.db-wal")
	if err := os.WriteFile(path, b.buf, 0o600); err != nil {
		t.Fatalf("write WAL: %v", err)
	}
	return path
}

func openReader(t *testing.T, path string) *Reader {
	t.Helper()
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestScanStopsAtUncommittedTail(t *testing.T) {
	dir := t.TempDir()
	b := newBuilder()
	b.frame(1, 0, 'a') // transaction 1, not committed yet
	b.frame(2, 0, 'b')
	b.frame(3, 3, 'c') // commit
	b.frame(4, 0, 'd') // transaction 2, never committed
	b.frame(5, 0, 'e')
	path := b.write(t, dir)

	scan, err := openReader(t, path).Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(scan.Transactions) != 1 {
		t.Fatalf("got %d transactions, want 1", len(scan.Transactions))
	}
	tx := scan.Transactions[0]
	if tx.CommitFrame != 3 {
		t.Errorf("commit frame = %d, want 3", tx.CommitFrame)
	}
	if tx.CommitPages != 3 {
		t.Errorf("commit pages = %d, want 3", tx.CommitPages)
	}
	if len(tx.Frames) != 3 {
		t.Fatalf("got %d frames, want 3", len(tx.Frames))
	}
	for i, f := range tx.Frames {
		if f.Pgno != uint32(i+1) {
			t.Errorf("frame %d page = %d, want %d", i, f.Pgno, i+1)
		}
	}
	if scan.CommitFrame != 3 {
		t.Errorf("scan commit frame = %d, want 3", scan.CommitFrame)
	}
}

func TestScanStopsAtSaltMismatch(t *testing.T) {
	dir := t.TempDir()
	b := newBuilder()
	b.frame(1, 0, 'a')
	b.frame(2, 2, 'b')
	// A frame from another generation.
	b.salt1 = 0xdeadbeef
	b.frame(3, 0, 'c')
	path := b.write(t, dir)

	scan, err := openReader(t, path).Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(scan.Transactions) != 1 {
		t.Fatalf("got %d transactions, want 1", len(scan.Transactions))
	}
	if !scan.Truncated {
		t.Error("scan did not report truncation")
	}
}

func TestScanStopsAtChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	b := newBuilder()
	b.frame(1, 0, 'a')
	b.frame(2, 2, 'b')
	b.corrupt()
	path := b.write(t, dir)

	scan, err := openReader(t, path).Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(scan.Transactions) != 0 {
		t.Fatalf("got %d transactions, want 0", len(scan.Transactions))
	}
	if !scan.Truncated {
		t.Error("scan did not report truncation")
	}
}

func TestScanStopsAtPageNumberZero(t *testing.T) {
	dir := t.TempDir()
	b := newBuilder()
	b.frame(1, 0, 'a')
	b.frame(0, 2, 'b')
	path := b.write(t, dir)

	scan, err := openReader(t, path).Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(scan.Transactions) != 0 {
		t.Fatalf("got %d transactions, want 0", len(scan.Transactions))
	}
	if !scan.Truncated {
		t.Error("scan did not report truncation")
	}
}

func TestScanHeaderOnly(t *testing.T) {
	dir := t.TempDir()
	path := newBuilder().write(t, dir)

	scan, err := openReader(t, path).Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(scan.Transactions) != 0 {
		t.Fatalf("got %d transactions, want 0", len(scan.Transactions))
	}
	if scan.CommitFrame != 0 {
		t.Errorf("commit frame = %d, want 0", scan.CommitFrame)
	}
}

func TestOpenMissingWAL(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "absent-wal"))
	if !errors.Is(err, ErrNoWAL) {
		t.Fatalf("err = %v, want ErrNoWAL", err)
	}
}

func TestOpenShortWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "short-wal")
	if err := os.WriteFile(path, make([]byte, 16), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrNoWAL) {
		t.Fatalf("err = %v, want ErrNoWAL", err)
	}
}

// TestScanMatchesSQLite checks the parser against the frames SQLite itself
// reports through the wal-index.
func TestScanMatchesSQLite(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "real.db")
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	const txns = 5
	for i := 0; i < txns; i++ {
		if _, err := db.Exec("INSERT INTO t(v) VALUES(?)", "value"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// The control connection is separate from the writer, which is how the
	// driver reads the frame count back.
	ctl, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open control: %v", err)
	}
	defer ctl.Close()

	var busy, log, ckpt int
	if err := ctl.QueryRow("PRAGMA main.wal_checkpoint(NOOP)").Scan(&busy, &log, &ckpt); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	r, err := Open(dbPath + "-wal")
	if err != nil {
		t.Fatalf("Open WAL: %v", err)
	}
	defer r.Close()
	scan, err := r.Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if scan.CommitFrame != uint64(log) {
		t.Errorf("commit frame = %d, want %d (nLog)", scan.CommitFrame, log)
	}
	if len(scan.Transactions) != txns+1 {
		// The CREATE TABLE is a transaction of its own.
		t.Errorf("got %d transactions, want %d", len(scan.Transactions), txns+1)
	}
	for _, tx := range scan.Transactions {
		if !tx.Frames[len(tx.Frames)-1].IsCommit() {
			t.Error("transaction does not end on a commit frame")
		}
	}

	// A later commit has to show up in the frame count of the control
	// connection as well.
	if _, err := db.Exec("INSERT INTO t(v) VALUES(?)", "later"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := ctl.QueryRow("PRAGMA main.wal_checkpoint(NOOP)").Scan(&busy, &log, &ckpt); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	r2, err := Open(dbPath + "-wal")
	if err != nil {
		t.Fatalf("Open WAL: %v", err)
	}
	defer r2.Close()
	scan2, err := r2.Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if scan2.CommitFrame != uint64(log) {
		t.Errorf("commit frame = %d, want %d (nLog)", scan2.CommitFrame, log)
	}
}
