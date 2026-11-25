package source

import (
	"cmp"
	"context"
	"database/sql"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/internal/wal"
	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/segment"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// testToken is the token of the tests.
const testToken = "the shared token of the tests"

// testDatabaseID is the name of the database of the tests.
const testDatabaseID = "primary.db"

// newTestDriver opens a driver on a fresh database.
func newTestDriver(t *testing.T, dir string) (*driver.Driver, string) {
	t.Helper()
	path := filepath.Join(dir, testDatabaseID)
	d, err := driver.Open(context.Background(), driver.Config{
		DSN:          path + "?_journal_mode=WAL",
		ScanInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open the driver: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, path
}

// reopenTestDriver opens a driver on the database of the tests again.
//
// The write-ahead log of the database is reset when the last connection of
// the database closes: the driver that opens it sees a new generation.
func reopenTestDriver(t *testing.T, path string) *driver.Driver {
	t.Helper()
	d, err := driver.Open(context.Background(), driver.Config{
		DSN:          path + "?_journal_mode=WAL",
		ScanInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open the driver: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// testConfig returns the configuration of the runtime of the tests.
func testConfig(t *testing.T, dir string) Config {
	t.Helper()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatalf("write the token: %v", err)
	}
	return Config{
		Spool: spool.Config{
			Root:         filepath.Join(dir, "spool"),
			Mode:         spool.ModeFull,
			SyncInterval: 10 * time.Millisecond,
		},
		Protocol: protocol.Config{
			TokenFile:  token,
			DatabaseID: testDatabaseID,
			StreamID:   1,
		},
		SnapshotDir:  filepath.Join(dir, "snapshots"),
		ScanInterval: 20 * time.Millisecond,
		GCInterval:   20 * time.Millisecond,
	}
}

// startPeer opens the runtime and lets it serve the listener.
func startPeer(t *testing.T, cfg Config) *Peer {
	t.Helper()
	p, _ := startPeerAt(t, cfg)
	return p
}

// startPeerAt opens the runtime, lets it serve the listener and returns the
// address of the listener.
func startPeerAt(t *testing.T, cfg Config) (*Peer, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go p.Run(ctx, ln)
	t.Cleanup(func() { ln.Close() })
	return p, ln.Addr().String()
}

// insert commits a transaction on the primary database.
func insert(t *testing.T, d *driver.Driver, v string) {
	t.Helper()
	if _, err := d.DB().Exec("INSERT INTO t(v) VALUES(?)", v); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

// createTable creates the table the tests write to.
func createTable(t *testing.T, d *driver.Driver) {
	t.Helper()
	if _, err := d.DB().Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create the table: %v", err)
	}
}

// segmentTxs returns the transactions of the published segments of the spool.
//
// The transactions are ordered by their sequence number.
func segmentTxs(t *testing.T, root string) []segment.Tx {
	t.Helper()
	dir := filepath.Join(root, testDatabaseID, "segments")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read the segments directory: %v", err)
	}
	var out []segment.Tx
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		seg, err := segment.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read the segment: %v", err)
		}
		out = append(out, seg.Txs...)
	}
	slices.SortFunc(out, func(a, b segment.Tx) int { return cmp.Compare(a.Seq, b.Seq) })
	return out
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

// keepLogOpen opens a connection that keeps the write-ahead log of the
// database alive.
//
// The log is deleted when the last connection of the database closes: the
// connection keeps the frames of the log around while the driver is closed.
func keepLogOpen(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open the connection: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db
}

// checkpoint writes the frames of the write-ahead log back into the database
// file.
//
// The database file is consistent afterwards: a checkpoint that was
// interrupted leaves it with pages of two different states. The log keeps its
// frames.
func checkpoint(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
		t.Fatalf("checkpoint the database: %v", err)
	}
}

// copyFile copies a file.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

// truncateLog cuts the tail of the write-ahead log of the primary database.
//
// The frames of the log are lost: the primary database lost the transactions
// that were committed at the end of the log.
func truncateLog(t *testing.T, path string) {
	t.Helper()
	if err := os.Truncate(path, wal.HeaderSize+16); err != nil {
		t.Fatalf("truncate the write-ahead log: %v", err)
	}
}

// corruptLog writes garbage into the frame of the log at the given position.
//
// The frame fails the checksum of the log: the transactions that follow it
// are lost.
func corruptLog(t *testing.T, path string, pos spool.Pos) {
	t.Helper()
	hdr, err := wal.ReadHeader(path)
	if err != nil {
		t.Fatalf("read the header of the write-ahead log: %v", err)
	}
	off := int64(wal.HeaderSize) + int64(pos.Frame-1)*int64(hdr.FrameSize())
	garbage := make([]byte, hdr.PageSize)
	for i := range garbage {
		garbage[i] = 0xff
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open the write-ahead log: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteAt(garbage, off+int64(wal.FrameHeaderSize)); err != nil {
		t.Fatalf("write the write-ahead log: %v", err)
	}
}
