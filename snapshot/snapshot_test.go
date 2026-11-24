package snapshot

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/segment"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// newTestDriver opens a driver on a fresh database.
func newTestDriver(t *testing.T) (*driver.Driver, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "primary.db")
	d, err := driver.Open(context.Background(), driver.Config{
		DSN:          path + "?_journal_mode=WAL",
		ScanInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, path
}

// newTestSpool opens a spool in a directory.
func newTestSpool(t *testing.T, root string) *spool.Spool {
	t.Helper()
	sp, err := spool.Open(context.Background(), spool.Config{
		Root:       root,
		DatabaseID: "app.db",
		Mode:       spool.ModeFull,
	})
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	t.Cleanup(func() { sp.Close() })
	return sp
}

// capture is the capture of the primary database of the tests.
//
// It stands in for the capture of the peer runtime: it scans the changes of
// the driver and appends them to the spool.
type capture struct {
	drv *driver.Driver
	sp  *spool.Spool

	mu     sync.Mutex
	cond   *sync.Cond
	active bool
	busy   bool
}

// newCapture starts a capture of the primary database.
func newCapture(d *driver.Driver, sp *spool.Spool) *capture {
	c := &capture{drv: d, sp: sp, active: true}
	c.cond = sync.NewCond(&c.mu)
	go c.loop()
	return c
}

// loop scans the changes of the driver and appends them to the spool.
func (c *capture) loop() {
	for {
		c.mu.Lock()
		for !c.active {
			c.cond.Wait()
		}
		c.busy = true
		c.mu.Unlock()

		batch, err := c.drv.Scan(context.Background())
		if err == nil {
			for _, change := range batch.Changes {
				tx := spool.Tx{
					Pos: spool.Pos{
						Salt1:     change.Pos.Salt1,
						Salt2:     change.Pos.Salt2,
						Frame:     change.Pos.Frame,
						WALOffset: change.Pos.WALOffset,
					},
					CommitPages: change.CommitPages,
				}
				for _, p := range change.Pages {
					tx.Pages = append(tx.Pages, spool.Page{Pgno: p.Pgno, Data: p.Data})
				}
				if err := c.sp.Append(context.Background(), tx); err != nil {
					return
				}
			}
			if len(batch.Changes) > 0 {
				// The segment is published, the chain holds the
				// transactions.
				if err := c.sp.Flush(context.Background()); err != nil {
					return
				}
			}
		}

		c.mu.Lock()
		c.busy = false
		c.cond.Broadcast()
		c.mu.Unlock()
		if err != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Suspend stops the capture until the returned function is called.
func (c *capture) Suspend(ctx context.Context) (func(), error) {
	c.mu.Lock()
	c.active = false
	for c.busy {
		c.cond.Wait()
	}
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		c.active = true
		c.cond.Broadcast()
		c.mu.Unlock()
	}, nil
}

// idleCapture is a capture that does not capture anything.
//
// The chain of the spool stays empty, the snapshot is the baseline of the
// chain that follows it.
type idleCapture struct{}

// Suspend stops the capture.
func (c *idleCapture) Suspend(ctx context.Context) (func(), error) {
	return func() {}, nil
}

// insert writes a row into the primary database.
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
		t.Fatalf("create table: %v", err)
	}
}

// rows returns the rows of the table of a database.
func rows(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	r, err := db.Query("SELECT v FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer r.Close()
	var out []string
	for r.Next() {
		var v string
		if err := r.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, v)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// theSnapshotFile returns the file of the snapshot in a directory.
func theSnapshotFile(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the directory: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("the directory holds %d files, want the file of the snapshot", len(entries))
	}
	return filepath.Join(dir, entries[0].Name())
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

func TestSnapshotIsBoundToTheLastTransactionOfTheChain(t *testing.T) {
	d, _ := newTestDriver(t)
	sp := newTestSpool(t, t.TempDir())
	cap := newCapture(d, sp)
	createTable(t, d)
	insert(t, d, "first")

	dir := t.TempDir()
	p, err := New(Config{Dir: dir, DatabaseID: "app.db", StreamID: 1}, d, sp, cap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// The chain holds the transaction of the insert.
	if !waitFor(t, func() bool {
		ack, err := sp.Durable()
		return err == nil && ack.Seq >= 2
	}) {
		ack, _ := sp.Durable()
		t.Fatalf("the chain holds the sequence number %d, want at least 2", ack.Seq)
	}

	meta, r, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer r.Close()
	if meta.BaselineSeq == 0 {
		t.Fatal("the snapshot has no baseline")
	}
	if meta.DatabaseID != segment.DatabaseID("app.db") {
		t.Error("the snapshot names another database")
	}
	if meta.PageSize == 0 {
		t.Error("the snapshot names no page size")
	}
	if meta.Bytes == 0 {
		t.Error("the snapshot is empty")
	}

	// The snapshot holds the state of the primary database.
	snapshot := theSnapshotFile(t, dir)
	if got := rows(t, snapshot); len(got) != 1 || got[0] != "first" {
		t.Errorf("the snapshot holds %v, want the rows of the primary", got)
	}

	// The chain continues after the baseline of the snapshot.
	insert(t, d, "second")
	if !waitFor(t, func() bool {
		ack, err := sp.Durable()
		return err == nil && ack.Seq > meta.BaselineSeq
	}) {
		ack, _ := sp.Durable()
		t.Fatalf("the chain holds the sequence number %d, want more than the baseline %d", ack.Seq, meta.BaselineSeq)
	}
}

func TestSnapshotDrainsTheTransactionsOfTheBoundary(t *testing.T) {
	d, _ := newTestDriver(t)
	sp := newTestSpool(t, t.TempDir())
	cap := newCapture(d, sp)
	createTable(t, d)
	insert(t, d, "first")

	p, err := New(Config{Dir: t.TempDir(), DatabaseID: "app.db", StreamID: 1}, d, sp, cap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !waitFor(t, func() bool {
		ack, err := sp.Durable()
		return err == nil && ack.Seq >= 2
	}) {
		t.Fatal("the chain did not hold the transactions")
	}

	meta, r, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	r.Close()

	// The chain of the spool ends at the baseline of the snapshot: the
	// transactions that follow it are the ones the backup applies.
	refs, err := sp.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	var last uint64
	for _, ref := range refs {
		last = ref.SeqEnd
	}
	if last != meta.BaselineSeq {
		t.Errorf("the chain ends at the sequence number %d, want the baseline %d", last, meta.BaselineSeq)
	}
}

func TestSnapshotOfADatabaseWithoutAChain(t *testing.T) {
	d, _ := newTestDriver(t)
	sp := newTestSpool(t, t.TempDir())
	createTable(t, d)
	insert(t, d, "first")

	p, err := New(Config{Dir: t.TempDir(), DatabaseID: "app.db", StreamID: 1}, d, sp, &idleCapture{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// The capture did not append anything yet, the chain is empty and the
	// snapshot is the baseline of the chain that follows it.
	meta, r, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer r.Close()
	if meta.BaselineSeq != 0 {
		t.Errorf("the baseline of the snapshot is %d, want 0", meta.BaselineSeq)
	}
}

func TestTheFileOfTheSnapshotIsRemovedWhenItIsClosed(t *testing.T) {
	d, _ := newTestDriver(t)
	sp := newTestSpool(t, t.TempDir())
	cap := newCapture(d, sp)
	createTable(t, d)

	dir := t.TempDir()
	p, err := New(Config{Dir: dir, DatabaseID: "app.db", StreamID: 1}, d, sp, cap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, r, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the directory: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("the directory holds %d files, want the file of the snapshot", len(entries))
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entries, err = os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the directory: %v", err)
	}
	if len(entries) != 0 {
		t.Error("the file of the snapshot was not removed")
	}
}
