package driver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// newTestDriver opens a driver on a fresh database.
func newTestDriver(t *testing.T, cfg Config) (*Driver, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	cfg.DSN = path + "?_journal_mode=WAL"
	if cfg.ScanInterval == 0 {
		cfg.ScanInterval = 20 * time.Millisecond
	}
	d, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, path
}

// createTable creates the table the tests write to.
func createTable(t *testing.T, d *Driver) {
	t.Helper()
	if _, err := d.DB().Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
}

// insert commits a single transaction.
func insert(t *testing.T, d *Driver, v string) {
	t.Helper()
	if _, err := d.DB().Exec("INSERT INTO t(v) VALUES(?)", v); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

// scanAll collects the changes of the driver until it runs empty.
func scanAll(t *testing.T, d *Driver) []Change {
	t.Helper()
	var out []Change
	for {
		batch, err := d.Scan(context.Background())
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if len(batch.Changes) == 0 {
			return out
		}
		out = append(out, batch.Changes...)
	}
}

func TestScanReturnsCommittedTransactions(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)
	insert(t, d, "a")
	insert(t, d, "b")

	changes := scanAll(t, d)
	if len(changes) != 3 {
		// The CREATE TABLE is a transaction of its own.
		t.Fatalf("got %d changes, want 3", len(changes))
	}
	for i, c := range changes {
		if c.Seq != uint64(i+1) {
			t.Errorf("change %d has seq %d", i, c.Seq)
		}
		if c.CommitPages == 0 {
			t.Errorf("change %d has no commit size", i)
		}
		if len(c.Pages) == 0 {
			t.Errorf("change %d has no pages", i)
		}
		for j, p := range c.Pages {
			if j > 0 && c.Pages[j-1].Pgno >= p.Pgno {
				t.Errorf("change %d pages are not ordered: %d then %d", i, c.Pages[j-1].Pgno, p.Pgno)
			}
			if p.Pgno > c.CommitPages {
				t.Errorf("change %d holds page %d beyond the commit size %d", i, p.Pgno, c.CommitPages)
			}
			if len(p.Data) != d.pageSize {
				t.Errorf("change %d page %d has %d bytes, want %d", i, p.Pgno, len(p.Data), d.pageSize)
			}
		}
	}
}

func TestScanIsEmptyWithoutCommits(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)
	scanAll(t, d)

	batch, err := d.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(batch.Changes) != 0 {
		t.Fatalf("got %d changes, want 0", len(batch.Changes))
	}
}

func TestPositionRoundTrip(t *testing.T) {
	p := Position{Salt1: 0x11223344, Salt2: 0x55667788, Frame: 42, WALOffset: 123456}
	b, err := p.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var got Position
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if got != p {
		t.Fatalf("got %v, want %v", got, p)
	}
}

func TestPositionResetsOnGenerationChange(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)
	insert(t, d, "a")
	scanAll(t, d)

	before := d.cursorPosition()
	if before.Frame == 0 {
		t.Fatal("the cursor did not move")
	}
	// A new generation restarts the frame number and the byte offset.
	next := Position{Salt1: before.Salt1 + 1, Salt2: before.Salt2}
	if err := d.handleGenerationChange(wal.Header{Salt1: next.Salt1, Salt2: next.Salt2}); err != nil {
		t.Fatalf("handleGenerationChange: %v", err)
	}
	after := d.cursorPosition()
	if after.Frame != 0 {
		t.Errorf("frame = %d, want 0", after.Frame)
	}
	if after.WALOffset != walOffset(0, d.pageSize) {
		t.Errorf("offset = %d, want %d", after.WALOffset, walOffset(0, d.pageSize))
	}
	if after.Salt1 != next.Salt1 || after.Salt2 != next.Salt2 {
		t.Errorf("salt = %v, want %v", after, next)
	}
}

func TestCheckFrames(t *testing.T) {
	if err := checkFrames(5, 5, 5); err != nil {
		t.Errorf("equal counts reported an error: %v", err)
	}
	if err := checkFrames(5, 6, 6); err != nil {
		t.Errorf("a growing WAL reported an error: %v", err)
	}
	if err := checkFrames(6, 5, 6); err == nil {
		t.Error("a count below the lower bound was accepted")
	}
	if err := checkFrames(5, 7, 6); err == nil {
		t.Error("a count above the upper bound was accepted")
	}
}

func TestHookFiresOnEveryConnection(t *testing.T) {
	const conns = 4
	d, _ := newTestDriver(t, Config{MaxOpenConns: conns})
	createTable(t, d)

	// Hold one connection per worker so that the pool has to open them all.
	held := make([]*sql.Conn, conns)
	for i := range held {
		c, err := d.DB().Conn(context.Background())
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		held[i] = c
	}
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()

	// Every managed connection has to hold the invariants.
	for i, c := range held {
		var autocheckpoint int64
		err := c.Raw(func(dc any) error {
			mc, ok := dc.(*managedConn)
			if !ok {
				return fmt.Errorf("unexpected connection type %T", dc)
			}
			n, err := pragmaInt(mc.conn, "PRAGMA wal_autocheckpoint")
			autocheckpoint = n
			return err
		})
		if err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
		if autocheckpoint != 0 {
			t.Fatalf("connection %d has wal_autocheckpoint %d, want 0", i, autocheckpoint)
		}
	}

	for i, c := range held {
		if _, err := c.ExecContext(context.Background(), "INSERT INTO t(v) VALUES(?)", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("insert on connection %d: %v", i, err)
		}
	}

	// Every commit has to be reported.
	deadline := time.After(5 * time.Second)
	seen := 0
	for seen < conns {
		select {
		case <-d.Events():
			seen++
		case <-deadline:
			t.Fatalf("saw %d notifications, want %d", seen, conns)
		}
	}
}

func TestHookPanicDoesNotFailTheCommit(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)

	// A closed channel makes the notification panic inside the hook. The
	// loop of the driver notifies as well, so it has to stop before the
	// channel closes.
	d.cancel()
	d.wg.Wait()
	d.closeEvents()

	if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('boom')"); err != nil {
		t.Fatalf("the commit failed because of the hook: %v", err)
	}
	changes := scanAll(t, d)
	if len(changes) != 2 {
		t.Fatalf("got %d changes, want 2", len(changes))
	}
}

func TestUnmanagedCheckpointIsDetected(t *testing.T) {
	d, path := newTestDriver(t, Config{})
	createTable(t, d)
	insert(t, d, "a")
	scanAll(t, d)

	// A connection the driver does not know about checkpoints the database.
	rogue, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open rogue: %v", err)
	}
	defer rogue.Close()
	if _, err := rogue.Exec("PRAGMA main.wal_checkpoint(PASSIVE)"); err != nil {
		t.Fatalf("rogue checkpoint: %v", err)
	}

	if _, err := d.Scan(context.Background()); err == nil {
		t.Fatal("the driver did not notice the unmanaged checkpoint")
	} else if !contains(err.Error(), "nBackfill") {
		t.Fatalf("the error does not mention nBackfill: %v", err)
	}
}

func TestVacuumShrinkDropsPages(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)
	for range 200 {
		insert(t, d, "padding")
	}
	scanAll(t, d)

	// A VACUUM makes the database smaller.
	if _, err := d.DB().Exec("VACUUM"); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	changes := scanAll(t, d)
	if len(changes) == 0 {
		t.Fatal("the vacuum did not produce a change")
	}
	last := changes[len(changes)-1]
	for _, p := range last.Pages {
		if p.Pgno > last.CommitPages {
			t.Errorf("the change holds page %d beyond the commit size %d", p.Pgno, last.CommitPages)
		}
	}
}

func TestPeriodicScanFindsCommitsWithoutNotifications(t *testing.T) {
	d, _ := newTestDriver(t, Config{ScanInterval: 20 * time.Millisecond, EventBuffer: 1})
	createTable(t, d)

	// Fill the notification channel so that every notification is dropped.
	select {
	case d.events <- Notification{DBName: "main"}:
	default:
	}

	insert(t, d, "a")

	// The notification is lost, the scan still finds the transaction.
	deadline := time.Now().Add(5 * time.Second)
	for {
		batch, err := d.Scan(context.Background())
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if len(batch.Changes) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the periodic scan did not find the commit")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestQuiesceBlocksWrites(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)

	if err := d.Quiesce(context.Background()); err != nil {
		t.Fatalf("Quiesce: %v", err)
	}
	defer d.Resume()

	done := make(chan error, 1)
	go func() {
		_, err := d.DB().Exec("INSERT INTO t(v) VALUES('blocked')")
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("the write was not blocked: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := d.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the write failed after the resume: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the write did not continue after the resume")
	}
}

func TestConcurrentScansAreSerialized(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 20 {
				if _, err := d.DB().Exec("INSERT INTO t(v) VALUES(?)", fmt.Sprintf("%d-%d", i, j)); err != nil {
					t.Errorf("insert: %v", err)
					return
				}
				if _, err := d.Scan(context.Background()); err != nil {
					t.Errorf("Scan: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
