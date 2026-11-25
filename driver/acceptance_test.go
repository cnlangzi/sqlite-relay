package driver

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// TestIncrementBoundaryComesFromTheWAL checks that the boundary of an
// increment is decided by the bytes of the WAL and not by the frame count the
// WAL hook reports.
//
// The hook reports the number of frames the WAL holds, which is not the
// number of transactions: a transaction that writes three pages reports one
// frame more than a transaction that writes one page.
func TestIncrementBoundaryComesFromTheWAL(t *testing.T) {
	d, path := newTestDriver(t, Config{})
	createTable(t, d)

	// A transaction that writes more than one page.
	if _, err := d.DB().Exec("INSERT INTO t(v) VALUES(?)", string(make([]byte, 8000))); err != nil {
		t.Fatalf("insert: %v", err)
	}
	insert(t, d, "a")

	// The notification of the hook.
	var notified Notification
	select {
	case notified = <-d.Events():
	case <-time.After(5 * time.Second):
		t.Fatal("no notification arrived")
	}

	batch, err := d.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(batch.Changes) != 3 {
		// The CREATE TABLE and the two inserts.
		t.Fatalf("got %d changes, want 3", len(batch.Changes))
	}

	// The boundary of the batch is the last commit frame of the WAL.
	reader, err := wal.Open(path + "-wal")
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer reader.Close()
	scan, err := reader.Scan()
	if err != nil {
		t.Fatalf("scan WAL: %v", err)
	}
	if batch.To.Frame != scan.CommitFrame {
		t.Errorf("the batch ends at frame %d, the WAL holds %d committed frames", batch.To.Frame, scan.CommitFrame)
	}
	if notified.Frames <= 0 {
		t.Error("the hook did not report a frame count")
	}
}

// TestResumeAfterRestart checks that the driver continues at the persisted
// capture position after a restart and that it does not lose an increment.
func TestResumeAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restart.db")
	dsn := path + "?_journal_mode=WAL"

	ctx := context.Background()
	d, err := Open(ctx, Config{DSN: dsn, ScanInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := d.DB().Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	insert(t, d, "before")

	batch, err := d.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(batch.Changes) == 0 {
		t.Fatal("the transaction before the restart was not captured")
	}
	resume := batch.To
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A transaction that is committed while the driver is not running.
	rogue, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := rogue.Exec("INSERT INTO t(v) VALUES('during')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rogue.Close()

	// The driver continues at the persisted position.
	d2, err := Open(ctx, Config{DSN: dsn, ScanInterval: 20 * time.Millisecond, ResumePosition: &resume})
	if err != nil {
		t.Fatalf("Open again: %v", err)
	}
	defer d2.Close()

	insert(t, d2, "after")

	var values []string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		batch, err := d2.Scan(ctx)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		for _, c := range batch.Changes {
			for _, p := range c.Pages {
				_ = p
			}
		}
		if len(batch.Changes) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	rows, err := d2.DB().Query("SELECT v FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		values = append(values, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(values) != 3 {
		t.Fatalf("the database holds %d rows, want 3", len(values))
	}
}

// TestCheckpointDoesNotBlockTheWrites checks that the regular checkpoint
// does not make a business write fail while the database is written
// continuously.
func TestCheckpointDoesNotBlockTheWrites(t *testing.T) {
	d, _ := newTestDriver(t, Config{
		ScanInterval:    10 * time.Millisecond,
		ReclaimBytes:    64 << 10,
		ReclaimInterval: 100 * time.Millisecond,
	})
	createTable(t, d)

	errs := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('x')"); err != nil {
				errs <- err
				return
			}
			// The consumer acknowledges what it captured, so that the
			// checkpoint may run.
			if batch, err := d.Scan(context.Background()); err == nil && len(batch.Changes) > 0 {
				d.CaptureAck(context.Background(), batch.To)
			}
			time.Sleep(time.Millisecond)
		}
		errs <- nil
	}()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("a business write failed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the writer did not finish")
	}
}

// TestTheSnapshotOfADatabaseWithoutAWriteAheadLog checks that a snapshot
// works when the write-ahead log holds no frame.
//
// The log holds no frame when the database was not written to yet: the
// database file holds every change that was committed and the snapshot is
// the file alone.
func TestTheSnapshotOfADatabaseWithoutAWriteAheadLog(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	ctx := context.Background()

	snapPath := filepath.Join(t.TempDir(), "snapshot.db")
	snap, err := d.Snapshot(ctx, snapPath)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Bytes == 0 {
		t.Error("the snapshot holds no bytes")
	}
	if err := checkIntegrity(snapPath); err != nil {
		t.Errorf("the snapshot is not a valid database: %v", err)
	}

	// The capture continues after the snapshot.
	createTable(t, d)
	insert(t, d, "a")
	batch, err := d.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(batch.Changes) == 0 {
		t.Error("the increments after the snapshot were lost")
	}
}

// TestSnapshotMatchesThePrimaryPageByPage checks that the snapshot and the
// increments that follow it bring the database to the state of the primary,
// page by page.
func TestSnapshotMatchesThePrimaryPageByPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "primary.db")
	dsn := path + "?_journal_mode=WAL"
	ctx := context.Background()

	d, err := Open(ctx, Config{DSN: dsn, ScanInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := d.DB().Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for range 100 {
		insert(t, d, "value")
	}
	if _, err := d.Scan(ctx); err != nil {
		t.Fatalf("Scan: %v", err)
	}

	snapPath := filepath.Join(dir, "snapshot.db")
	snap, err := d.Snapshot(ctx, snapPath)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for range 50 {
		insert(t, d, "later")
	}
	batch, err := d.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(batch.Changes) == 0 {
		t.Fatal("the increments after the snapshot were lost")
	}
	for _, c := range batch.Changes {
		if c.Pos.Frame <= snap.Pos.Frame {
			continue
		}
		if err := applyPages(snapPath, path, pageMap(c), c.CommitPages, d.pageSize); err != nil {
			t.Fatalf("apply pages: %v", err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Write the whole WAL of the primary back, so that its file holds the
	// same state as the snapshot.
	rogue, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := rogue.Exec("PRAGMA main.wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	rogue.Close()

	compareFiles(t, path, snapPath, d.pageSize)
}

// compareFiles compares two database files page by page.
//
// The change counter of page 1 is left out, it counts the write
// transactions of a database and not its content.
func compareFiles(t *testing.T, want, got string, pageSize int) {
	t.Helper()
	a, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("read %s: %v", want, err)
	}
	b, err := os.ReadFile(got)
	if err != nil {
		t.Fatalf("read %s: %v", got, err)
	}
	if len(a) != len(b) {
		t.Fatalf("%s holds %d bytes, %s holds %d bytes", want, len(a), got, len(b))
	}
	for offset := 0; offset < len(a); offset += pageSize {
		pageA := a[offset : offset+pageSize]
		pageB := b[offset : offset+pageSize]
		if offset == 0 {
			// The change counter and the number of the change it belongs
			// to are not part of the content.
			pageA = append([]byte(nil), pageA...)
			pageB = append([]byte(nil), pageB...)
			for _, i := range []int{24, 25, 26, 27, 92, 93, 94, 95} {
				pageA[i] = 0
				pageB[i] = 0
			}
		}
		if !bytes.Equal(pageA, pageB) {
			t.Fatalf("the page at offset %d differs", offset)
		}
	}
}
