package driver

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// TestPreCheckpointedDatabaseIsAccepted checks that a database which was
// checkpointed before the driver took it over does not look like an
// unmanaged checkpoint.
//
// The first observation of the backfill count is the baseline, not a
// violation.
func TestPreCheckpointedDatabaseIsAccepted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pre.db")
	dsn := path + "?_journal_mode=WAL"

	rogue, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// The connection stays open so that the WAL survives.
	defer rogue.Close()
	if _, err := rogue.Exec("CREATE TABLE t(x)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for range 5 {
		if _, err := rogue.Exec("INSERT INTO t VALUES(1)"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if _, err := rogue.Exec("PRAGMA main.wal_checkpoint(PASSIVE)"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	d, err := Open(context.Background(), Config{DSN: dsn, ScanInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	time.Sleep(200 * time.Millisecond)
	if _, err := d.Scan(context.Background()); err != nil {
		t.Fatalf("Scan: %v", err)
	}
}

// TestBatchDoesNotRepeatCapturedTransactions checks that a capture round
// does not hand out transactions that the consumer has received already.
//
// The reader starts at the beginning of the WAL again when it is reset,
// which happens when the WAL moves while it is read.
func TestBatchDoesNotRepeatCapturedTransactions(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)
	insert(t, d, "a")
	insert(t, d, "b")

	first, err := d.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(first.Changes) != 3 {
		t.Fatalf("got %d changes, want 3", len(first.Changes))
	}

	d.resetReader()

	second, err := d.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, c := range second.Changes {
		if c.Pos.Frame <= first.To.Frame {
			t.Fatalf("the batch repeats the frame %d, the cursor is at %d", c.Pos.Frame, first.To.Frame)
		}
	}
}

// TestQuiesceBlocksPreparedStatements checks that a statement that was
// prepared before the quiesce is held back as well.
//
// The statement pool of database/sql keeps prepared statements around and
// reuses them without preparing them again, so the gate has to be applied
// when the statement runs.
func TestQuiesceBlocksPreparedStatements(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)

	stmt, err := d.DB().Prepare("INSERT INTO t(v) VALUES(?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()
	if _, err := stmt.Exec("warm"); err != nil {
		t.Fatalf("exec: %v", err)
	}

	if err := d.Quiesce(context.Background()); err != nil {
		t.Fatalf("Quiesce: %v", err)
	}
	defer d.Resume()

	done := make(chan error, 1)
	go func() {
		_, err := stmt.Exec("during")
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the statement was not held back: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := d.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the statement failed after the resume: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the statement did not continue after the resume")
	}
}

// TestQuiesceHonoursTheCallerContext checks that a writer that waits for
// the resume can be cancelled by its own context.
func TestQuiesceHonoursTheCallerContext(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)

	if err := d.Quiesce(context.Background()); err != nil {
		t.Fatalf("Quiesce: %v", err)
	}
	defer d.Resume()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := d.DB().BeginTx(ctx, nil); err == nil {
		t.Fatal("the transaction was not held back")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the transaction did not follow the context of the caller")
	}
}

// TestQuiesceBlocksReadsWhenAsked checks that read transactions are held back
// when the caller asks for it.
func TestQuiesceBlocksReadsWhenAsked(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)

	if err := d.QuiesceWith(context.Background(), QuiesceOptions{BlockReads: true}); err != nil {
		t.Fatalf("Quiesce: %v", err)
	}
	defer d.Resume()

	done := make(chan error, 1)
	go func() {
		rows, err := d.DB().Query("SELECT count(*) FROM t")
		if err == nil {
			rows.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the read was not held back: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := d.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the read failed after the resume: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read did not continue after the resume")
	}
}

// TestResetOfTheWALWithUncapturedFramesIsReported checks that the driver
// notices a reset of the WAL that lost frames it had not captured.
func TestResetOfTheWALWithUncapturedFramesIsReported(t *testing.T) {
	d, _ := newTestDriver(t, Config{ScanInterval: 10 * time.Millisecond})
	createTable(t, d)
	insert(t, d, "a")
	scanAll(t, d)

	// A transaction that the driver observes but does not capture.
	insert(t, d, "b")

	deadline := time.Now().Add(5 * time.Second)
	boundary := uint64(0)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		boundary = d.walBoundary
		d.mu.Unlock()
		if boundary > d.cursorPosition().Frame {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if boundary <= d.cursorPosition().Frame {
		t.Fatal("the driver did not observe the frame count of the WAL")
	}

	err := d.handleGenerationChange(wal.Header{Salt1: 1, Salt2: 2})
	if err == nil {
		t.Fatal("the driver did not notice the reset of the WAL")
	}
	if !contains(err.Error(), "snapshot") {
		t.Fatalf("the error does not ask for a new snapshot: %v", err)
	}
}

// TestHookIsRestoredAfterAutocheckpoint checks that the driver puts its WAL
// hook back on a connection whose automatic checkpoint was switched on.
func TestHookIsRestoredAfterAutocheckpoint(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)

	conn, err := d.DB().Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "PRAGMA main.wal_autocheckpoint = 1000"); err != nil {
		t.Fatalf("pragma: %v", err)
	}

	err = d.checkInvariants()
	if err == nil {
		t.Fatal("the driver did not notice the automatic checkpoint")
	}
	if !contains(err.Error(), "wal_autocheckpoint") {
		t.Fatalf("the error does not name the pragma: %v", err)
	}

	// The hook is back, the commits are reported again.
	before := len(d.events)
	if _, err := conn.ExecContext(context.Background(), "INSERT INTO t(v) VALUES('after')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(d.events) > before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the hook of the connection was not put back")
}

// TestCaptureAckOfAnOlderGenerationIsIgnored checks that a late
// acknowledgement does not move the acknowledged position back.
func TestCaptureAckOfAnOlderGenerationIsIgnored(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)
	insert(t, d, "a")
	batch, err := d.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if err := d.CaptureAck(context.Background(), batch.To); err != nil {
		t.Fatalf("CaptureAck: %v", err)
	}

	// An acknowledgement of another generation says nothing about the
	// frames of the current one.
	old := batch.To
	old.Salt1++
	if err := d.CaptureAck(context.Background(), old); err != nil {
		t.Fatalf("CaptureAck: %v", err)
	}
	if got := d.ackFrames(); got != batch.To.Frame {
		t.Fatalf("the acknowledged position is %d, want %d", got, batch.To.Frame)
	}
}
