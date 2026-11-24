package driver

import (
	"context"
	"database/sql"
	"errors"
	"os"
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
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("the error does not ask for a new snapshot: %v", err)
	}
	if !contains(err.Error(), "snapshot") {
		t.Fatalf("the error does not ask for a new snapshot: %v", err)
	}
}

// TestResumePositionOfAnotherGenerationNeedsASnapshot checks that a resume
// position of a generation that is not the current one asks for a new
// snapshot.
func TestResumePositionOfAnotherGenerationNeedsASnapshot(t *testing.T) {
	dir := t.TempDir()
	dsn := filepath.Join(dir, "resume.db") + "?_journal_mode=WAL"

	// The connection stays open so that the WAL survives.
	rogue, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rogue.Close()
	if _, err := rogue.Exec("CREATE TABLE t(x)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = Open(context.Background(), Config{
		DSN:            dsn,
		ResumePosition: &Position{Salt1: 1, Salt2: 2, Frame: 3},
	})
	if !errors.Is(err, ErrSnapshotRequired) {
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

// TestNewGenerationIsAcknowledgedAndReclaimed checks that the WAL can be
// reclaimed again after it started a new generation.
//
// The frame numbers of a new generation start over. An acknowledgement that
// compares them with the frames of the old generation is taken for a step
// back and dropped, the checkpoint manager never releases its read mark and
// the driver stops checkpointing and reclaiming.
func TestNewGenerationIsAcknowledgedAndReclaimed(t *testing.T) {
	d, path := newTestDriver(t, Config{
		ScanInterval:      10 * time.Millisecond,
		ReclaimBytes:      32 << 10,
		ReclaimInterval:   50 * time.Millisecond,
		ReclaimTimeout:    5 * time.Second,
		TruncateOnReclaim: true,
	})
	createTable(t, d)
	for _, v := range []string{"a", "b", "c", "d", "e"} {
		insert(t, d, v)
	}
	ackAll(t, d)

	// The reclaim shrinks the WAL, the next write starts a new generation.
	waitForTruncatedWAL(t, path)

	insert(t, d, "f")
	changes := scanAll(t, d)
	if len(changes) == 0 {
		t.Fatal("the increment after the truncate was lost")
	}
	last := changes[len(changes)-1].Pos
	if err := d.CaptureAck(context.Background(), last); err != nil {
		t.Fatalf("CaptureAck: %v", err)
	}
	if got := d.ackFrames(); got != last.Frame {
		t.Fatalf("the acknowledged position is %d, want %d", got, last.Frame)
	}
	if st := d.Stats(); st.AckPosition.Salt1 != last.Salt1 || st.AckPosition.Salt2 != last.Salt2 {
		t.Fatalf("the acknowledged position is %v, want the generation of %v", st.AckPosition, last)
	}

	// The WAL of the new generation is reclaimed as well.
	insert(t, d, "g")
	ackAll(t, d)
	waitForTruncatedWAL(t, path)
}

// TestTheBoundaryOfTheLogFollowsTheGeneration checks that the boundary of
// the log does not survive the snapshot that moved the capture cursor.
//
// The boundary belongs to the generation of the log that was read. A
// generation that is left behind does not say anything about the frames of
// the current one: a reset of the log that lost nothing must not ask for a
// new snapshot.
func TestTheBoundaryOfTheLogFollowsTheGeneration(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)
	insert(t, d, "a")
	scanAll(t, d)

	// The log holds more frames than the cursor captured.
	d.mu.Lock()
	d.walBoundary = 100
	d.mu.Unlock()

	// A snapshot moves the cursor into the current generation of the log.
	dir := t.TempDir()
	snap, err := d.Snapshot(context.Background(), filepath.Join(dir, "snapshot.db"))
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	d.mu.Lock()
	boundary := d.walBoundary
	d.mu.Unlock()
	if boundary != snap.Pos.Frame {
		t.Errorf("the boundary of the log is %d, want %d", boundary, snap.Pos.Frame)
	}

	// A reset of the log that lost no frame does not ask for a new
	// snapshot.
	next := Position{Salt1: snap.Pos.Salt1 + 1, Salt2: snap.Pos.Salt2}
	if err := d.handleGenerationChange(wal.Header{Salt1: next.Salt1, Salt2: next.Salt2}); err != nil {
		t.Errorf("handleGenerationChange: %v", err)
	}
}

// TestTheFramesThatPredateTheDriverAreNotLost checks that the frames that
// the log held before the driver started are not reported as lost.
//
// The consumer holds the increments that it has seen already and the driver
// does not know which ones they are. A reset of the log does not take them
// away from the consumer.
func TestTheFramesThatPredateTheDriverAreNotLost(t *testing.T) {
	d, _ := newTestDriver(t, Config{})
	createTable(t, d)
	insert(t, d, "a")
	scanAll(t, d)

	// The driver starts with a log that holds frames it has not captured.
	d.mu.Lock()
	d.cursor = Position{Salt1: 1, Salt2: 2, WALOffset: walOffset(0, d.pageSize)}
	d.cursorSet = true
	d.startFrames = 3
	d.walBoundary = 3
	d.mu.Unlock()

	// A reset of the log does not report the frames of the consumer.
	if err := d.handleGenerationChange(wal.Header{Salt1: 3, Salt2: 4}); err != nil {
		t.Errorf("handleGenerationChange: %v", err)
	}

	// The frames that the driver had a chance to capture are lost.
	d.mu.Lock()
	d.cursor = Position{Salt1: 1, Salt2: 2, Frame: 1, WALOffset: walOffset(1, d.pageSize)}
	d.cursorSet = true
	d.startFrames = 0
	d.walBoundary = 3
	d.mu.Unlock()

	err := d.handleGenerationChange(wal.Header{Salt1: 5, Salt2: 6})
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Errorf("the error %v does not ask for a new snapshot", err)
	}
}

// ackAll captures the increments of the driver and acknowledges them.
func ackAll(t *testing.T, d *Driver) {
	t.Helper()
	for {
		batch, err := d.Scan(context.Background())
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if len(batch.Changes) == 0 {
			return
		}
		if err := d.CaptureAck(context.Background(), batch.To); err != nil {
			t.Fatalf("CaptureAck: %v", err)
		}
	}
}

// waitForTruncatedWAL waits until the WAL of the database is empty.
func waitForTruncatedWAL(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		info, err := os.Stat(path + "-wal")
		if err == nil && info.Size() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	info, _ := os.Stat(path + "-wal")
	size := int64(-1)
	if info != nil {
		size = info.Size()
	}
	t.Fatalf("the WAL was not truncated, its size is %d", size)
}
