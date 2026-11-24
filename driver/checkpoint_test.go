package driver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestBackfillNeverPassesAck is the core invariant of the driver: SQLite
// must never write back more frames than the consumer has acknowledged.
//
// As long as that holds, SQLite cannot reset the WAL and overwrite frames
// the consumer has not persisted yet.
func TestBackfillNeverPassesAck(t *testing.T) {
	d, _ := newTestDriver(t, Config{
		ScanInterval: 10 * time.Millisecond,
		// Keep the WAL bounded, the test writes for a few seconds.
		ReclaimBytes:    1 << 20,
		ReclaimInterval: 100 * time.Millisecond,
	})
	createTable(t, d)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('x')"); err != nil {
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			batch, err := d.Scan(context.Background())
			if err != nil {
				return
			}
			if len(batch.Changes) > 0 {
				// The acknowledgement is delayed, so the backfill has to
				// wait for it.
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
				if err := d.CaptureAck(context.Background(), batch.To); err != nil {
					return
				}
			}
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := d.Stats()
		// A negative backfill count means that SQLite could not report the
		// state of the WAL, there is nothing to compare then.
		if st.BackfillFrames >= 0 &&
			st.AckPosition.Salt1 == st.CaptureCursor.Salt1 && st.AckPosition.Salt2 == st.CaptureCursor.Salt2 {
			if uint64(st.BackfillFrames) > st.AckPosition.Frame {
				close(stop)
				wg.Wait()
				t.Fatalf("nBackfill %d is ahead of the acknowledged frame %d", st.BackfillFrames, st.AckPosition.Frame)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

// TestCheckpointStaysBehindTheAck checks that a checkpoint does not write
// back frames the consumer has not acknowledged.
func TestCheckpointStaysBehindTheAck(t *testing.T) {
	d, _ := newTestDriver(t, Config{
		ScanInterval: 10 * time.Millisecond,
		// The reclaim must not interfere with the measurement.
		ReclaimBytes:    1 << 30,
		ReclaimInterval: time.Hour,
	})
	createTable(t, d)
	insert(t, d, "a")

	// Capture the transaction but do not acknowledge it.
	batch, err := d.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(batch.Changes) == 0 {
		t.Fatal("the transaction was not captured")
	}

	for range 5 {
		if _, err := d.Checkpoint(context.Background()); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
	}
	st := d.Stats()
	if st.BackfillFrames > 0 {
		t.Fatalf("the checkpoint wrote back %d frames that were not acknowledged", st.BackfillFrames)
	}

	// The frames are written back once the consumer acknowledges them.
	if err := d.CaptureAck(context.Background(), batch.To); err != nil {
		t.Fatalf("CaptureAck: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := d.Checkpoint(context.Background()); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if d.Stats().BackfillFrames > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the acknowledged frames were never written back")
}

// TestReclaimKeepsTheWALBounded checks that the WAL space is given back to
// SQLite while the consumer keeps up.
func TestReclaimKeepsTheWALBounded(t *testing.T) {
	d, path := newTestDriver(t, Config{
		ScanInterval:    10 * time.Millisecond,
		ReclaimBytes:    256 << 10,
		ReclaimInterval: 200 * time.Millisecond,
		ReclaimTimeout:  5 * time.Second,
	})
	createTable(t, d)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := d.DB().Exec("INSERT INTO t(v) VALUES(?)", fmt.Sprintf("value-%d", i)); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			batch, err := d.Scan(context.Background())
			if err != nil {
				return
			}
			if len(batch.Changes) > 0 {
				if err := d.CaptureAck(context.Background(), batch.To); err != nil {
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// The WAL has to be reset again and again, otherwise it grows without
	// bound.
	var minSize, maxSize int64 = -1, 0
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		info, err := os.Stat(path + "-wal")
		if err != nil {
			continue
		}
		if minSize < 0 || info.Size() < minSize {
			minSize = info.Size()
		}
		if info.Size() > maxSize {
			maxSize = info.Size()
		}
	}
	close(stop)
	wg.Wait()

	if maxSize <= 0 {
		t.Fatal("the WAL never grew")
	}
	if minSize < 0 {
		t.Fatal("the size of the WAL was never observed")
	}
	if minSize*4 > maxSize {
		t.Fatalf("the WAL was never reset: it stayed between %d and %d bytes", minSize, maxSize)
	}
}

// TestTruncateStartsANewGeneration checks that the WAL can be shrunk and
// that the increments continue in the new generation.
func TestTruncateStartsANewGeneration(t *testing.T) {
	d, path := newTestDriver(t, Config{
		ScanInterval:      10 * time.Millisecond,
		ReclaimBytes:      32 << 10,
		ReclaimInterval:   50 * time.Millisecond,
		ReclaimTimeout:    5 * time.Second,
		TruncateOnReclaim: true,
	})
	createTable(t, d)
	insert(t, d, "a")

	// Acknowledge everything so that the reclaim may run.
	for {
		batch, err := d.Scan(context.Background())
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if len(batch.Changes) == 0 {
			break
		}
		if err := d.CaptureAck(context.Background(), batch.To); err != nil {
			t.Fatalf("CaptureAck: %v", err)
		}
	}

	// Wait for the WAL to be truncated.
	deadline := time.Now().Add(5 * time.Second)
	truncated := false
	for time.Now().Before(deadline) {
		info, err := os.Stat(path + "-wal")
		if err == nil && info.Size() == 0 {
			truncated = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !truncated {
		info, _ := os.Stat(path + "-wal")
		size := int64(-1)
		if info != nil {
			size = info.Size()
		}
		st := d.Stats()
		t.Fatalf("the WAL was not truncated, its size is %d; reclaim=%+v ckpt=%+v cursor=%v ack=%v", size, st.Reclaim, st.Checkpoint, st.CaptureCursor, st.AckPosition)
	}

	// The increments continue in the new generation.
	before := d.cursorPosition()
	insert(t, d, "b")
	changes := scanAll(t, d)
	if len(changes) == 0 {
		t.Fatal("the increment after the truncate was lost")
	}
	if changes[len(changes)-1].Pos.Salt1 == before.Salt1 && changes[len(changes)-1].Pos.Salt2 == before.Salt2 {
		t.Error("the increment was not reported in the new generation")
	}
}

// TestSnapshotIsValidAndComplete checks that a snapshot is a valid database
// and that the increments after it bring the database up to date.
func TestSnapshotIsValidAndComplete(t *testing.T) {
	d, path := newTestDriver(t, Config{ScanInterval: 10 * time.Millisecond})
	createTable(t, d)
	for i := range 50 {
		insert(t, d, fmt.Sprintf("value-%d", i))
	}
	scanAll(t, d)

	dir := t.TempDir()
	snapPath := filepath.Join(dir, "snapshot.db")
	snap, err := d.Snapshot(context.Background(), snapPath)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Pos.Frame == 0 {
		t.Fatal("the snapshot is not bound to a position")
	}
	if err := checkIntegrity(snapPath); err != nil {
		t.Fatalf("the snapshot is not a valid database: %v", err)
	}

	// The increments after the snapshot.
	for i := range 20 {
		insert(t, d, fmt.Sprintf("later-%d", i))
	}
	batch, err := d.Scan(context.Background())
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

	// The result has to hold the same rows as the primary.
	got := readRows(t, snapPath)
	want := readRows(t, path)
	if len(got) != len(want) {
		t.Fatalf("the snapshot holds %d rows, the primary holds %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d is %q in the snapshot and %q in the primary", i, got[i], want[i])
		}
	}
}

// TestSnapshotSurvivesAConcurrentWrite checks that a snapshot is not torn
// when the database is written while it is taken.
func TestSnapshotSurvivesAConcurrentWrite(t *testing.T) {
	d, _ := newTestDriver(t, Config{ScanInterval: 10 * time.Millisecond})
	createTable(t, d)
	for i := range 20 {
		insert(t, d, fmt.Sprintf("value-%d", i))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 5000 {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('noise')"); err != nil {
				return
			}
			// Keep the database small, the snapshot copies it.
			time.Sleep(time.Millisecond)
		}
	}()

	dir := t.TempDir()
	for i := range 5 {
		snapPath := filepath.Join(dir, fmt.Sprintf("snapshot-%d.db", i))
		snap, err := d.Snapshot(context.Background(), snapPath)
		if err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("Snapshot: %v", err)
		}
		if err := checkIntegrity(snapPath); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("the snapshot is not a valid database: %v", err)
		}
		if snap.Pos.Frame == 0 {
			close(stop)
			wg.Wait()
			t.Fatal("the snapshot is not bound to a position")
		}
	}
	close(stop)
	wg.Wait()
}

// pageMap turns the pages of a change into a map.
func pageMap(c Change) map[uint32][]byte {
	m := make(map[uint32][]byte, len(c.Pages))
	for _, p := range c.Pages {
		m[p.Pgno] = p.Data
	}
	return m
}

// readRows reads the rows of the table of a database.
func readRows(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT v FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query %s: %v", path, err)
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
