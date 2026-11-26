//go:build e2e

package e2e

import (
	"os"
	"reflect"
	"testing"
	"time"
)

// TestTheBackupHoldsTheDataOfThePrimary checks that the increments of the
// primary database end up in the database of the backup.
//
// The backup takes a snapshot of the primary as its baseline and it applies
// the increments that follow it. The two databases hold the same rows.
func TestTheBackupHoldsTheDataOfThePrimary(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.createSchema()
	r.insert(200)
	r.entries(100)

	r.startBackup()
	r.waitCatchUp()

	r.compare()
}

// TestTheBackupHoldsTheSchemaOfThePrimary checks that the schema of the
// primary database ends up in the database of the backup.
//
// A schema is a change of the pages of the database like any other change:
// the backup holds it without being told about it.
func TestTheBackupHoldsTheSchemaOfThePrimary(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.startBackup()
	r.waitCatchUp()

	for _, ddl := range []string{
		createAccounts,
		createEntries,
		createIndex,
		"ALTER TABLE accounts ADD COLUMN opened_at INTEGER",
		"DROP TABLE entries",
	} {
		if _, err := r.primary.do("ddl %s", ddl); err != nil {
			t.Fatalf("the ddl %q: %v", ddl, err)
		}
	}
	r.waitCatchUp()

	r.compareSchema()
}

// TestTheBackupGrowsAndShrinksWithThePrimary checks that the database of
// the backup grows and shrinks with the database of the primary.
//
// The pages of the primary database are copied into the backup: the number
// of the rows and the number of the pages are the same on both sides.
func TestTheBackupGrowsAndShrinksWithThePrimary(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.createSchema()
	r.insert(10)
	r.startBackup()
	r.waitCatchUp()

	before := r.pageCounts()

	r.bulk(5000)
	r.waitCatchUp()

	grown := r.pageCounts()
	if grown.primary <= before.primary {
		t.Errorf("the primary database holds %d pages, it held %d before the insert", grown.primary, before.primary)
	}
	if grown.backup != grown.primary {
		t.Errorf("the backup holds %d pages, the primary holds %d", grown.backup, grown.primary)
	}
	r.compare()

	if _, err := r.primary.do("delete"); err != nil {
		t.Fatalf("the delete: %v", err)
	}
	if _, err := r.primary.do("vacuum"); err != nil {
		t.Fatalf("the vacuum: %v", err)
	}
	r.waitCatchUp()

	shrunk := r.pageCounts()
	if shrunk.primary >= grown.primary {
		t.Errorf("the primary database holds %d pages, it held %d before the vacuum", shrunk.primary, grown.primary)
	}
	if shrunk.backup != shrunk.primary {
		t.Errorf("the backup holds %d pages, the primary holds %d", shrunk.backup, shrunk.primary)
	}
	r.compare()
}

// TestTheRelayKeepsThePrimaryWritableWhileTheBackupIsAway checks that the
// primary database stays writable while the backup is not connected.
//
// A transaction of the business does not wait for the network: the
// increments are captured into the local spool and the write-ahead log of
// the primary database does not grow without a bound.
func TestTheRelayKeepsThePrimaryWritableWhileTheBackupIsAway(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.createSchema()

	start := time.Now()
	r.insert(1000)
	elapsed := time.Since(start)
	if elapsed > 30*time.Second {
		t.Errorf("the 1000 commits took %s, want less than 30s", elapsed)
	}

	// The capture runs after the commits: the statistics are polled until
	// the runtime caught up.
	if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().LagSeq > 0 }) {
		t.Errorf("the source lags %d transactions, want a lag while the backup is away", r.sourceStats().LagSeq)
	}
	if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().Spool.UnackedSegments > 0 }) {
		t.Error("the spool holds no segment the backup did not acknowledge")
	}
	// The increments are durable in the spool: the driver writes the frames
	// of the log back.
	if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().Driver.Checkpoint.Checkpointed > 0 }) {
		t.Errorf("the driver did not write a frame back: %+v", r.sourceStats().Driver.Checkpoint)
	}
	if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().Driver.AckPosition.Frame > 0 }) {
		t.Error("the driver holds no acknowledged position")
	}

	// The backup comes back and catches up.
	r.startBackup()
	r.waitCatchUp()
	r.compare()
}

// TestTheWALIsReclaimedOnlyAfterTheSpoolIsDurable checks that the driver
// does not write a frame of the log back before the spool holds it.
//
// The reclaim of the write-ahead log waits for the acknowledgement of the
// capture: a frame that the spool does not hold may not be written back.
func TestTheWALIsReclaimedOnlyAfterTheSpoolIsDurable(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.createSchema()
	r.startBackup()
	r.waitCatchUp()

	var peak int
	var reclaimed bool
	var dropped int
	checker := &backfillChecker{}
	frames := r.sourceStats().Driver.WALFrames
	for range 20 {
		r.insert(50)
		st := r.sourceStats()
		if st.Driver.WALFrames > peak {
			peak = st.Driver.WALFrames
		}
		if st.Driver.Reclaim.Done {
			reclaimed = true
		}
		// The reclaim gives the space of the log back to SQLite: the log
		// starts over and holds the frames of the transactions that follow
		// the reclaim only.
		if st.Driver.WALFrames < frames {
			dropped += frames - st.Driver.WALFrames
		}
		frames = st.Driver.WALFrames
		if err := checker.check(st); err != nil {
			t.Fatal(err)
		}
	}
	if peak == 0 {
		t.Fatal("the write-ahead log did not grow")
	}
	if !reclaimed {
		t.Error("the write-ahead log was not reclaimed")
	}
	if dropped == 0 {
		t.Errorf("the write-ahead log holds %d frames, it never started over", frames)
	}
}

// TestTheWALStaysBoundedWhileTheBackupIsPaused checks that the write-ahead
// log of the primary database does not grow without a bound while the
// backup is paused.
//
// The backup holds the increments in the local spool: the log does not have
// to keep the frames that the spool holds already.
//
// The size of the file of the log is not the measure: SQLite does not
// shrink the file, it writes the frames back into the database file and
// reuses the space of the log. The frames the log holds are the measure:
// the log starts over when the driver gave the space back.
func TestTheWALStaysBoundedWhileTheBackupIsPaused(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.createSchema()
	r.startBackup()
	r.waitCatchUp()

	if _, err := r.backup.do("pause"); err != nil {
		t.Fatalf("the pause: %v", err)
	}

	var reclaimed bool
	var dropped int
	frames := r.sourceStats().Driver.WALFrames
	for range 40 {
		r.insert(50)
		st := r.sourceStats()
		if st.Driver.Reclaim.Done {
			reclaimed = true
		}
		if st.Driver.WALFrames < frames {
			dropped += frames - st.Driver.WALFrames
		}
		frames = st.Driver.WALFrames
	}
	if !reclaimed {
		t.Error("the driver did not give the space of the log back")
	}
	if dropped == 0 {
		t.Errorf("the log was not recycled: it holds %d frames", frames)
	}
}

// TestTheBackupServesTheBusinessWhileItIsPaused checks that the business
// may read the database of the backup while the backup is paused.
//
// The paused backup does not touch its database: the file belongs to the
// business until the backup is resumed.
func TestTheBackupServesTheBusinessWhileItIsPaused(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.createSchema()
	r.insert(100)
	r.startBackup()
	r.waitCatchUp()

	if _, err := r.backup.do("pause"); err != nil {
		t.Fatalf("the pause: %v", err)
	}

	before := fileStateOf(r.t, r.backupDBPath())

	// The business reads the database of the backup.
	db := r.openBackupDB()
	got, err := readRows(db, queryAccounts)
	if err != nil {
		t.Fatalf("the query on the database of the backup: %v", err)
	}
	if len(got.Rows) != 100 {
		t.Errorf("the business read %d rows, want 100", len(got.Rows))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close the database of the backup: %v", err)
	}

	// The relay did not touch the database of the backup.
	if after := fileStateOf(r.t, r.backupDBPath()); after != before {
		t.Errorf("the database of the backup changed while it was paused: %+v, want %+v", after, before)
	}

	// The primary writes while the backup is paused.
	r.insert(50)
	if rows := r.queryBackup(queryAccounts); len(rows.Rows) != 100 {
		t.Errorf("the backup holds %d rows, want the 100 rows it was paused with", len(rows.Rows))
	}

	if _, err := r.backup.do("resume"); err != nil {
		t.Fatalf("the resume: %v", err)
	}
	r.waitCatchUp()
	r.compare()
}

// TestThePausedBackupSurvivesARestart checks that a backup that was paused
// does not touch its database after a restart.
//
// The state of the backup holds the pause: the database belongs to the
// business until the backup is resumed.
func TestThePausedBackupSurvivesARestart(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.createSchema()
	r.insert(100)
	r.startBackup()
	r.waitCatchUp()

	if _, err := r.backup.do("pause"); err != nil {
		t.Fatalf("the pause: %v", err)
	}
	before := fileStateOf(r.t, r.backupDBPath())
	applied := r.backupStats().AppliedSeq

	if err := r.backup.crash(); err == nil {
		t.Error("the backup was killed, want the error of the wait")
	}

	// The backup comes back and finds the pause in its state.
	r.startBackup()
	st := r.backupStats()
	if st.State != "paused" {
		t.Errorf("the backup is in the state %q, want %q", st.State, "paused")
	}
	if after := fileStateOf(r.t, r.backupDBPath()); after != before {
		t.Errorf("the database of the backup changed while it was paused: %+v, want %+v", after, before)
	}

	// The source does not push the increments of the chain into the
	// database of the backup.
	r.insert(50)
	if got := r.backupStats().AppliedSeq; got != applied {
		t.Errorf("the backup applied %d transactions, want the %d it was paused with", got, applied)
	}

	if _, err := r.backup.do("resume"); err != nil {
		t.Fatalf("the resume: %v", err)
	}
	r.waitCatchUp()
	r.compare()
}

// TestTheStatusReportsTheStateOfTheRelay checks that the statistics of the
// two sides report the state of the relay.
func TestTheStatusReportsTheStateOfTheRelay(t *testing.T) {
	t.Run("the lag of the backup", func(t *testing.T) {
		r := newRelay(t)
		r.startPrimary()
		r.createSchema()
		r.insert(100)

		if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().LagSeq > 0 }) {
			t.Errorf("the source lags %d transactions, want a lag while the backup is away", r.sourceStats().LagSeq)
		}
		if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().Spool.UnackedSegments > 0 }) {
			t.Error("the spool holds no segment the backup did not acknowledge")
		}

		r.startBackup()
		r.waitCatchUp()
		if st := r.sourceStats(); st.LagSeq != 0 {
			t.Errorf("the source lags %d transactions, want no lag", st.LagSeq)
		}

		// The backup is away: the source reports the age of the last
		// acknowledgement.
		if err := r.backup.crash(); err == nil {
			t.Fatal("the backup was killed, want the error of the wait")
		}
		r.insert(50)
		if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().Lag > 0 }) {
			t.Errorf("the source does not report the age of the acknowledgement: %+v", r.sourceStats())
		}
	})

	t.Run("the spool that is full", func(t *testing.T) {
		r := newRelay(t)
		r.writeSettings(settings{SpoolMaxBytes: 1 << 20})
		r.startPrimary()
		r.createSchema()
		r.insert(2000)

		if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().SpoolFull }) {
			t.Fatalf("the spool is not full: %+v", r.sourceStats().Spool)
		}
		st := r.sourceStats()
		if st.LastError == "" {
			t.Error("the source does not report the failure of the capture")
		}
		// The frames of the log that the spool does not hold stay in the
		// log.
		if st.Driver.Checkpoint.Checkpointed > int(st.Driver.AckPosition.Frame) {
			t.Errorf("the driver wrote %d frames back, the spool holds %d of them",
				st.Driver.Checkpoint.Checkpointed, st.Driver.AckPosition.Frame)
		}
	})

	t.Run("the chain that is broken", func(t *testing.T) {
		r := newRelay(t)
		// The segments of the chain are kept and they are small: the chain
		// has to hold a segment that the test can remove.
		r.writeSettings(settings{Retention: 3600, SegmentMaxBytes: 64 << 10})
		r.startPrimary()
		r.createSchema()
		r.insert(100)
		r.startBackup()
		r.waitCatchUp()

		if err := r.primary.stop(); err != nil {
			t.Fatalf("the primary did not stop cleanly: %v", err)
		}
		files := r.segmentFiles()
		if len(files) < 3 {
			t.Fatalf("the spool holds %d segments, want at least 3 to break the chain", len(files))
		}
		// A segment in the middle of the chain leaves a hole: the chain
		// does not cover the primary database any more.
		if err := os.Remove(files[1]); err != nil {
			t.Fatalf("remove the segment: %v", err)
		}
		r.startPrimary()

		if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().SnapshotRequired }) {
			t.Errorf("the source does not report that the backup needs a new baseline: %+v", r.sourceStats())
		}
		if !waitFor(catchUpTimeout, func() bool { return r.backupStats().SnapshotRequired > 0 }) {
			t.Errorf("the backup did not ask for a new baseline: %+v", r.backupStats())
		}
	})
}

// compareSchema checks that the backup holds the schema of the primary
// database.
func (r *relay) compareSchema() {
	r.t.Helper()
	want := r.queryPrimary(querySchema)
	got := r.queryBackup(querySchema)
	if !reflect.DeepEqual(want, got) {
		r.t.Errorf("the backup does not hold the schema of the primary database:\nprimary: %v\nbackup:  %v",
			want.Rows, got.Rows)
	}
}

// pageCounts returns the number of the pages of the primary database and of
// the database of the backup.
func (r *relay) pageCounts() pageCount {
	r.t.Helper()
	return pageCount{
		primary: r.pageCount(r.queryPrimary("PRAGMA page_count")),
		backup:  r.pageCount(r.queryBackup("PRAGMA page_count")),
	}
}

// pageCount is the number of the pages of a database.
type pageCount struct {
	primary int
	backup  int
}

// pageCount returns the number of the pages of the answer of the pragma.
func (r *relay) pageCount(res rows) int {
	r.t.Helper()
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		r.t.Fatal("the pragma of the page count returned no rows")
	}
	n, ok := res.Rows[0][0].(float64)
	if !ok {
		r.t.Fatalf("the pragma of the page count returned %v", res.Rows[0][0])
	}
	return int(n)
}

// fileState describes the file of the database of the backup.
type fileState struct {
	size  int64
	mtime time.Time
}

// fileStateOf returns the state of a file.
func fileStateOf(t testing.TB, path string) fileState {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fileState{size: info.Size(), mtime: info.ModTime()}
}
