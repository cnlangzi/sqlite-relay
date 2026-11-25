//go:build e2e

package e2e

import (
	"os"
	"strings"
	"testing"
)

// TestTheRelayContinuesAfterACleanRestartOfThePrimary checks that the
// chain of the segments survives a clean restart of the primary.
//
// The primary stops writing, it closes the peer and it closes the driver.
// The write-ahead log of the primary database is deleted when the last
// connection to it closes: the log starts a new generation. The marker of
// the clean close proves that the chain covers the primary database: the
// backup keeps its baseline.
func TestTheRelayContinuesAfterACleanRestartOfThePrimary(t *testing.T) {
	r := newRelay(t)
	// The segments of the chain are kept: the chain has to survive the
	// restart of the primary.
	r.writeSettings(settings{Retention: 3600})
	r.startPrimary()
	r.createSchema()
	r.insert(100)
	r.startBackup()
	r.waitCatchUp()

	if err := r.primary.stop(); err != nil {
		t.Fatalf("the primary did not stop cleanly: %v", err)
	}
	if code := r.primary.exitCode(); code != 0 {
		t.Errorf("the primary exited with the code %d, want 0", code)
	}
	if _, err := os.Stat(r.walPath()); !os.IsNotExist(err) {
		t.Errorf("the write-ahead log of the primary database is still there: %v", err)
	}

	r.startPrimary()
	r.insert(50)
	r.waitCatchUp()

	if st := r.backupStats(); st.Snapshots != 1 {
		t.Errorf("the backup installed %d snapshots, want 1: the chain did not survive the restart", st.Snapshots)
	}
	r.compare()
}

// TestTheRelayRebuildsTheChainWhenTheSegmentsAreBroken checks that the
// relay rebuilds the chain when a segment of the spool is gone.
//
// The chain has a hole: the transactions of the hole are gone. The spool
// starts a new chain, the epoch is raised and the backup gets a new
// baseline.
func TestTheRelayRebuildsTheChainWhenTheSegmentsAreBroken(t *testing.T) {
	r := newRelay(t)
	// The segments are kept after the backup acknowledged them: the test
	// needs a chain of more than one segment.
	r.writeSettings(settings{SegmentMaxBytes: 64 << 10, Retention: 3600})
	r.startPrimary()
	r.createSchema()
	r.insert(200)
	r.startBackup()
	r.waitCatchUp()

	epoch := r.sourceStats().Spool.Epoch
	if err := r.primary.stop(); err != nil {
		t.Fatalf("the primary did not stop cleanly: %v", err)
	}
	files := r.segmentFiles()
	if len(files) < 3 {
		t.Fatalf("the spool holds %d segments, want at least 3 to break the chain", len(files))
	}
	// The first segment of the chain is not broken: the chain would start
	// behind it. A segment in the middle leaves a hole.
	if err := os.Remove(files[1]); err != nil {
		t.Fatalf("remove the segment: %v", err)
	}

	r.startPrimary()
	if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().Spool.Epoch > epoch }) {
		t.Errorf("the epoch of the chain did not change: %d", r.sourceStats().Spool.Epoch)
	}

	// The backup sees the new generation of the chain and asks for a new
	// baseline.
	if !waitFor(catchUpTimeout, func() bool { return r.backupStats().Snapshots == 2 }) {
		t.Errorf("the backup installed %d snapshots, want 2", r.backupStats().Snapshots)
	}
	r.insert(50)
	r.waitCatchUp()
	r.compare()
}

// TestTheRelayResumesAfterACrashOfThePrimary checks that the relay
// continues after the primary died.
//
// The write-ahead log of the primary database survives the crash: the log
// keeps its generation and the chain continues where it ended. The backup
// keeps its baseline.
func TestTheRelayResumesAfterACrashOfThePrimary(t *testing.T) {
	r := newRelay(t)
	// The segments of the chain are kept: the chain has to survive the
	// crash of the primary.
	r.writeSettings(settings{Retention: 3600})
	r.startPrimary()
	r.createSchema()
	r.insert(100)
	r.startBackup()
	r.waitCatchUp()

	err := r.primary.crash()
	if err == nil {
		t.Fatal("the primary was killed, want the error of the wait")
	}
	if !strings.Contains(err.Error(), "signal: killed") {
		t.Errorf("the primary ended with %v, want a process that was killed", err)
	}
	if _, err := os.Stat(r.walPath()); err != nil {
		t.Errorf("the write-ahead log of the primary database is gone: %v", err)
	}

	r.startPrimary()
	r.insert(50)
	r.waitCatchUp()

	if st := r.backupStats(); st.Snapshots != 1 {
		t.Errorf("the backup installed %d snapshots, want 1: the chain did not survive the crash", st.Snapshots)
	}
	r.compare()
}

// TestTheRelayRecoversFromACrashOfTheBackup checks that the backup
// continues after it died while it applied the increments.
//
// The backup persists the sequence number it applied: it continues where it
// stopped. No increment is applied twice and none is skipped.
func TestTheRelayRecoversFromACrashOfTheBackup(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()
	r.createSchema()
	r.insert(100)
	r.startBackup()
	r.waitCatchUp()
	applied := r.backupStats().AppliedSeq

	// A big transaction: the backup is applying it when it dies.
	r.bulk(2000)
	if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().CapturedSeq > applied }) {
		t.Fatalf("the source did not capture the increments: %+v", r.sourceStats())
	}
	if err := r.backup.crash(); err == nil {
		t.Fatal("the backup was killed, want the error of the wait")
	}

	r.startBackup()
	if !waitFor(catchUpTimeout, func() bool { return r.backupStats().AppliedSeq >= applied }) {
		t.Errorf("the backup applied %d transactions, want at least the %d it applied before the crash",
			r.backupStats().AppliedSeq, applied)
	}
	r.waitCatchUp()
	r.compare()
}

// TestTheBackupAsksForANewBaselineWhenTheHistoryIsGone checks that the
// backup gets a new baseline when the history it needs to continue is gone.
//
// The backup is away while the source collects the history of the chain.
// The chain of the source does not hold the transactions the backup applied
// any more: the source tells the backup that it needs a new baseline instead
// of skipping the increments silently.
func TestTheBackupAsksForANewBaselineWhenTheHistoryIsGone(t *testing.T) {
	r := newRelay(t)
	r.startPrimary()

	// The backup takes its baseline before the primary writes: the history
	// it holds ends where the chain of the source begins.
	r.startBackup()
	if !waitFor(catchUpTimeout, func() bool { return r.backupStats().Snapshots > 0 }) {
		t.Fatalf("the backup did not take the snapshot of the source: %+v", r.backupStats())
	}
	if err := r.backup.crash(); err == nil {
		t.Fatal("the backup was killed, want the error of the wait")
	}

	// The source captures the increments of the primary while the backup is
	// away.
	r.createSchema()
	r.insert(200)
	if !waitFor(catchUpTimeout, func() bool { return len(r.segmentFiles()) > 1 }) {
		t.Fatalf("the spool holds %d segments, want more than one", len(r.segmentFiles()))
	}

	// The primary dies: the write-ahead log survives and the chain keeps
	// its generation.
	if err := r.primary.crash(); err == nil {
		t.Fatal("the primary was killed, want the error of the wait")
	}

	// The history of the chain is gone: the first segment of the spool is
	// not there any more.
	files := r.segmentFiles()
	if len(files) == 0 {
		t.Fatal("the spool holds no segment")
	}
	if err := os.Remove(files[0]); err != nil {
		t.Fatalf("remove the segment: %v", err)
	}

	r.startPrimary()
	r.startBackup()

	// The source tells the backup that its history is not enough: the
	// backup asks for a new baseline.
	if !waitFor(catchUpTimeout, func() bool { return r.backupStats().SnapshotRequired > 0 }) {
		t.Fatalf("the backup did not ask for a new baseline: %+v", r.backupStats())
	}
	if !waitFor(catchUpTimeout, func() bool { return r.backupStats().Snapshots > 0 }) {
		t.Errorf("the backup did not install a new baseline: %+v", r.backupStats())
	}
	r.insert(50)
	r.waitCatchUp()
	r.compare()
}

// TestTheSourceStandsStillWhenTheSpoolIsFull checks that the capture
// stands still when the spool is full.
//
// The spool reached its limit: the capture waits and the frames of the
// write-ahead log stay in the log. The increments are not lost: the capture
// continues when the spool has room again.
func TestTheSourceStandsStillWhenTheSpoolIsFull(t *testing.T) {
	r := newRelay(t)
	r.writeSettings(settings{SpoolMaxBytes: 1 << 20})
	r.startPrimary()
	r.createSchema()
	r.startBackup()
	r.waitCatchUp()

	// The backup is away: the spool fills up with the increments that the
	// backup does not acknowledge.
	if err := r.backup.crash(); err == nil {
		t.Fatal("the backup was killed, want the error of the wait")
	}
	r.insert(2000)

	if !waitFor(catchUpTimeout, func() bool { return r.sourceStats().SpoolFull }) {
		t.Fatalf("the spool is not full: %+v", r.sourceStats().Spool)
	}
	// The frames of the log that the spool does not hold stay in the log.
	checker := &backfillChecker{}
	if err := checker.check(r.sourceStats()); err != nil {
		t.Error(err)
	}

	// The backup comes back: it applies the segments of the spool, the
	// space is released and the increments that did not fit into the spool
	// are captured.
	r.startBackup()
	r.waitCatchUp()
	r.compare()
}
