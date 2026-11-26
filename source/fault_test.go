package source

import (
	"os"
	"path/filepath"
	"testing"
)

// TestTheChainIsRebuiltWhenTheLogLostItsTail checks that the runtime starts
// a new chain when the primary database lost the transactions the chain
// holds.
//
// The tail of the write-ahead log is lost: the generation of the log did not
// change and the log holds fewer frames than the chain. The marker of the
// clean close does not help: the frames it names are gone.
func TestTheChainIsRebuiltWhenTheLogLostItsTail(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, path := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	p := startPeer(t, cfg)
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}
	epoch := p.Stats().Spool.Epoch
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The log survives the close of the driver: the connection keeps it
	// alive.
	keep := keepLogOpen(t, path)
	if err := d.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}
	// The database file is consistent: the copy of it can be opened.
	checkpoint(t, keep)

	// The database and its log are copied into another directory and the
	// tail of the log is lost.
	broken := t.TempDir()
	copyFile(t, path, filepath.Join(broken, testDatabaseID))
	copyFile(t, path+"-wal", filepath.Join(broken, testDatabaseID+"-wal"))
	truncateLog(t, filepath.Join(broken, testDatabaseID+"-wal"))

	d2 := reopenTestDriver(t, filepath.Join(broken, testDatabaseID))
	cfg.Driver = d2
	p2 := startPeer(t, cfg)

	if !waitFor(t, func() bool { return p2.Stats().SnapshotRequired }) {
		t.Fatalf("the runtime does not report that the backup needs a new baseline: %+v", p2.Stats())
	}
	if got := p2.Stats().Spool.Epoch; got != epoch+1 {
		t.Errorf("the epoch of the spool is %d, want %d", got, epoch+1)
	}
	if got := p2.Stats().Spool.Segments; got != 0 {
		t.Errorf("the spool holds %d segments, want 0", got)
	}
}

// TestTheChainIsRebuiltWhenTheLogIsGone checks that the runtime starts a new
// chain when the write-ahead log of the primary database is gone.
//
// The log is gone: the primary database holds the state of the last checkpoint
// and the generation of the log changed. The chain of the segments may have a
// hole: the backup needs a new baseline.
func TestTheChainIsRebuiltWhenTheLogIsGone(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, path := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	p := startPeer(t, cfg)
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}
	epoch := p.Stats().Spool.Epoch
	// The runtime dies: it does not write the marker of the clean close.
	p.cancel()
	if err := p.sp.Close(); err != nil {
		t.Fatalf("close the spool: %v", err)
	}
	// The log survives the close of the driver: the connection keeps it
	// alive.
	keep := keepLogOpen(t, path)
	if err := d.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}
	// The database file is consistent: the copy of it can be opened.
	checkpoint(t, keep)

	// The database and its log are copied into another directory and the log
	// is gone.
	broken := t.TempDir()
	copyFile(t, path, filepath.Join(broken, testDatabaseID))
	copyFile(t, path+"-wal", filepath.Join(broken, testDatabaseID+"-wal"))
	if err := os.Remove(filepath.Join(broken, testDatabaseID+"-wal")); err != nil {
		t.Fatalf("remove the write-ahead log: %v", err)
	}

	d2 := reopenTestDriver(t, filepath.Join(broken, testDatabaseID))
	cfg.Driver = d2
	p2 := startPeer(t, cfg)

	// The primary writes: the log of the new generation shows up and the
	// driver knows the generation of the log. The table of the copy may be
	// gone: the log held it and the log is gone.
	if _, err := d2.DB().Exec("CREATE TABLE t2(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create the table: %v", err)
	}

	if !waitFor(t, func() bool { return p2.Stats().SnapshotRequired }) {
		t.Fatalf("the runtime does not report that the backup needs a new baseline: %+v", p2.Stats())
	}
	if got := p2.Stats().Spool.Epoch; got != epoch+1 {
		t.Errorf("the epoch of the spool is %d, want %d", got, epoch+1)
	}
}

// TestTheChainIsRebuiltWhenTheLogIsCorrupt checks that the runtime starts a
// new chain when the write-ahead log of the primary database is corrupt.
//
// The frame of the last transaction of the chain is garbage: the log holds
// fewer frames than the chain and the generation of the log did not change.
func TestTheChainIsRebuiltWhenTheLogIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, path := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	p := startPeer(t, cfg)
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}
	epoch := p.Stats().Spool.Epoch
	ack, err := p.sp.Durable()
	if err != nil {
		t.Fatalf("the durable acknowledgement of the spool: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The log survives the close of the driver: the connection keeps it
	// alive.
	keep := keepLogOpen(t, path)
	if err := d.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}
	// The database file is consistent: the copy of it can be opened.
	checkpoint(t, keep)

	// The database and its log are copied into another directory and the
	// frame of the last transaction of the chain is garbage.
	broken := t.TempDir()
	copyFile(t, path, filepath.Join(broken, testDatabaseID))
	copyFile(t, path+"-wal", filepath.Join(broken, testDatabaseID+"-wal"))
	corruptLog(t, filepath.Join(broken, testDatabaseID+"-wal"), ack.Pos)

	d2 := reopenTestDriver(t, filepath.Join(broken, testDatabaseID))
	cfg.Driver = d2
	p2 := startPeer(t, cfg)

	if !waitFor(t, func() bool { return p2.Stats().SnapshotRequired }) {
		t.Fatalf("the runtime does not report that the backup needs a new baseline: %+v", p2.Stats())
	}
	if got := p2.Stats().Spool.Epoch; got != epoch+1 {
		t.Errorf("the epoch of the spool is %d, want %d", got, epoch+1)
	}
}

// TestTheRuntimeReportsAReadOnlyDirectory checks that the runtime reports the
// failure of a directory it cannot write and that it does not lose the
// transactions it captured.
//
// The directory of the database of the spool is read-only: the spool cannot
// write its segments. The runtime keeps the transactions and writes them when
// the directory is writable again.
func TestTheRuntimeReportsAReadOnlyDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	insert(t, d, "a")
	if !waitFor(t, func() bool { return len(segmentTxs(t, cfg.Spool.Root)) >= 2 }) {
		t.Fatalf("the spool holds %d transactions, want 2", len(segmentTxs(t, cfg.Spool.Root)))
	}

	// The directory of the database and the directories below it cannot be
	// written any more.
	spoolDir := filepath.Join(cfg.Spool.Root, testDatabaseID)
	setReadOnly(t, spoolDir, true)
	defer setReadOnly(t, spoolDir, false)

	insert(t, d, "b")
	if !waitFor(t, func() bool { return p.Stats().LastError != "" }) {
		t.Fatal("the runtime did not report the failure of the write")
	}

	// The transactions that could not be written are not lost.
	setReadOnly(t, spoolDir, false)
	if !waitFor(t, func() bool { return len(segmentTxs(t, cfg.Spool.Root)) >= 3 }) {
		t.Fatalf("the spool holds %d transactions, want 3", len(segmentTxs(t, cfg.Spool.Root)))
	}
}

// setReadOnly makes a directory and the directories below it read-only.
func setReadOnly(t *testing.T, dir string, readOnly bool) {
	t.Helper()
	mode := os.FileMode(0o755)
	if readOnly {
		mode = 0o555
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			setReadOnly(t, filepath.Join(dir, e.Name()), readOnly)
		}
	}
	if err := os.Chmod(dir, mode); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
}

// TestTheChainCounterOfTheEndOfTheChain checks the change counter of the end
// of the chain.
//
// The counter is the one of the newest page 1 image the chain holds.
func TestTheChainCounterOfTheEndOfTheChain(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d

	p := startPeer(t, cfg)
	// The chain is empty: the counter is unknown.
	if c := chainChangeCounter(p.sp); c.known {
		t.Errorf("the change counter of an empty chain is %d, want unknown", c.value)
	}

	// The transaction that created the table wrote page 1.
	createTable(t, d)
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 1 }) {
		t.Fatalf("the runtime captured %d transactions, want 1", p.Stats().CapturedSeq)
	}
	c := chainChangeCounter(p.sp)
	if !c.known {
		t.Fatal("the change counter of the chain is unknown")
	}
	primary, err := d.ChangeCounter()
	if err != nil {
		t.Fatalf("ChangeCounter: %v", err)
	}
	if c.value != primary {
		t.Errorf("the change counter of the chain is %d, want %d", c.value, primary)
	}

	// The transaction that follows did not write page 1: the counter of the
	// chain does not move.
	insert(t, d, "a")
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}
	c = chainChangeCounter(p.sp)
	if !c.known {
		t.Fatal("the change counter of the chain is unknown")
	}
	primary, err = d.ChangeCounter()
	if err != nil {
		t.Fatalf("ChangeCounter: %v", err)
	}
	if c.value != primary {
		t.Errorf("the change counter of the chain is %d, want %d", c.value, primary)
	}
}

// TestTheChainCounterOfAnUnreadableSegment checks that the change counter of
// the end of the chain is unknown when the segment cannot be read.
func TestTheChainCounterOfAnUnreadableSegment(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 1 }) {
		t.Fatalf("the runtime captured %d transactions, want 1", p.Stats().CapturedSeq)
	}
	segs, err := p.sp.Segments()
	if err != nil {
		t.Fatalf("the segments of the spool: %v", err)
	}
	if len(segs) == 0 {
		t.Fatal("the spool holds no segment")
	}
	if err := os.WriteFile(segs[0].Path, []byte("not a segment"), 0o644); err != nil {
		t.Fatalf("write the segment: %v", err)
	}
	if c := chainChangeCounter(p.sp); c.known {
		t.Errorf("the change counter of an unreadable segment is %d, want unknown", c.value)
	}
}
