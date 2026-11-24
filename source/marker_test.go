package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// TestJudgeChain checks the verdict of the continuity of the chain.
func TestJudgeChain(t *testing.T) {
	ack := spool.Ack{
		Seq: 42,
		Pos: spool.Pos{Salt1: 11, Salt2: 22, Frame: 87},
	}
	marker := &cleanMarker{Seq: ack.Seq, Pos: markerPosOf(ack.Pos)}

	for _, tc := range []struct {
		name   string
		ack    spool.Ack
		gen    driver.Position
		marker *cleanMarker
		want   verdict
	}{
		{
			name: "the chain is empty",
			ack:  spool.Ack{},
			gen:  driver.Position{Salt1: 11, Salt2: 22},
			want: verdictUnknown,
		},
		{
			name: "the driver does not know the generation",
			ack:  ack,
			gen:  driver.Position{},
			want: verdictUnknown,
		},
		{
			name: "the log was not reset",
			ack:  ack,
			gen:  driver.Position{Salt1: 11, Salt2: 22, Frame: 87},
			want: verdictContinuous,
		},
		{
			name:   "the log was reset and the marker describes the chain",
			ack:    ack,
			gen:    driver.Position{Salt1: 33, Salt2: 44, Frame: 3},
			marker: marker,
			want:   verdictContinuous,
		},
		{
			name: "the log was reset and there is no marker",
			ack:  ack,
			gen:  driver.Position{Salt1: 33, Salt2: 44, Frame: 3},
			want: verdictBroken,
		},
		{
			name:   "the log was reset and the marker describes another chain",
			ack:    ack,
			gen:    driver.Position{Salt1: 33, Salt2: 44, Frame: 3},
			marker: &cleanMarker{Seq: 41, Pos: markerPosOf(ack.Pos)},
			want:   verdictBroken,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := judgeChain(tc.ack, tc.gen, tc.marker); got != tc.want {
				t.Errorf("the verdict is %d, want %d", got, tc.want)
			}
		})
	}
}

// TestTheCloseMarksTheChainAsComplete checks that the runtime marks the
// chain of the segments as complete when it closes.
func TestTheCloseMarksTheChainAsComplete(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	p := startPeer(t, cfg)
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}
	ack, err := p.sp.Durable()
	if err != nil {
		t.Fatalf("the durable acknowledgement of the spool: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(cfg.Spool.Root, testDatabaseID, markerFile)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the runtime did not write the marker: %v", err)
	}
	m := readMarker(path)
	if m == nil {
		t.Fatal("the marker cannot be read")
	}
	if m.Seq != ack.Seq {
		t.Errorf("the marker names the sequence number %d, want %d", m.Seq, ack.Seq)
	}
	if m.Pos != markerPosOf(ack.Pos) {
		t.Errorf("the marker names the position %+v, want %+v", m.Pos, markerPosOf(ack.Pos))
	}

	// The durable acknowledgement covers every transaction of the chain and
	// the chain holds no transaction twice.
	segs, err := p.sp.Segments()
	if err != nil {
		t.Fatalf("the segments of the spool: %v", err)
	}
	var last uint64
	for _, s := range segs {
		last = s.SeqEnd
	}
	if last != ack.Seq {
		t.Errorf("the chain ends at the sequence number %d, want %d", last, ack.Seq)
	}
	for i, tx := range segmentTxs(t, cfg.Spool.Root) {
		if tx.Seq != uint64(i+1) {
			t.Fatalf("the transaction %d has the sequence number %d, want %d", i, tx.Seq, i+1)
		}
	}
}

// TestTheMarkerThatCannotBeParsedCountsAsNoMarker checks that the runtime
// does not trust a marker it cannot read.
func TestTheMarkerThatCannotBeParsedCountsAsNoMarker(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, markerFile)
	if err := os.WriteFile(path, []byte(`{"seq": 42, "pos":`), 0o644); err != nil {
		t.Fatalf("write the marker: %v", err)
	}
	if m := readMarker(path); m != nil {
		t.Errorf("the runtime read the marker %+v, want no marker", m)
	}
}

// TestTheCloseDoesNotMarkAChainThatIsFull checks that the runtime does not
// mark a chain that misses the transactions it could not write.
func TestTheCloseDoesNotMarkAChainThatIsFull(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	// The spool holds a few transactions only.
	cfg.Spool.MaxBytes = 64 << 10
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	for i := range 50 {
		insert(t, d, string(rune('a'+i)))
	}
	if !waitFor(t, func() bool { return p.Stats().SpoolFull }) {
		t.Fatal("the runtime did not report that the spool is full")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if m := readMarker(p.markerPath()); m != nil {
		t.Errorf("the runtime marked a chain that does not cover the primary database: %+v", m)
	}
}

// TestTheCloseDoesNotMarkAChainWhoseBarrierDidNotRun checks that the runtime
// does not mark a chain whose durable acknowledgement lags behind the
// capture cursor of the driver.
func TestTheCloseDoesNotMarkAChainWhoseBarrierDidNotRun(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Spool.Mode = spool.ModeNormal
	// The barrier runs once an hour, the transactions never reach it.
	cfg.Spool.SyncInterval = time.Hour
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	p := startPeer(t, cfg)
	if !waitFor(t, func() bool { return p.Stats().Driver.CaptureCursor.Frame > 0 }) {
		t.Fatal("the runtime did not capture the transaction")
	}
	if got := p.Stats().CapturedSeq; got != 0 {
		t.Fatalf("the runtime captured %d transactions, want 0", got)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if m := readMarker(p.markerPath()); m != nil {
		t.Errorf("the runtime marked a chain that does not cover the primary database: %+v", m)
	}
}

// TestTheChainContinuesAcrossAGenerationChange checks that the chain of the
// segments survives the reset of the write-ahead log.
//
// The runtime closes cleanly: the marker describes the end of the chain. The
// log is reset while the runtime is down, the chain covers the primary
// database nevertheless and the capture continues.
func TestTheChainContinuesAcrossAGenerationChange(t *testing.T) {
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
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The log is reset when the last connection of the database closes.
	if err := d.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}

	d2 := reopenTestDriver(t, path)
	cfg.Driver = d2
	p2 := startPeer(t, cfg)
	epoch := p2.Stats().Spool.Epoch

	insert(t, d2, "b")
	if !waitFor(t, func() bool { return p2.Stats().CapturedSeq >= 3 }) {
		t.Fatalf("the runtime captured %d transactions, want 3", p2.Stats().CapturedSeq)
	}
	st := p2.Stats()
	if st.SnapshotRequired {
		t.Errorf("the runtime asks for a new baseline: %s", st.SnapshotReason)
	}
	if st.Spool.Epoch != epoch {
		t.Errorf("the epoch of the spool is %d, want %d", st.Spool.Epoch, epoch)
	}
	// The chain continues across the generation: the sequence numbers stay
	// continuous.
	for i, tx := range segmentTxs(t, cfg.Spool.Root) {
		if tx.Seq != uint64(i+1) {
			t.Fatalf("the transaction %d has the sequence number %d, want %d", i, tx.Seq, i+1)
		}
	}
}

// TestTheChainIsRebuiltWhenTheMarkerIsMissing checks that the runtime starts
// a new chain when the log was reset and the marker of the last clean close
// is gone.
//
// The chain may have a hole: the backup needs a new baseline.
func TestTheChainIsRebuiltWhenTheMarkerIsMissing(t *testing.T) {
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
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}
	// The marker is gone: the runtime cannot prove that the chain covers
	// the primary database.
	if err := os.Remove(p.markerPath()); err != nil {
		t.Fatalf("remove the marker: %v", err)
	}

	d2 := reopenTestDriver(t, path)
	cfg.Driver = d2
	p2 := startPeer(t, cfg)
	epoch := p2.Stats().Spool.Epoch

	insert(t, d2, "b")
	if !waitFor(t, func() bool { return p2.Stats().SnapshotRequired }) {
		t.Fatal("the runtime does not report that the backup needs a new baseline")
	}
	st := p2.Stats()
	if st.Spool.Epoch != epoch+1 {
		t.Errorf("the epoch of the spool is %d, want %d", st.Spool.Epoch, epoch+1)
	}
	if st.Spool.Segments != 0 {
		t.Errorf("the spool holds %d segments, want 0", st.Spool.Segments)
	}
	// The new chain starts over: the sequence numbers start over.
	insert(t, d2, "c")
	if !waitFor(t, func() bool { return p2.Stats().CapturedSeq >= 1 }) {
		t.Fatal("the runtime did not capture the transaction that follows the new baseline")
	}
	txs := segmentTxs(t, cfg.Spool.Root)
	if len(txs) == 0 {
		t.Fatal("the chain of the new baseline holds no transaction")
	}
	if txs[0].Seq != 1 {
		t.Errorf("the chain starts at the sequence number %d, want 1", txs[0].Seq)
	}
}

// TestTheChainIsRebuiltWhenTheMarkerIsStale checks that a marker that does
// not describe the end of the chain any more does not prove that the chain
// covers the primary database.
//
// The runtime that captured the transactions did not write a marker: the
// marker of the last clean close is still on the disk and the chain moved
// since then.
func TestTheChainIsRebuiltWhenTheMarkerIsStale(t *testing.T) {
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
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	marker := readMarker(p.markerPath())
	if marker == nil {
		t.Fatal("the runtime did not write the marker")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}

	// The runtime captures more transactions: the chain moves.
	d2 := reopenTestDriver(t, path)
	cfg.Driver = d2
	p2 := startPeer(t, cfg)
	insert(t, d2, "b")
	if !waitFor(t, func() bool { return p2.Stats().CapturedSeq >= 3 }) {
		t.Fatalf("the runtime captured %d transactions, want 3", p2.Stats().CapturedSeq)
	}
	if err := p2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The marker on the disk is the one of the last clean close: it does
	// not describe the end of the chain any more.
	if err := writeMarker(p2.markerPath(), *marker); err != nil {
		t.Fatalf("write the marker: %v", err)
	}
	if err := d2.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}

	d3 := reopenTestDriver(t, path)
	cfg.Driver = d3
	p3 := startPeer(t, cfg)
	epoch := p3.Stats().Spool.Epoch

	insert(t, d3, "c")
	if !waitFor(t, func() bool { return p3.Stats().SnapshotRequired }) {
		t.Fatal("the runtime does not report that the backup needs a new baseline")
	}
	if got := p3.Stats().Spool.Epoch; got != epoch+1 {
		t.Errorf("the epoch of the spool is %d, want %d", got, epoch+1)
	}
}

// TestTheRuntimeRemovesTheMarkerOfAResetChain checks that the marker of a
// chain that is gone does not stay on the disk.
func TestTheRuntimeRemovesTheMarkerOfAResetChain(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	p := startPeer(t, cfg)
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The runtime starts again on the same spool.
	p2 := startPeer(t, cfg)
	if readMarker(p2.markerPath()) == nil {
		t.Fatal("the runtime did not write the marker")
	}

	// The chain of the segments is broken: the driver reports that a new
	// snapshot is needed.
	if err := p2.recoverChain(context.Background(), driver.ErrSnapshotRequired); err != nil {
		t.Fatalf("recoverChain: %v", err)
	}
	if m := readMarker(p2.markerPath()); m != nil {
		t.Errorf("the marker of the chain that is gone is still on the disk: %+v", m)
	}
}
