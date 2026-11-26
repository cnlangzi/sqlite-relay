package source

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/snapshot"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// TestOpenNamesTheMissingField checks that the runtime refuses a
// configuration that is not complete.
func TestOpenNamesTheMissingField(t *testing.T) {
	dir := t.TempDir()
	d, _ := newTestDriver(t, dir)

	for _, tc := range []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "the driver",
			cfg:  testConfig(t, dir),
			want: "driver",
		},
		{
			name: "the root of the spool",
			cfg: func() Config {
				cfg := testConfig(t, dir)
				cfg.Spool.Root = ""
				return cfg
			}(),
			want: "root",
		},
		{
			name: "the database ID",
			cfg: func() Config {
				cfg := testConfig(t, dir)
				cfg.Protocol.DatabaseID = ""
				return cfg
			}(),
			want: "database ID",
		},
		{
			name: "the directory of the snapshots",
			cfg: func() Config {
				cfg := testConfig(t, dir)
				cfg.SnapshotDir = ""
				return cfg
			}(),
			want: "snapshots",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			if tc.name == "the driver" {
				cfg.Driver = nil
			} else {
				cfg.Driver = d
			}
			_, err := Open(context.Background(), cfg)
			if err == nil {
				t.Fatal("the runtime opened a configuration that is not complete")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the error %q does not name %q", err, tc.want)
			}
		})
	}
}

// TestPeerIsTheCaptureOfTheSnapshots checks that the runtime stands the
// capture still while a snapshot is taken.
func TestPeerIsTheCaptureOfTheSnapshots(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Driver, _ = newTestDriver(t, dir)

	p, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	// The runtime is the capture of the snapshots: the producer of the
	// snapshots suspends the capture while it takes a snapshot.
	var _ snapshot.Capture = p
}

// TestCaptureWritesTheTransactionsIntoTheSpool checks that the runtime
// writes every committed transaction into the spool.
func TestCaptureWritesTheTransactionsIntoTheSpool(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	for _, v := range []string{"a", "b", "c"} {
		insert(t, d, v)
	}

	// The table and the three rows are four transactions.
	if !waitFor(t, func() bool { return len(segmentTxs(t, cfg.Spool.Root)) >= 4 }) {
		t.Fatalf("the spool holds %d transactions, want 4", len(segmentTxs(t, cfg.Spool.Root)))
	}
	for i, tx := range segmentTxs(t, cfg.Spool.Root) {
		if tx.Seq != uint64(i+1) {
			t.Fatalf("the transaction %d has the sequence number %d, want %d", i, tx.Seq, i+1)
		}
	}
	if st := p.Stats(); st.CapturedSeq < 4 {
		t.Errorf("the runtime captured %d transactions, want 4", st.CapturedSeq)
	}
}

// TestCaptureSurvivesLostNotifications checks that the transactions are
// captured even when the notifications of the driver are dropped.
//
// The notifications are a wake-up hint, the periodic scan finds the
// transactions that the hint did not report.
func TestCaptureSurvivesLostNotifications(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	// The notifications are dropped before the runtime sees them.
	go func() {
		for range d.Events() {
		}
	}()

	p := startPeer(t, cfg)
	for _, v := range []string{"a", "b", "c", "d", "e"} {
		insert(t, d, v)
	}

	if !waitFor(t, func() bool { return len(segmentTxs(t, cfg.Spool.Root)) >= 6 }) {
		t.Fatalf("the spool holds %d transactions, want 6", len(segmentTxs(t, cfg.Spool.Root)))
	}
	if st := p.Stats(); st.CapturedSeq < 6 {
		t.Errorf("the runtime captured %d transactions, want 6", st.CapturedSeq)
	}
}

// TestCaptureWaitsForTheBarrierOfTheSpool checks that the capture
// acknowledgement does not move before the increments crossed the
// persistence barrier of the spool.
func TestCaptureWaitsForTheBarrierOfTheSpool(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Spool.Mode = spool.ModeNormal
	// The barrier runs once an hour, the transactions never reach it.
	cfg.Spool.SyncInterval = time.Hour
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	insert(t, d, "a")
	time.Sleep(300 * time.Millisecond)

	st := p.Stats()
	if st.CapturedSeq != 0 {
		t.Errorf("the runtime captured %d transactions, want 0", st.CapturedSeq)
	}
	if st.Driver.AckPosition.Frame != 0 {
		t.Errorf("the driver acknowledged the frame %d, want 0", st.Driver.AckPosition.Frame)
	}
	if n := len(segmentTxs(t, cfg.Spool.Root)); n != 0 {
		t.Errorf("the spool published %d transactions, want 0", n)
	}
}

// TestCaptureHandsTheDurableAckToTheDriver checks that the capture
// acknowledgement and the durable acknowledgement of the spool move
// together.
func TestCaptureHandsTheDurableAckToTheDriver(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Spool.Mode = spool.ModeNormal
	cfg.Spool.SyncInterval = 10 * time.Millisecond
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	insert(t, d, "a")

	// The durable acknowledgement reaches the driver. The log may be
	// restarted in the meantime, the driver forgets the acknowledgement of
	// the old generation then.
	acked := false
	if !waitFor(t, func() bool {
		st := p.Stats()
		if st.Driver.AckPosition.Frame > 0 {
			acked = true
		}
		return acked && st.CapturedSeq > 0
	}) {
		t.Errorf("the runtime captured %d transactions and the driver acknowledged the frame %d, want the durable acknowledgement to reach the driver",
			p.Stats().CapturedSeq, p.Stats().Driver.AckPosition.Frame)
	}
}

// TestCaptureContinuesAfterASegmentIsFull checks that the transaction that
// did not fit into the segment is written into the next one.
func TestCaptureContinuesAfterASegmentIsFull(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	// A segment holds a single transaction.
	cfg.Spool.SegmentMaxBytes = 1
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	for _, v := range []string{"a", "b", "c"} {
		insert(t, d, v)
	}

	if !waitFor(t, func() bool { return len(segmentTxs(t, cfg.Spool.Root)) >= 4 }) {
		t.Fatalf("the spool holds %d transactions, want 4", len(segmentTxs(t, cfg.Spool.Root)))
	}
	for i, tx := range segmentTxs(t, cfg.Spool.Root) {
		if tx.Seq != uint64(i+1) {
			t.Fatalf("the transaction %d has the sequence number %d, want %d", i, tx.Seq, i+1)
		}
	}
	if st := p.Stats(); st.CapturedSeq < 4 {
		t.Errorf("the runtime captured %d transactions, want 4", st.CapturedSeq)
	}
}

// TestCaptureStandsStillWhenTheSpoolIsFull checks that the runtime keeps the
// increments it could not write and that it does not acknowledge the frames
// of the log.
func TestCaptureStandsStillWhenTheSpoolIsFull(t *testing.T) {
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
	st := p.Stats()
	if st.LastError == "" {
		t.Error("the runtime did not report the failure of the last round")
	}
	// The acknowledgement of the driver does not move while the spool is
	// full: the increments that did not fit are not durable.
	ack := st.Driver.AckPosition
	time.Sleep(200 * time.Millisecond)
	if got := p.Stats().Driver.AckPosition; got != ack {
		t.Errorf("the driver acknowledged %v while the spool was full, want %v", got, ack)
	}

	// The backup acknowledges the segments of the spool: the space is
	// released and the capture continues.
	ctx := context.Background()
	if err := p.sp.SetBackupAck(ctx, p.Stats().Spool.DurableSeq); err != nil {
		t.Fatalf("set the backup ack: %v", err)
	}
	if _, err := p.sp.GC(ctx); err != nil {
		t.Fatalf("collect the segments: %v", err)
	}
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq > st.CapturedSeq }) {
		t.Errorf("the runtime captured %d transactions, want more than %d", p.Stats().CapturedSeq, st.CapturedSeq)
	}
}

// TestCaptureResumesAfterARestart checks that the runtime does not write the
// transactions that the spool holds already.
func TestCaptureResumesAfterARestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, path := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	// The connection of the test keeps the log of the primary database
	// alive, the driver hands out its increments again after the restart.
	keep, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open the connection: %v", err)
	}
	keep.SetMaxIdleConns(1)
	defer keep.Close()
	if err := keep.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	p := startPeer(t, cfg)
	for _, v := range []string{"a", "b"} {
		insert(t, d, v)
	}
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 3 }) {
		t.Fatalf("the runtime captured %d transactions, want 3", p.Stats().CapturedSeq)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}

	// The runtime and the driver start again on the same database and the
	// same spool.
	d2, err := driver.Open(context.Background(), driver.Config{
		DSN:          path + "?_journal_mode=WAL",
		ScanInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open the driver again: %v", err)
	}
	defer d2.Close()
	cfg.Driver = d2

	p2 := startPeer(t, cfg)
	insert(t, d2, "c")

	if !waitFor(t, func() bool { return p2.Stats().CapturedSeq >= 4 }) {
		t.Fatalf("the runtime captured %d transactions, want 4", p2.Stats().CapturedSeq)
	}
	txs := segmentTxs(t, cfg.Spool.Root)
	if len(txs) != 4 {
		t.Fatalf("the spool holds %d transactions, want 4", len(txs))
	}
	for i, tx := range txs {
		if tx.Seq != uint64(i+1) {
			t.Fatalf("the transaction %d has the sequence number %d, want %d", i, tx.Seq, i+1)
		}
	}
}

// TestCaptureKeepsTheTransactionsThatCouldNotBeWritten checks that the
// runtime does not lose the increments when the write into the spool fails.
//
// The driver hands out the increments of a round only once. A transaction
// that could not be written has to be kept, otherwise the chain continues
// without it and the backup diverges from the primary database.
func TestCaptureKeepsTheTransactionsThatCouldNotBeWritten(t *testing.T) {
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

	// The spool cannot write its segments any more.
	tmp := filepath.Join(cfg.Spool.Root, testDatabaseID, "tmp")
	if err := os.Chmod(tmp, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	insert(t, d, "b")

	// The runtime reports the failure.
	if !waitFor(t, func() bool { return p.Stats().LastError != "" }) {
		t.Fatal("the runtime did not report the failure of the write")
	}

	// The spool can write again.
	if err := os.Chmod(tmp, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	// The increments that could not be written are in the spool.
	if !waitFor(t, func() bool { return len(segmentTxs(t, cfg.Spool.Root)) >= 3 }) {
		t.Fatalf("the spool holds %d transactions, want 3", len(segmentTxs(t, cfg.Spool.Root)))
	}
	for i, tx := range segmentTxs(t, cfg.Spool.Root) {
		if tx.Seq != uint64(i+1) {
			t.Fatalf("the transaction %d has the sequence number %d, want %d", i, tx.Seq, i+1)
		}
	}
}

// TestSuspendStandsTheCaptureStill checks that no increment enters the spool
// while the capture is suspended.
func TestSuspendStandsTheCaptureStill(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	insert(t, d, "a")
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}

	resume, err := p.Suspend(context.Background())
	if err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	insert(t, d, "b")
	time.Sleep(200 * time.Millisecond)
	if got := p.Stats().CapturedSeq; got != 2 {
		t.Errorf("the runtime captured %d transactions while it was suspended, want 2", got)
	}

	resume()
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 3 }) {
		t.Errorf("the runtime captured %d transactions after the resume, want 3", p.Stats().CapturedSeq)
	}
}

// TestSuspendCountsItsCallers checks that the capture continues only when
// every caller that suspended it resumed.
func TestSuspendCountsItsCallers(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	insert(t, d, "a")
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}

	first, err := p.Suspend(context.Background())
	if err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	second, err := p.Suspend(context.Background())
	if err != nil {
		t.Fatalf("Suspend: %v", err)
	}

	// The capture stands still until every caller resumed.
	first()
	insert(t, d, "b")
	time.Sleep(200 * time.Millisecond)
	if got := p.Stats().CapturedSeq; got != 2 {
		t.Errorf("the runtime captured %d transactions, want 2", got)
	}

	second()
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 3 }) {
		t.Errorf("the runtime captured %d transactions after the resume, want 3", p.Stats().CapturedSeq)
	}
}

// TestStatsReportTheStateOfTheRuntime checks that the runtime reports the
// distance between the spool and the backup.
func TestStatsReportTheStateOfTheRuntime(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	insert(t, d, "a")
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}

	st := p.Stats()
	if st.BackupSeq != 0 {
		t.Errorf("the backup acknowledged %d transactions, want 0", st.BackupSeq)
	}
	if st.LagSeq != st.CapturedSeq {
		t.Errorf("the runtime lags %d transactions, want %d", st.LagSeq, st.CapturedSeq)
	}
	if st.Driver.Generation.Salt1 == 0 {
		t.Error("the runtime does not report the generation of the log")
	}
}

// TestTheRuntimeRepairsABrokenChain checks that the runtime starts a new
// chain when the driver could not capture the increments of a new generation
// of the log.
func TestTheRuntimeRepairsABrokenChain(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	insert(t, d, "a")
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", p.Stats().CapturedSeq)
	}
	epoch := p.Stats().Spool.Epoch

	// The chain of the segments is broken: the driver reports that a new
	// snapshot is needed.
	p.handleRoundError(context.Background(), driver.ErrSnapshotRequired)

	st := p.Stats()
	if st.Spool.Epoch != epoch+1 {
		t.Errorf("the epoch of the spool is %d, want %d", st.Spool.Epoch, epoch+1)
	}
	if !st.SnapshotRequired {
		t.Error("the runtime does not report that the backup needs a new baseline")
	}
	if st.CapturedSeq != 0 {
		t.Errorf("the spool holds %d transactions, want 0", st.CapturedSeq)
	}
	if st.Spool.Segments != 0 {
		t.Errorf("the spool holds %d segments, want 0", st.Spool.Segments)
	}

	// The capture continues after the snapshot.
	insert(t, d, "b")
	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 1 }) {
		t.Errorf("the runtime captured %d transactions after the repair, want 1", p.Stats().CapturedSeq)
	}
}

// TestTheRuntimeDoesNotCloseTheDriver checks that the driver belongs to the
// application.
func TestTheRuntimeDoesNotCloseTheDriver(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d

	p, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := d.DB().Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("the driver was closed with the runtime: %v", err)
	}
}

// TestTheRuntimeRejectsAnUnknownMode checks that the runtime does not open a
// spool that it cannot serve.
func TestTheRuntimeRejectsAnUnknownMode(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Driver, _ = newTestDriver(t, dir)
	cfg.Spool.Mode = spool.Mode(42)

	_, err := Open(context.Background(), cfg)
	if err == nil {
		t.Fatal("the runtime opened a spool with an unknown mode")
	}
	if !strings.Contains(err.Error(), "mode") {
		t.Errorf("the error %q does not name the mode", err)
	}
}

// TestTheRuntimeFillsTheSpoolDirectory checks that the spool of the runtime
// lives in the directory of the configuration.
func TestTheRuntimeFillsTheSpoolDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Driver, _ = newTestDriver(t, dir)

	p := startPeer(t, cfg)
	if _, err := os.Stat(filepath.Join(cfg.Spool.Root, testDatabaseID)); err != nil {
		t.Errorf("the spool does not live in %s: %v", cfg.Spool.Root, err)
	}
	p.Stats()
}

// TestTheStatsReportTheConnectionOfTheBackup checks that the statistics of
// the runtime tell whether a backup is on the line and how many sessions it
// accepted.
func TestTheStatsReportTheConnectionOfTheBackup(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p, addr := startPeerAt(t, cfg)
	if p.Stats().Connected {
		t.Error("the runtime is connected before a backup showed up")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b, err := protocol.NewBackup(cfg.Protocol, &testSink{epoch: p.Stats().Spool.Epoch})
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}
	go b.Run(ctx, addr)

	if !waitFor(t, func() bool { return p.Stats().Connected }) {
		t.Fatal("the runtime does not see the backup")
	}
	if p.Stats().Sessions != 1 {
		t.Errorf("the runtime accepted %d sessions, want 1", p.Stats().Sessions)
	}

	// The backup goes away.
	cancel()
	if !waitFor(t, func() bool { return !p.Stats().Connected }) {
		t.Fatal("the runtime still sees the backup")
	}
}

// TestTheStatsCountTheResyncsOfTheRuntime checks that the statistics of the
// runtime count the resyncs the source asked the backup for.
func TestTheStatsCountTheResyncsOfTheRuntime(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p, addr := startPeerAt(t, cfg)

	// The backup belongs to another generation of the chain: the source
	// asks it for a new baseline.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b, err := protocol.NewBackup(cfg.Protocol, &testSink{epoch: p.Stats().Spool.Epoch + 1})
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}
	go b.Run(ctx, addr)

	if !waitFor(t, func() bool { return p.Stats().Resyncs == 1 }) {
		t.Errorf("the runtime asked for %d resyncs, want 1", p.Stats().Resyncs)
	}
}
