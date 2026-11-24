package backup

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/segment"
)

// startSource starts a source that serves its listener.
//
// It returns the address of the listener.
func startSource(t *testing.T, cfg Config, src protocol.SegmentSource, provider protocol.SnapshotProvider) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s, err := protocol.NewSourceWithSnapshot(cfg.Protocol, src, provider)
	if err != nil {
		t.Fatalf("NewSourceWithSnapshot: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.Serve(ctx, ln)
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// startPeer opens a backup and lets it run.
func startPeer(t *testing.T, cfg Config, addr string) *Peer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	go p.Run(ctx, addr)
	return p
}

// testSnapshotProvider returns a provider that sends the bytes of a
// database as the snapshot of the source.
func testSnapshotProvider(t *testing.T, path, databaseID string, epoch, baseline uint64) *fakeProvider {
	t.Helper()
	return &fakeProvider{
		meta: protocol.SnapshotMeta{
			FormatVersion: 1,
			DatabaseID:    segment.DatabaseID(databaseID),
			StreamID:      1,
			Epoch:         epoch,
			BaselineSeq:   baseline,
			PageSize:      testPageSize,
		},
		data: mustRead(t, path),
	}
}

func TestPeerInstallsTheSnapshotAndAppliesTheChain(t *testing.T) {
	root := t.TempDir()
	cfg := testConfig(t, root)
	primary := newTestDatabase(t, t.TempDir(), "primary.db", true)

	src := &fakeSource{}
	src.epoch = 3
	provider := testSnapshotProvider(t, primary, cfg.Protocol.DatabaseID, 3, 10)
	addr := startSource(t, cfg, src, provider)
	p := startPeer(t, cfg, addr)

	// The backup takes the snapshot as its baseline.
	if !waitFor(t, func() bool { return p.Stats().AppliedSeq == 10 }) {
		t.Fatalf("the backup applied the sequence number %d, want the baseline 10", p.Stats().AppliedSeq)
	}
	if p.Stats().Epoch != 3 {
		t.Errorf("the epoch of the backup is %d, want 3", p.Stats().Epoch)
	}
	backup := filepath.Join(root, cfg.Protocol.DatabaseID, dbName)
	if rows := query(t, backup); len(rows) != 1 || rows[0] != "first" {
		t.Errorf("the backup holds %v, want the rows of the snapshot", rows)
	}

	// The chain that follows the baseline is applied.
	write(t, primary, "second")
	tx := transaction(t, primary, 11)
	seg := testSegment(t, cfg.Protocol.DatabaseID, 3, 11, tx)
	src.add(protocol.Segment{SeqStart: 11, SeqEnd: 11, Epoch: 3, Bytes: seg})
	if !waitFor(t, func() bool { return p.Stats().AppliedSeq == 11 }) {
		t.Fatalf("the backup applied the sequence number %d, want 11", p.Stats().AppliedSeq)
	}
	if rows := query(t, backup); len(rows) != 2 {
		t.Errorf("the backup holds %v, want the rows of the primary", rows)
	}
}

func TestPeerDoesNotTouchTheDatabaseWhileItIsPaused(t *testing.T) {
	root := t.TempDir()
	cfg := testConfig(t, root)
	primary := newTestDatabase(t, t.TempDir(), "primary.db", true)

	src := &fakeSource{}
	src.epoch = 3
	provider := testSnapshotProvider(t, primary, cfg.Protocol.DatabaseID, 3, 10)
	addr := startSource(t, cfg, src, provider)
	p := startPeer(t, cfg, addr)
	if !waitFor(t, func() bool { return p.Stats().AppliedSeq == 10 }) {
		t.Fatalf("the backup applied the sequence number %d, want the baseline 10", p.Stats().AppliedSeq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if p.Stats().State != StatePaused {
		t.Errorf("the state of the backup is %q, want %q", p.Stats().State, StatePaused)
	}
	backup := filepath.Join(root, cfg.Protocol.DatabaseID, dbName)
	before, err := os.Stat(backup)
	if err != nil {
		t.Fatalf("stat the backup: %v", err)
	}
	sum := checksum(t, backup)

	// The source keeps capturing, the backup does not touch its database.
	write(t, primary, "second")
	tx := transaction(t, primary, 11)
	seg := testSegment(t, cfg.Protocol.DatabaseID, 3, 11, tx)
	src.add(protocol.Segment{SeqStart: 11, SeqEnd: 11, Epoch: 3, Bytes: seg})
	time.Sleep(500 * time.Millisecond)
	after, err := os.Stat(backup)
	if err != nil {
		t.Fatalf("stat the backup: %v", err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Error("the backup database was written while the backup was paused")
	}
	if checksum(t, backup) != sum {
		t.Error("the content of the backup database changed while the backup was paused")
	}
	// The business may read the backup database while the backup is paused.
	if rows := query(t, backup); len(rows) != 1 {
		t.Errorf("the backup holds %v, want the rows of the snapshot", rows)
	}

	// The resume brings the backup back to the chain.
	if err := p.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !waitFor(t, func() bool { return p.Stats().AppliedSeq == 11 }) {
		t.Fatalf("the backup applied the sequence number %d, want 11", p.Stats().AppliedSeq)
	}
	if rows := query(t, backup); len(rows) != 2 {
		t.Errorf("the backup holds %v, want the rows of the primary", rows)
	}
}

func TestPeerAsksForANewSnapshotWhenTheChainIsBroken(t *testing.T) {
	root := t.TempDir()
	cfg := testConfig(t, root)
	primary := newTestDatabase(t, t.TempDir(), "primary.db", true)

	src := &fakeSource{}
	src.epoch = 3
	provider := testSnapshotProvider(t, primary, cfg.Protocol.DatabaseID, 3, 10)
	addr := startSource(t, cfg, src, provider)
	p := startPeer(t, cfg, addr)
	if !waitFor(t, func() bool { return p.Stats().AppliedSeq == 10 }) {
		t.Fatalf("the backup applied the sequence number %d, want the baseline 10", p.Stats().AppliedSeq)
	}

	// The chain of the source is reset, the epoch of the source changes.
	src.mu.Lock()
	src.epoch = 5
	src.mu.Unlock()
	provider.mu.Lock()
	provider.meta.Epoch = 5
	provider.meta.BaselineSeq = 20
	provider.mu.Unlock()

	// The segment of the new chain does not belong to the chain of the
	// backup, the backup asks for a new snapshot.
	write(t, primary, "second")
	tx := transaction(t, primary, 21)
	seg := testSegment(t, cfg.Protocol.DatabaseID, 5, 21, tx)
	src.add(protocol.Segment{SeqStart: 21, SeqEnd: 21, Epoch: 5, Bytes: seg})

	// The backup takes the new baseline.
	if !waitFor(t, func() bool { return p.Stats().AppliedSeq == 20 }) {
		t.Fatalf("the backup applied the sequence number %d, want the baseline 20", p.Stats().AppliedSeq)
	}
	if p.Stats().Epoch != 5 {
		t.Errorf("the epoch of the backup is %d, want 5", p.Stats().Epoch)
	}
	if p.Stats().SnapshotRequired == 0 {
		t.Error("the backup did not ask for a snapshot")
	}
}

func TestPeerStaysResumableWhenTheSourceDoesNotAnswerThePause(t *testing.T) {
	root := t.TempDir()
	cfg := testConfig(t, root)
	primary := newTestDatabase(t, t.TempDir(), "primary.db", true)

	src := &fakeSource{epoch: 3, block: make(chan struct{})}
	t.Cleanup(func() { close(src.block) })
	provider := testSnapshotProvider(t, primary, cfg.Protocol.DatabaseID, 3, 10)
	addr := startSource(t, cfg, src, provider)
	p := startPeer(t, cfg, addr)
	if !waitFor(t, func() bool { return p.Stats().AppliedSeq == 10 }) {
		t.Fatalf("the backup applied the sequence number %d, want the baseline 10", p.Stats().AppliedSeq)
	}

	// The source does not answer the pause: its send loop is held.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := p.Pause(ctx); err == nil {
		t.Fatal("the pause did not fail although the source did not answer")
	}
	// The backup is paused and it can be resumed: the resume puts the
	// source and the backup back in sync.
	if p.Stats().State != StatePaused {
		t.Errorf("the state of the backup is %q, want %q", p.Stats().State, StatePaused)
	}
	if err := p.Resume(context.Background()); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if p.Stats().State != StateSyncing {
		t.Errorf("the state of the backup is %q, want %q", p.Stats().State, StateSyncing)
	}
}

func TestPeerRefusesToResumeWhileTheBusinessHoldsTheDatabase(t *testing.T) {
	root := t.TempDir()
	cfg := testConfig(t, root)
	primary := newTestDatabase(t, t.TempDir(), "primary.db", true)

	src := &fakeSource{}
	src.epoch = 3
	provider := testSnapshotProvider(t, primary, cfg.Protocol.DatabaseID, 3, 10)
	addr := startSource(t, cfg, src, provider)
	p := startPeer(t, cfg, addr)
	if !waitFor(t, func() bool { return p.Stats().AppliedSeq == 10 }) {
		t.Fatalf("the backup applied the sequence number %d, want the baseline 10", p.Stats().AppliedSeq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	// The business opened the backup database and left a journal behind.
	backup := filepath.Join(root, cfg.Protocol.DatabaseID, dbName)
	if err := os.WriteFile(backup+"-journal", []byte("the journal of the business"), 0o644); err != nil {
		t.Fatalf("write the journal: %v", err)
	}
	if err := p.Resume(ctx); err == nil {
		t.Fatal("the backup resumed while the business held the database")
	}
}
