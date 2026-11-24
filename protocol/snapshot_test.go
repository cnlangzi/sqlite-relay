package protocol

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// testProvider returns a provider that produces a snapshot of the given
// bytes.
func testProvider(cfg Config, epoch, baseline uint64, data []byte) *fakeProvider {
	return &fakeProvider{
		meta: SnapshotMeta{
			FormatVersion: 1,
			DatabaseID:    segment.DatabaseID(cfg.DatabaseID),
			StreamID:      cfg.StreamID,
			Epoch:         epoch,
			BaselineSeq:   baseline,
			PageSize:      pageSize,
		},
		data: data,
	}
}

func TestBackupAsksForASnapshotWhenItHasNoBaseline(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	startBackup(t, cfg, &fakeSink{needsSnapshot: true}, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	hello, ok := srv.lastHello()
	if !ok {
		t.Fatal("the backup did not connect")
	}
	if !hello.NeedsSnapshot {
		t.Error("the hello does not ask for a snapshot")
	}
}

func TestBackupDoesNotAskForASnapshotWhenItHasOne(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	sink.setApplied(5)
	sink.epoch = 3
	startBackup(t, cfg, sink, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	hello, ok := srv.lastHello()
	if !ok {
		t.Fatal("the backup did not connect")
	}
	if hello.NeedsSnapshot {
		t.Error("the hello asks for a snapshot, the backup has a baseline")
	}
	if hello.Epoch != 3 {
		t.Errorf("the hello names the epoch %d, want 3", hello.Epoch)
	}
}

func TestBackupInstallsTheSnapshotAndContinuesWithTheChain(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	src := &fakeSource{}
	src.setEpoch(3)
	provider := testProvider(cfg, 3, 7, []byte("the bytes of the snapshot"))
	sink := &fakeSink{needsSnapshot: true}
	addr, _, _ := startSourceWithSnapshot(t, cfg, src, provider)
	startBackup(t, cfg, sink, addr)

	if !waitFor(t, 5*time.Second, func() bool { return sink.installed() }) {
		t.Fatalf("the sink did not install the snapshot, the events are %v", sink.eventList())
	}
	events := sink.eventList()
	if !slices.Contains(events, "begin") || !slices.Contains(events, "commit") {
		t.Errorf("the events are %v, want the installation of the snapshot", events)
	}
	meta, data := sink.snapshot()
	if meta.BaselineSeq != 7 {
		t.Errorf("the manifest names the baseline sequence %d, want 7", meta.BaselineSeq)
	}
	if meta.Epoch != 3 {
		t.Errorf("the manifest names the epoch %d, want 3", meta.Epoch)
	}
	if meta.DatabaseID != segment.DatabaseID(cfg.DatabaseID) {
		t.Errorf("the manifest names the database %x, want %x", meta.DatabaseID, segment.DatabaseID(cfg.DatabaseID))
	}
	if string(data) != "the bytes of the snapshot" {
		t.Errorf("the sink received %q, want the bytes of the snapshot", data)
	}
	applied, err := sink.AppliedSeq(context.Background())
	if err != nil {
		t.Fatalf("AppliedSeq: %v", err)
	}
	if applied != 7 {
		t.Errorf("the sink holds the sequence number %d, want the baseline 7", applied)
	}

	// The chain continues after the baseline of the snapshot.
	src.add(Segment{SeqStart: 8, SeqEnd: 9, Bytes: testSegment(t, cfg.DatabaseID, 3, 8, 2)})
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 2 }) {
		t.Fatalf("the sink applied %v, want the transactions 8 and 9", sink.seqs())
	}
	if seqs := sink.seqs(); !slices.Equal(seqs, []uint64{8, 9}) {
		t.Errorf("the sink applied %v, want the transactions 8 and 9", seqs)
	}
}

func TestBackupFlushesBeforeItAcknowledges(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	srv.sink = sink
	startBackup(t, cfg, sink, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	srv.send(Batch{Segments: [][]byte{testSegment(t, cfg.DatabaseID, 0, 1, 3)}})
	if !waitFor(t, 5*time.Second, func() bool { return len(srv.acks()) == 1 }) {
		t.Fatalf("the backup sent %d acknowledgements, want 1", len(srv.acks()))
	}
	want := []string{"apply:1", "apply:2", "apply:3", "flush", "ack:3"}
	if events := sink.eventList(); !slices.Equal(events, want) {
		t.Errorf("the events are %v, want %v", events, want)
	}
}

func TestBackupSkipsTheSegmentsWhileItIsPaused(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	b := startBackup(t, cfg, sink, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Pause(ctx, 0); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	// The segment arrives while the backup is paused. It is not applied and
	// it is not acknowledged.
	srv.send(Batch{Segments: [][]byte{testSegment(t, cfg.DatabaseID, 0, 1, 3)}})
	time.Sleep(300 * time.Millisecond)
	if seqs := sink.seqs(); len(seqs) != 0 {
		t.Errorf("the sink applied %v while the backup was paused, want nothing", seqs)
	}
	if acks := srv.acks(); len(acks) != 0 {
		t.Errorf("the backup acknowledged %v while it was paused, want nothing", acks)
	}
}

func TestSourceRefusesASnapshotItCannotProduce(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	src := &fakeSource{}
	sink := &fakeSink{needsSnapshot: true}
	addr, _, _ := startSource(t, cfg, src)
	startBackup(t, cfg, sink, addr)

	// The source has no provider, the backup does not receive a snapshot.
	time.Sleep(300 * time.Millisecond)
	if sink.installed() {
		t.Error("the sink installed a snapshot the source could not produce")
	}
}
