package protocol

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// startBackup starts a backup that dials the server.
func startBackup(t *testing.T, cfg Config, sink SegmentSink, addr string) *Backup {
	t.Helper()
	b, err := NewBackup(cfg, sink)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx, addr)
	return b
}

func TestBackupAppliesSegments(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	src := &fakeSource{}
	sink := &fakeSink{}
	addr, source, _ := startSource(t, cfg, src)
	startBackup(t, cfg, sink, addr)

	src.add(Segment{SeqStart: 1, SeqEnd: 3, Bytes: testSegment(t, cfg.DatabaseID, 0, 1, 3)})
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 3 }) {
		t.Fatalf("the sink applied %v, want three transactions", sink.seqs())
	}
	if seqs := sink.seqs(); !slices.Equal(seqs, []uint64{1, 2, 3}) {
		t.Errorf("the sink applied %v, want the transactions 1, 2 and 3", seqs)
	}
	if !waitFor(t, 5*time.Second, func() bool { return source.AppliedSeq() == 3 }) {
		t.Errorf("the source knows the sequence number %d, want 3", source.AppliedSeq())
	}
}

func TestBackupSkipsDuplicateSegment(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	startBackup(t, cfg, sink, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	seg := testSegment(t, cfg.DatabaseID, 0, 1, 3)
	srv.send(Batch{Segments: [][]byte{seg}})
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 3 }) {
		t.Fatalf("the sink applied %v, want three transactions", sink.seqs())
	}

	srv.send(Batch{Segments: [][]byte{seg}})
	if !waitFor(t, 5*time.Second, func() bool { return len(srv.acks()) == 2 }) {
		t.Fatalf("the backup sent %d acknowledgements, want 2", len(srv.acks()))
	}
	if seqs := sink.seqs(); len(seqs) != 3 {
		t.Errorf("the sink applied %v, want the three transactions once", seqs)
	}
	acks := srv.acks()
	if acks[1].AppliedSeq != 3 {
		t.Errorf("the acknowledgement names the sequence number %d, want 3", acks[1].AppliedSeq)
	}
}

func TestBackupAppliesOnlyNewTransactions(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	sink.setApplied(2)
	startBackup(t, cfg, sink, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	srv.send(Batch{Segments: [][]byte{testSegment(t, cfg.DatabaseID, 0, 1, 3)}})
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 1 }) {
		t.Fatalf("the sink applied %v, want one transaction", sink.seqs())
	}
	if seqs := sink.seqs(); seqs[0] != 3 {
		t.Errorf("the sink applied the transaction %d, want the transaction 3", seqs[0])
	}
	acks := srv.acks()
	if len(acks) != 1 || acks[0].AppliedSeq != 3 {
		t.Errorf("the backup acknowledged %v, want the sequence number 3", acks)
	}
}

func TestBackupResyncsOnGap(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	startBackup(t, cfg, sink, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	srv.send(Batch{Segments: [][]byte{testSegment(t, cfg.DatabaseID, 0, 5, 2)}})
	if !waitFor(t, 5*time.Second, func() bool { return slices.Contains(srv.types(), MsgResyncRequired) }) {
		t.Fatalf("the backup sent %v, want a resync", srv.types())
	}
	if seqs := sink.seqs(); len(seqs) != 0 {
		t.Errorf("the sink applied %v, want nothing", seqs)
	}
}

func TestBackupResyncsOnCorruptSegment(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	startBackup(t, cfg, sink, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	seg := testSegment(t, cfg.DatabaseID, 0, 1, 2)
	seg[len(seg)-40] ^= 0xff
	srv.send(Batch{Segments: [][]byte{seg}})
	if !waitFor(t, 5*time.Second, func() bool { return slices.Contains(srv.types(), MsgResyncRequired) }) {
		t.Fatalf("the backup sent %v, want a resync", srv.types())
	}
	if seqs := sink.seqs(); len(seqs) != 0 {
		t.Errorf("the sink applied %v, want nothing", seqs)
	}
}

func TestBackupAppliesEverySegmentOnce(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	cfg.HeartbeatInterval = 100 * time.Millisecond
	src := &fakeSource{}
	sink := &fakeSink{}
	addr, source, _ := startSource(t, cfg, src)
	startBackup(t, cfg, sink, addr)

	var want []uint64
	for i := uint64(1); i <= 40; i++ {
		src.add(Segment{SeqStart: i, SeqEnd: i, Bytes: testSegment(t, cfg.DatabaseID, 0, i, 1)})
		want = append(want, i)
	}
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == len(want) }) {
		t.Fatalf("the sink applied %v, want %v", sink.seqs(), want)
	}
	if seqs := sink.seqs(); !slices.Equal(seqs, want) {
		t.Errorf("the sink applied %v, want %v", seqs, want)
	}
	if !waitFor(t, 5*time.Second, func() bool { return source.AppliedSeq() == 40 }) {
		t.Errorf("the source knows the sequence number %d, want 40", source.AppliedSeq())
	}
}

func TestBackupResumesAfterPause(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	cfg.HeartbeatInterval = 100 * time.Millisecond
	src := &fakeSource{}
	sink := &fakeSink{}
	addr, _, _ := startSource(t, cfg, src)
	b := startBackup(t, cfg, sink, addr)

	for i := uint64(1); i <= 5; i++ {
		src.add(Segment{SeqStart: i, SeqEnd: i, Bytes: testSegment(t, cfg.DatabaseID, 0, i, 1)})
	}
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 5 }) {
		t.Fatalf("the sink applied %v, want five transactions", sink.seqs())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Pause(ctx, 5); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	// A segment that shows up while the backup is paused waits for the
	// resume.
	src.add(Segment{SeqStart: 6, SeqEnd: 6, Bytes: testSegment(t, cfg.DatabaseID, 0, 6, 1)})
	time.Sleep(300 * time.Millisecond)
	if seqs := sink.seqs(); len(seqs) != 5 {
		t.Errorf("the sink applied %v while the backup was paused, want five transactions", seqs)
	}
	if err := b.Resume(ctx, 5); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 6 }) {
		t.Fatalf("the sink applied %v, want six transactions", sink.seqs())
	}
	if seqs := sink.seqs(); !slices.Equal(seqs, []uint64{1, 2, 3, 4, 5, 6}) {
		t.Errorf("the sink applied %v, want the transactions 1 to 6", seqs)
	}
}

func TestBackupReconnectsWithGrowingDelay(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 50 * time.Millisecond
	cfg.BackoffMax = 400 * time.Millisecond
	srv := newTestServer(t, cfg)
	srv.closeAfterHello = true
	startBackup(t, cfg, &fakeSink{}, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() >= 4 }) {
		t.Fatalf("the backup connected %d times, want at least 4", srv.helloCount())
	}
	times := srv.helloTimes()
	last := time.Duration(0)
	for i := 1; i < len(times); i++ {
		d := times[i].Sub(times[i-1])
		if d < last {
			t.Errorf("the delay %d is shorter than the delay %d before it", d, last)
		}
		if d > cfg.BackoffMax {
			t.Errorf("the delay %d is longer than the maximum %d", d, cfg.BackoffMax)
		}
		last = d
	}
}

func TestBackoffDelay(t *testing.T) {
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, 60 * time.Second, 60 * time.Second,
	}
	for i, w := range want {
		d := backoffDelay(time.Second, 60*time.Second, i)
		if d < w {
			t.Errorf("the delay %d is shorter than %d", d, w)
		}
		if d > 60*time.Second {
			t.Errorf("the delay %d is longer than the maximum", d)
		}
	}
}

func TestBackupPausesAndResumes(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	b := startBackup(t, cfg, &fakeSink{}, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Pause(ctx, 7); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if !slices.Contains(srv.types(), MsgPause) {
		t.Fatalf("the server received %v, want a pause", srv.types())
	}
	if err := b.Pause(ctx, 7); err != nil {
		t.Fatalf("the second Pause: %v", err)
	}
	if err := b.Resume(ctx, 7); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return slices.Contains(srv.types(), MsgResume) }) {
		t.Fatalf("the server sent %v, want a resume", srv.types())
	}
}

func TestBackupResumesAfterBrokenConnection(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	src := &fakeSource{}
	sink := &fakeSink{}
	sink.failSeq = 2
	addr, source, _ := startSource(t, cfg, src)
	startBackup(t, cfg, sink, addr)

	src.add(Segment{SeqStart: 1, SeqEnd: 3, Bytes: testSegment(t, cfg.DatabaseID, 0, 1, 3)})
	src.add(Segment{SeqStart: 4, SeqEnd: 6, Bytes: testSegment(t, cfg.DatabaseID, 0, 4, 3)})
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 6 }) {
		t.Fatalf("the sink applied %v, want the six transactions", sink.seqs())
	}
	if seqs := sink.seqs(); !slices.Equal(seqs, []uint64{1, 2, 3, 4, 5, 6}) {
		t.Errorf("the sink applied %v, want the transactions 1 to 6 once", seqs)
	}
	if !waitFor(t, 5*time.Second, func() bool { return source.AppliedSeq() == 6 }) {
		t.Errorf("the source knows the sequence number %d, want 6", source.AppliedSeq())
	}
}

func TestBackupReconnectsWithTheAppliedSequence(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	b := startBackup(t, cfg, sink, srv.addr())

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	srv.send(Batch{Segments: [][]byte{testSegment(t, cfg.DatabaseID, 0, 1, 3)}})
	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 3 }) {
		t.Fatalf("the sink applied %v, want three transactions", sink.seqs())
	}
	srv.closeConns()
	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 2 }) {
		t.Fatal("the backup did not connect again")
	}
	if b.LastError() == nil {
		t.Error("the backup did not remember the error of the broken connection")
	}
	hello, ok := srv.lastHello()
	if !ok {
		t.Fatal("the backup did not connect again")
	}
	if hello.AppliedSeq != 3 {
		t.Errorf("the hello names the sequence number %d, want 3", hello.AppliedSeq)
	}
}

func TestBackupReportsResync(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	srv := newTestServer(t, cfg)
	sink := &fakeSink{}
	b, err := NewBackup(cfg, sink)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- b.Run(ctx, srv.addr()) }()

	if !waitFor(t, 5*time.Second, func() bool { return srv.helloCount() == 1 }) {
		t.Fatal("the backup did not connect")
	}
	srv.send(Batch{Segments: [][]byte{testSegment(t, cfg.DatabaseID, 0, 5, 2)}})
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrResyncRequired) {
			t.Fatalf("the run ended with %v, want %v", err, ErrResyncRequired)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not end")
	}
}
