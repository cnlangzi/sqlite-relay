package protocol

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/spool"
)

// newTestSpool opens a spool that holds the given transactions.
func newTestSpool(t *testing.T, txs int) *spool.Spool {
	t.Helper()
	ctx := context.Background()
	sp, err := spool.Open(ctx, spool.Config{
		Root:       t.TempDir(),
		DatabaseID: "app.db",
		Mode:       spool.ModeFull,
	})
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	t.Cleanup(func() { sp.Close() })
	for i := range txs {
		tx := spool.Tx{
			Pos:         spool.Pos{Salt1: 1, Salt2: 2, Frame: uint64(i)},
			CommitPages: 4,
			Pages:       []spool.Page{{Pgno: 1, Data: testPage(uint64(i + 1))}},
		}
		if err := sp.Append(ctx, tx); err != nil {
			t.Fatalf("spool.Append: %v", err)
		}
	}
	if err := sp.Flush(ctx); err != nil {
		t.Fatalf("spool.Flush: %v", err)
	}
	return sp
}

func TestSpoolSourceServesTheSegmentsOfTheSpool(t *testing.T) {
	ctx := context.Background()
	sp := newTestSpool(t, 3)
	refs, err := sp.Segments()
	if err != nil {
		t.Fatalf("spool.Segments: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("the spool holds %d segments, want 1", len(refs))
	}

	src := NewSpoolSource(sp)
	if src.Epoch() != sp.Epoch() {
		t.Errorf("the epoch is %d, want %d", src.Epoch(), sp.Epoch())
	}
	if src.OldestSeq() != refs[0].SeqStart {
		t.Errorf("the lowest sequence number is %d, want %d", src.OldestSeq(), refs[0].SeqStart)
	}

	segs, err := src.SegmentsAfter(ctx, 0, 1<<20)
	if err != nil {
		t.Fatalf("SegmentsAfter: %v", err)
	}
	if len(segs) != 1 {
		t.Fatalf("the source holds %d segments, want 1", len(segs))
	}
	if segs[0].SeqStart != refs[0].SeqStart || segs[0].SeqEnd != refs[0].SeqEnd {
		t.Errorf("the segment holds the sequence numbers %d to %d, want %d to %d", segs[0].SeqStart, segs[0].SeqEnd, refs[0].SeqStart, refs[0].SeqEnd)
	}
	if segs[0].Epoch != refs[0].Epoch {
		t.Errorf("the epoch of the segment is %d, want %d", segs[0].Epoch, refs[0].Epoch)
	}
	want, err := os.ReadFile(refs[0].Path)
	if err != nil {
		t.Fatalf("read the segment: %v", err)
	}
	if !bytes.Equal(segs[0].Bytes, want) {
		t.Error("the segment does not hold the bytes of the file of the spool")
	}

	segs, err = src.SegmentsAfter(ctx, refs[0].SeqEnd, 1<<20)
	if err != nil {
		t.Fatalf("SegmentsAfter: %v", err)
	}
	if len(segs) != 0 {
		t.Errorf("the source holds %d segments, want none", len(segs))
	}
}

func TestSpoolSourcePushesTheSegmentsOfTheSpool(t *testing.T) {
	cfg := testConfig(t)
	cfg.BackoffMin = 20 * time.Millisecond
	sp := newTestSpool(t, 3)
	refs, err := sp.Segments()
	if err != nil {
		t.Fatalf("spool.Segments: %v", err)
	}

	src := NewSpoolSource(sp)
	sink := &fakeSink{}
	addr, source, _ := startSource(t, cfg, src)
	startBackup(t, cfg, sink, addr)

	if !waitFor(t, 5*time.Second, func() bool { return len(sink.seqs()) == 3 }) {
		t.Fatalf("the sink applied %v, want three transactions", sink.seqs())
	}
	if seqs := sink.seqs(); seqs[0] != refs[0].SeqStart || seqs[2] != refs[0].SeqEnd {
		t.Errorf("the sink applied %v, want the sequence numbers %d to %d", seqs, refs[0].SeqStart, refs[0].SeqEnd)
	}
	if !waitFor(t, 5*time.Second, func() bool { return source.AppliedSeq() == refs[0].SeqEnd }) {
		t.Errorf("the source knows the sequence number %d, want %d", source.AppliedSeq(), refs[0].SeqEnd)
	}
}
