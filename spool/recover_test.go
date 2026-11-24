package spool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// reopen closes the spool and opens it again.
func reopen(t *testing.T, s *Spool, cfg Config) *Spool {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	return s2
}

// fillSpool writes the given number of transactions into the spool, one
// segment per transaction.
func fillSpool(t *testing.T, s *Spool, n int) {
	t.Helper()
	for i := range n {
		appendTx(t, s, testTx(uint64(i+1), 1))
		if err := s.Flush(context.Background()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
}

func TestRecoverAfterRestart(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 3)

	s2 := reopen(t, s, cfg)
	ack, err := s2.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 3 {
		t.Errorf("the durable sequence number is %d after the restart, want 3", ack.Seq)
	}
	if ack.Pos != testTx(3, 1).Pos {
		t.Errorf("the durable position is %+v, want %+v", ack.Pos, testTx(3, 1).Pos)
	}
	segs, err := s2.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("the spool holds %d segments after the restart, want 3", len(segs))
	}

	// The sequence numbers continue where they stopped.
	appendTx(t, s2, testTx(4, 1))
	if err := s2.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	ack, err = s2.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 4 {
		t.Errorf("the durable sequence number is %d, want 4", ack.Seq)
	}
}

func TestTheScanIsTheTruth(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 3)

	// The state file goes behind the segments.
	path := filepath.Join(root, "app.db", "state", "capture.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the state file: %v", err)
	}
	var state captureState
	if err := json.Unmarshal(b, &state); err != nil {
		t.Fatalf("decode the state file: %v", err)
	}
	state.Seq = 1
	b, err = json.Marshal(state)
	if err != nil {
		t.Fatalf("encode the state file: %v", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write the state file: %v", err)
	}

	s2 := reopen(t, s, cfg)
	ack, err := s2.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 3 {
		t.Errorf("the durable sequence number is %d, want 3", ack.Seq)
	}

	// The state file runs ahead of the segments.
	if err := s2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	b, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the state file: %v", err)
	}
	if err := json.Unmarshal(b, &state); err != nil {
		t.Fatalf("decode the state file: %v", err)
	}
	state.Seq = 99
	b, err = json.Marshal(state)
	if err != nil {
		t.Fatalf("encode the state file: %v", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write the state file: %v", err)
	}
	s3, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s3.Close()
	ack, err = s3.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 3 {
		t.Errorf("the durable sequence number is %d, want 3", ack.Seq)
	}
}

func TestTheTemporaryFilesAreRemoved(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)

	path := filepath.Join(root, "app.db", "tmp", "0000000000000001.tmp")
	if err := os.WriteFile(path, []byte("not a segment"), 0o644); err != nil {
		t.Fatalf("write the temporary file: %v", err)
	}
	s2 := reopen(t, s, cfg)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the temporary file was not removed: %v", err)
	}
	ack, err := s2.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 0 {
		t.Errorf("the durable sequence number is %d, want 0", ack.Seq)
	}
}

func TestAMissingSegmentBreaksTheChain(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 3)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	middle := filepath.Join(root, "app.db", "segments", "0000000000000002-0000000000000002.seg")
	if err := os.Remove(middle); err != nil {
		t.Fatalf("remove the segment: %v", err)
	}
	broken, err := Open(context.Background(), cfg)
	if !errors.Is(err, ErrChainBroken) {
		t.Fatalf("Open returned %v, want ErrChainBroken", err)
	}
	if broken == nil {
		t.Fatal("the spool that comes with the error is missing")
	}
	defer broken.Close()
	if !broken.Stats().Broken {
		t.Error("the spool does not report the broken chain")
	}
	if broken.Stats().BrokenReason == "" {
		t.Error("the spool does not explain the broken chain")
	}
}

func TestABrokenSegmentInTheMiddleBreaksTheChain(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 3)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	middle := filepath.Join(root, "app.db", "segments", "0000000000000002-0000000000000002.seg")
	b, err := os.ReadFile(middle)
	if err != nil {
		t.Fatalf("read the segment: %v", err)
	}
	b[segment.HeaderSize+4] ^= 0xff
	if err := os.WriteFile(middle, b, 0o644); err != nil {
		t.Fatalf("write the segment: %v", err)
	}
	if _, err := Open(context.Background(), cfg); !errors.Is(err, ErrChainBroken) {
		t.Fatalf("Open returned %v, want ErrChainBroken", err)
	}
}

func TestABrokenSegmentAtTheEndIsDropped(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 3)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	last := filepath.Join(root, "app.db", "segments", "0000000000000003-0000000000000003.seg")
	b, err := os.ReadFile(last)
	if err != nil {
		t.Fatalf("read the segment: %v", err)
	}
	b[segment.HeaderSize+4] ^= 0xff
	if err := os.WriteFile(last, b, 0o644); err != nil {
		t.Fatalf("write the segment: %v", err)
	}
	s2, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()
	if s2.Stats().Broken {
		t.Error("the spool reports a broken chain")
	}
	ack, err := s2.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 2 {
		t.Errorf("the durable sequence number is %d, want 2", ack.Seq)
	}
	segs, err := s2.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 2 {
		t.Errorf("the spool holds %d segments, want 2", len(segs))
	}
}

func TestTheDirectoryOfAnotherDatabaseIsRejected(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(root, "app.db", "state", "stream.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the state file: %v", err)
	}
	var state streamState
	if err := json.Unmarshal(b, &state); err != nil {
		t.Fatalf("decode the state file: %v", err)
	}
	state.DBID = "0000000000000000"
	b, err = json.Marshal(state)
	if err != nil {
		t.Fatalf("encode the state file: %v", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write the state file: %v", err)
	}
	if _, err := Open(context.Background(), cfg); !errors.Is(err, ErrDatabaseMismatch) {
		t.Fatalf("Open returned %v, want ErrDatabaseMismatch", err)
	}
}
