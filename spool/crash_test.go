package spool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// The tests of this file build the state of the disk a crash leaves behind
// and check that the spool starts up with it. The production code does not
// know about the crash points.

func TestCrashDuringTheWriteLeavesAHalfSegment(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	appendTx(t, s, testTx(1, 1))

	// The process died in the middle of the record.
	path := filepath.Join(root, "app.db", "tmp", "0000000000000001.tmp")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the temporary file: %v", err)
	}
	if err := os.WriteFile(path, b[:len(b)-10], 0o644); err != nil {
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

func TestCrashBeforeThePublishLeavesASegmentInTheTemporaryDirectory(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The segment was written and synced but the rename did not happen.
	published := filepath.Join(root, "app.db", "segments", "0000000000000001-0000000000000001.seg")
	tmp := filepath.Join(root, "app.db", "tmp", "0000000000000001.tmp")
	if err := os.Rename(published, tmp); err != nil {
		t.Fatalf("move the segment: %v", err)
	}

	s2 := reopen(t, s, cfg)
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
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

func TestCrashBeforeTheDirectorySyncKeepsTheSegmentThatWasRenamed(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The rename reached the disk, the sync of the directory did not. The
	// segment is there and the scan finds it.
	s2, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()
	ack, err := s2.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 1 {
		t.Errorf("the durable sequence number is %d, want 1", ack.Seq)
	}
}

func TestCrashDuringTheStateUpdateLeavesABrokenStateFile(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 2)

	path := filepath.Join(root, "app.db", "state", "capture.json")
	if err := os.WriteFile(path, []byte(`{"seq": 12`), 0o644); err != nil {
		t.Fatalf("write the state file: %v", err)
	}
	s2 := reopen(t, s, cfg)
	ack, err := s2.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 2 {
		t.Errorf("the durable sequence number is %d, want 2", ack.Seq)
	}
}

func TestCrashDuringTheAckUpdateLeavesABrokenAckFile(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 2)
	if err := s.SetBackupAck(context.Background(), 2); err != nil {
		t.Fatalf("SetBackupAck: %v", err)
	}

	path := filepath.Join(root, "app.db", "peers", "backup.ack")
	if err := os.WriteFile(path, []byte(`{"seq": 2, "epo`), 0o644); err != nil {
		t.Fatalf("write the acknowledgement: %v", err)
	}
	s2 := reopen(t, s, cfg)
	ack, err := s2.BackupAck()
	if err != nil {
		t.Fatalf("BackupAck: %v", err)
	}
	if ack != 0 {
		t.Errorf("the acknowledgement is %d, want 0", ack)
	}
	res, err := s2.GC(context.Background())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.Segments != 0 {
		t.Errorf("the collection removed %d segments, want 0", res.Segments)
	}
}

func TestCrashDuringTheGarbageCollectionRemovesTheOldestSegments(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Root: root, DatabaseID: "app.db", Mode: ModeFull}
	s := newTestSpool(t, cfg)
	fillSpool(t, s, 3)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The collection removed the oldest segment and the process died
	// before it removed the next one.
	oldest := filepath.Join(root, "app.db", "segments", "0000000000000001-0000000000000001.seg")
	if err := os.Remove(oldest); err != nil {
		t.Fatalf("remove the segment: %v", err)
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
	if ack.Seq != 3 {
		t.Errorf("the durable sequence number is %d, want 3", ack.Seq)
	}
}
