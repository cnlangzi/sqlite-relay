package spool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// pageSize is the size of the pages the tests write.
const pageSize = 512

// syncCounter counts the calls of the sync seam of a spool.
type syncCounter struct {
	segmentFiles int
	segmentDirs  int
	otherFiles   int
	otherDirs    int
}

// install replaces the sync seam of the spool with counters.
func (c *syncCounter) install(s *Spool) {
	fileSync, dirSync := s.syncFile, s.syncDir
	s.syncFile = func(path string, f *os.File) error {
		if strings.HasPrefix(path, s.segDir+string(filepath.Separator)) || strings.HasPrefix(path, s.tmpDir+string(filepath.Separator)) {
			c.segmentFiles++
		} else {
			c.otherFiles++
		}
		return fileSync(path, f)
	}
	s.syncDir = func(path string) error {
		if path == s.segDir {
			c.segmentDirs++
		} else {
			c.otherDirs++
		}
		return dirSync(path)
	}
}

// total returns the number of the calls of the seam.
func (c *syncCounter) total() int {
	return c.segmentFiles + c.segmentDirs + c.otherFiles + c.otherDirs
}

// newTestSpool opens a spool in a fresh directory.
func newTestSpool(t *testing.T, cfg Config) *Spool {
	t.Helper()
	if cfg.Root == "" {
		cfg.Root = t.TempDir()
	}
	if cfg.DatabaseID == "" {
		cfg.DatabaseID = "app.db"
	}
	s, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// testTx returns a transaction with the given sequence number and pages.
func testTx(seq uint64, pages int) Tx {
	tx := Tx{
		Pos:         Pos{Salt1: 7, Salt2: 9, Frame: seq, WALOffset: int64(seq) * 4096},
		CommitPages: 4,
	}
	for i := range pages {
		data := make([]byte, pageSize)
		for j := range data {
			data[j] = byte(seq + uint64(i) + uint64(j))
		}
		tx.Pages = append(tx.Pages, Page{Pgno: uint32(i + 1), Data: data})
	}
	return tx
}

// appendTx appends a transaction and fails the test when it does not work.
func appendTx(t *testing.T, s *Spool, tx Tx) {
	t.Helper()
	if err := s.Append(context.Background(), tx); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestOpenNeedsRootAndDatabaseID(t *testing.T) {
	if _, err := Open(context.Background(), Config{DatabaseID: "app.db"}); err == nil {
		t.Error("a spool without a root directory was opened")
	}
	if _, err := Open(context.Background(), Config{Root: t.TempDir()}); err == nil {
		t.Error("a spool without a database ID was opened")
	}
	if _, err := Open(context.Background(), Config{Root: t.TempDir(), DatabaseID: "a/b"}); err == nil {
		t.Error("a spool with a path separator in the database ID was opened")
	}
	if _, err := Open(context.Background(), Config{Root: t.TempDir(), DatabaseID: "app.db", PeerID: "a/b"}); err == nil {
		t.Error("a spool with a path separator in the peer ID was opened")
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{Root: "/tmp/relay", DatabaseID: "app.db"}.withDefaults()
	if cfg.Mode != ModeOff {
		t.Errorf("the mode is %v, want OFF", cfg.Mode)
	}
	if cfg.SyncInterval != time.Second {
		t.Errorf("the sync interval is %v, want 1s", cfg.SyncInterval)
	}
	if cfg.SyncBytes != 4<<20 {
		t.Errorf("the sync bytes are %d, want 4MiB", cfg.SyncBytes)
	}
	if cfg.SegmentMaxBytes != 8<<20 {
		t.Errorf("the size of a segment is %d, want 8MiB", cfg.SegmentMaxBytes)
	}
	if cfg.MaxBytes != 0 {
		t.Errorf("the limit of the spool is %d, want 0", cfg.MaxBytes)
	}
	if cfg.WarnBytes != 0 {
		t.Errorf("the warning is at %d bytes, want 0", cfg.WarnBytes)
	}
	if cfg.Retention != 0 {
		t.Errorf("the retention is %v, want 0", cfg.Retention)
	}
	if cfg.PeerID != "backup" {
		t.Errorf("the peer is %q, want %q", cfg.PeerID, "backup")
	}
}

func TestDefaultModeIsOff(t *testing.T) {
	s := newTestSpool(t, Config{})
	if s.Stats().Mode != ModeOff {
		t.Errorf("the mode is %v, want OFF", s.Stats().Mode)
	}
}

func TestDirectoryLayout(t *testing.T) {
	root := t.TempDir()
	s := newTestSpool(t, Config{Root: root, DatabaseID: "app.db"})
	appendTx(t, s, testTx(1, 1))
	appendTx(t, s, testTx(2, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	dir := filepath.Join(root, "app.db")
	for _, sub := range []string{"segments", "tmp", "state", "peers"} {
		if info, err := os.Stat(filepath.Join(dir, sub)); err != nil || !info.IsDir() {
			t.Errorf("the directory %s is missing", sub)
		}
	}
	seg := filepath.Join(dir, "segments", "0000000000000001-0000000000000002.seg")
	if _, err := os.Stat(seg); err != nil {
		t.Errorf("the segment %s is missing: %v", seg, err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "tmp"))
	if err != nil {
		t.Fatalf("read the temporary directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("the temporary directory holds %d files, want 0", len(entries))
	}
	for _, name := range []string{"stream.json", "capture.json"} {
		if _, err := os.Stat(filepath.Join(dir, "state", name)); err != nil {
			t.Errorf("the state file %s is missing: %v", name, err)
		}
	}
	if err := s.SetBackupAck(context.Background(), 2); err != nil {
		t.Fatalf("SetBackupAck: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "peers", "backup.ack")); err != nil {
		t.Errorf("the acknowledgement of the backup is missing: %v", err)
	}
}

func TestNoSegmentUsesTheLtxExtension(t *testing.T) {
	s := newTestSpool(t, Config{})
	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	entries, err := os.ReadDir(s.segDir)
	if err != nil {
		t.Fatalf("read the segments directory: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".ltx") {
			t.Errorf("the segment %s uses the extension of another format", e.Name())
		}
		if !strings.HasSuffix(e.Name(), ".seg") {
			t.Errorf("the segment %s does not use the extension of the segments", e.Name())
		}
	}
}

func TestOffModePublishesWithoutSyncing(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeOff})
	counter := &syncCounter{}
	counter.install(s)

	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if counter.total() != 0 {
		t.Errorf("the mode OFF synced %d times, want 0", counter.total())
	}

	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 1 {
		t.Errorf("the durable sequence number is %d, want 1", ack.Seq)
	}
	segs, err := s.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 1 {
		t.Fatalf("the spool holds %d segments, want 1", len(segs))
	}
}

func TestFullModeSyncsTheFileAndTheDirectory(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	counter := &syncCounter{}
	counter.install(s)

	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if counter.segmentFiles != 1 {
		t.Errorf("the mode FULL synced %d segment files, want 1", counter.segmentFiles)
	}
	if counter.segmentDirs != 1 {
		t.Errorf("the mode FULL synced the segments directory %d times, want 1", counter.segmentDirs)
	}
}

func TestNormalModePublishesOnTheBarrier(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeNormal})
	counter := &syncCounter{}
	counter.install(s)

	for i := range 3 {
		appendTx(t, s, testTx(uint64(i+1), 1))
		if err := s.Flush(context.Background()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 0 {
		t.Errorf("the durable sequence number is %d before the barrier, want 0", ack.Seq)
	}
	segs, err := s.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 0 {
		t.Errorf("the spool holds %d segments before the barrier, want 0", len(segs))
	}

	if err := s.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	ack, err = s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 3 {
		t.Errorf("the durable sequence number is %d after the barrier, want 3", ack.Seq)
	}
	segs, err = s.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 3 {
		t.Errorf("the spool holds %d segments after the barrier, want 3", len(segs))
	}
	if counter.segmentFiles != 3 {
		t.Errorf("the barrier synced %d segment files, want 3", counter.segmentFiles)
	}
	if counter.segmentDirs != 1 {
		t.Errorf("the barrier synced the segments directory %d times, want 1", counter.segmentDirs)
	}
}

func TestDurableAckNamesTheLastTransaction(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	for i := range 3 {
		appendTx(t, s, testTx(uint64(i+1), 1))
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 3 {
		t.Errorf("the durable sequence number is %d, want 3", ack.Seq)
	}
	want := testTx(3, 1).Pos
	if ack.Pos != want {
		t.Errorf("the durable position is %+v, want %+v", ack.Pos, want)
	}
}

func TestSegmentFullKeepsTheTransaction(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull, SegmentMaxBytes: 700})
	appendTx(t, s, testTx(1, 1))

	err := s.Append(context.Background(), testTx(2, 1))
	if !errors.Is(err, ErrSegmentFull) {
		t.Fatalf("the append returned %v, want ErrSegmentFull", err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	appendTx(t, s, testTx(2, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 2 {
		t.Errorf("the durable sequence number is %d, want 2", ack.Seq)
	}
	segs, err := s.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 2 {
		t.Fatalf("the spool holds %d segments, want 2", len(segs))
	}
	if segs[0].SeqStart != 1 || segs[0].SeqEnd != 1 {
		t.Errorf("the first segment covers %d to %d, want 1 to 1", segs[0].SeqStart, segs[0].SeqEnd)
	}
	if segs[1].SeqStart != 2 || segs[1].SeqEnd != 2 {
		t.Errorf("the second segment covers %d to %d, want 2 to 2", segs[1].SeqStart, segs[1].SeqEnd)
	}
}

func TestSpoolFullRejectsTheWrite(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull, MaxBytes: 1000})
	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}

	err = s.Append(context.Background(), testTx(2, 1))
	if !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("the append returned %v, want ErrSpoolFull", err)
	}
	after, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if after.Seq != ack.Seq {
		t.Errorf("the durable sequence number moved to %d, want %d", after.Seq, ack.Seq)
	}
}

func TestWarnBytesIsReported(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull, WarnBytes: 1})
	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !s.Stats().Warn {
		t.Error("the spool does not warn about its size")
	}
}

func TestGarbageCollectionKeepsTheUnacknowledgedSegments(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	for i := range 3 {
		appendTx(t, s, testTx(uint64(i+1), 1))
		if err := s.Flush(context.Background()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	res, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.Segments != 0 {
		t.Errorf("the collection removed %d segments, want 0", res.Segments)
	}
	if len(s.segs) != 3 {
		t.Errorf("the spool holds %d segments, want 3", len(s.segs))
	}

	if err := s.SetBackupAck(context.Background(), 2); err != nil {
		t.Fatalf("SetBackupAck: %v", err)
	}
	res, err = s.GC(context.Background())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.Segments != 2 {
		t.Errorf("the collection removed %d segments, want 2", res.Segments)
	}
	if len(s.segs) != 1 {
		t.Errorf("the spool holds %d segments, want 1", len(s.segs))
	}
	if s.segs[0].SeqEnd != 3 {
		t.Errorf("the spool keeps the segment that ends at %d, want 3", s.segs[0].SeqEnd)
	}
}

func TestGarbageCollectionKeepsTheSegmentsThatAreTooYoung(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull, Retention: time.Hour})
	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.SetBackupAck(context.Background(), 1); err != nil {
		t.Fatalf("SetBackupAck: %v", err)
	}
	res, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.Segments != 0 {
		t.Errorf("the collection removed %d segments, want 0", res.Segments)
	}
}

func TestBackupAckThatWentBackRemovesLess(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	for i := range 3 {
		appendTx(t, s, testTx(uint64(i+1), 1))
		if err := s.Flush(context.Background()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	if err := s.SetBackupAck(context.Background(), 3); err != nil {
		t.Fatalf("SetBackupAck: %v", err)
	}
	// The acknowledgement of the backup went back.
	state, err := s.readAckState()
	if err != nil {
		t.Fatalf("read the acknowledgement: %v", err)
	}
	state.Seq = 1
	b, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("encode the acknowledgement: %v", err)
	}
	if err := os.WriteFile(s.ackPath(), b, 0o644); err != nil {
		t.Fatalf("write the acknowledgement: %v", err)
	}
	ack, err := s.BackupAck()
	if err != nil {
		t.Fatalf("BackupAck: %v", err)
	}
	if ack != 1 {
		t.Errorf("the acknowledgement is %d, want 1", ack)
	}
	res, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.Segments != 1 {
		t.Errorf("the collection removed %d segments, want 1", res.Segments)
	}
	if len(s.segs) != 2 {
		t.Errorf("the spool holds %d segments, want 2", len(s.segs))
	}
}

func TestResetEmptiesTheSpool(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.SetBackupAck(context.Background(), 1); err != nil {
		t.Fatalf("SetBackupAck: %v", err)
	}
	epoch := s.Epoch()

	if err := s.Reset(context.Background()); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if s.Epoch() != epoch+1 {
		t.Errorf("the epoch is %d after the reset, want %d", s.Epoch(), epoch+1)
	}
	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 0 {
		t.Errorf("the durable sequence number is %d after the reset, want 0", ack.Seq)
	}
	backup, err := s.BackupAck()
	if err != nil {
		t.Fatalf("BackupAck: %v", err)
	}
	if backup != 0 {
		t.Errorf("the acknowledgement of the backup is %d after the reset, want 0", backup)
	}
	for _, dir := range []string{s.segDir, s.tmpDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		if len(entries) != 0 {
			t.Errorf("the directory %s holds %d files after the reset, want 0", dir, len(entries))
		}
	}
	appendTx(t, s, testTx(1, 1))
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	ack, err = s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 1 {
		t.Errorf("the durable sequence number is %d after the reset, want 1", ack.Seq)
	}
}

func TestStatsMatchTheDisk(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	for i := range 2 {
		appendTx(t, s, testTx(uint64(i+1), 1))
		if err := s.Flush(context.Background()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	st := s.Stats()
	if st.Mode != ModeFull {
		t.Errorf("the mode is %v, want FULL", st.Mode)
	}
	if st.Segments != 2 {
		t.Errorf("the spool holds %d segments, want 2", st.Segments)
	}
	if st.DurableSeq != 2 {
		t.Errorf("the durable sequence number is %d, want 2", st.DurableSeq)
	}
	if st.UnackedSegments != 2 {
		t.Errorf("the spool holds %d unacknowledged segments, want 2", st.UnackedSegments)
	}
	if st.UnackedBytes != st.Bytes {
		t.Errorf("the unacknowledged bytes are %d, the spool holds %d bytes", st.UnackedBytes, st.Bytes)
	}
	var size int64
	entries, err := os.ReadDir(s.segDir)
	if err != nil {
		t.Fatalf("read the segments directory: %v", err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", e.Name(), err)
		}
		size += info.Size()
	}
	if st.Bytes != size {
		t.Errorf("the spool holds %d bytes, the segments hold %d bytes", st.Bytes, size)
	}
	if st.DiskFreeBytes <= 0 {
		t.Errorf("the file system has %d bytes left", st.DiskFreeBytes)
	}
	if st.Broken {
		t.Error("the spool reports a broken chain")
	}
}

func TestPendingReportsTheWork(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeNormal})
	appendTx(t, s, testTx(1, 1))
	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if pending.Segments != 1 {
		t.Errorf("the spool holds %d pending segments, want 1", pending.Segments)
	}
	if pending.OpenBytes == 0 {
		t.Error("the segment that is open holds no bytes")
	}
	if pending.Age <= 0 {
		t.Error("the pending segment has no age")
	}
}

func TestARenameThatFailsDoesNotAdvanceTheAck(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	appendTx(t, s, testTx(1, 1))

	// The segments directory cannot be written into.
	if err := os.Chmod(s.segDir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer os.Chmod(s.segDir, 0o755)

	if err := s.Flush(context.Background()); err == nil {
		t.Fatal("the flush succeeded although the segment could not be published")
	}
	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 0 {
		t.Errorf("the durable sequence number is %d, want 0", ack.Seq)
	}
}

func TestADirectorySyncThatFailsDoesNotAdvanceTheAck(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	dirSync := s.syncDir
	s.syncDir = func(path string) error {
		if path == s.segDir {
			return errors.New("the directory cannot be synced")
		}
		return dirSync(path)
	}
	appendTx(t, s, testTx(1, 1))

	if err := s.Flush(context.Background()); err == nil {
		t.Fatal("the flush succeeded although the directory could not be synced")
	}
	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("Durable: %v", err)
	}
	if ack.Seq != 0 {
		t.Errorf("the durable sequence number is %d, want 0", ack.Seq)
	}
}

func TestTheStateFilesAreReplacedAtomically(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	for i := range 50 {
		if err := s.SetBackupAck(context.Background(), uint64(i+1)); err != nil {
			t.Fatalf("SetBackupAck: %v", err)
		}
		b, err := os.ReadFile(s.ackPath())
		if err != nil {
			t.Fatalf("read the acknowledgement: %v", err)
		}
		var state ackState
		if err := json.Unmarshal(b, &state); err != nil {
			t.Fatalf("the acknowledgement is not a complete file: %v", err)
		}
		if state.Seq != uint64(i+1) {
			t.Errorf("the acknowledgement holds the sequence number %d, want %d", state.Seq, i+1)
		}
	}
	entries, err := os.ReadDir(s.peerDir)
	if err != nil {
		t.Fatalf("read the peers directory: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("the peers directory holds %d files, want 1", len(entries))
	}
}

func TestTheSpoolDoesNotWriteOnItsOwn(t *testing.T) {
	s := newTestSpool(t, Config{Mode: ModeFull})
	before := filesOf(t, s.dir)
	time.Sleep(100 * time.Millisecond)
	after := filesOf(t, s.dir)
	if len(before) != len(after) {
		t.Fatalf("the spool holds %d files, want %d", len(after), len(before))
	}
	for path, info := range before {
		if after[path] != info {
			t.Errorf("the file %s changed without a call of the caller", path)
		}
	}
}

// filesOf returns the size and the modification time of every file below
// the directory.
func filesOf(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		out[path] = fmt.Sprintf("%d %d", info.Size(), info.ModTime().UnixNano())
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return out
}
