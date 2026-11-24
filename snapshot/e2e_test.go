package snapshot

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/backup"
	"github.com/cnlangzi/sqlite-relay/protocol"
)

// testToken is the token of the tests.
const testToken = "the shared token of the tests"

// e2eConfig returns the configuration of the source and of the backup of the
// end-to-end test.
func e2eConfig(t *testing.T, root string) protocol.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatalf("write the token: %v", err)
	}
	return protocol.Config{
		TokenFile:  path,
		DatabaseID: "app.db",
		StreamID:   1,
		BackupID:   "backup-1",
	}
}

// TestTheBackupHoldsTheDataOfThePrimary runs the whole chain: the primary
// database, the spool, the source, the protocol and the backup.
func TestTheBackupHoldsTheDataOfThePrimary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := t.TempDir()
	d, path := newTestDriver(t)
	sp := newTestSpool(t, root)
	cap := newCapture(d, sp)
	createTable(t, d)
	insert(t, d, "first")

	cfg := e2eConfig(t, root)
	producer, err := New(Config{Dir: filepath.Join(root, "snapshots"), DatabaseID: cfg.DatabaseID, StreamID: cfg.StreamID}, d, sp, cap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// The chain holds the transactions of the primary before the backup
	// asks for its baseline.
	if !waitFor(t, func() bool {
		ack, err := sp.Durable()
		return err == nil && ack.Seq > 0
	}) {
		t.Fatal("the chain did not hold the transactions of the primary")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	source, err := protocol.NewSourceWithSnapshot(cfg, protocol.NewSpoolSource(sp), producer)
	if err != nil {
		t.Fatalf("NewSourceWithSnapshot: %v", err)
	}
	go source.Serve(ctx, ln)

	peer, err := backup.Open(ctx, backup.Config{Root: root, Protocol: cfg})
	if err != nil {
		t.Fatalf("backup.Open: %v", err)
	}
	go peer.Run(ctx, ln.Addr().String())

	// The backup takes the snapshot of the primary as its baseline.
	if !waitFor(t, func() bool { return peer.Stats().AppliedSeq > 0 }) {
		st := peer.Stats()
		t.Fatalf("the backup applied the sequence number %d, want the baseline of the snapshot: the backup holds %v, the source holds %v", st.AppliedSeq, st.LastError, source.LastError())
	}
	backupPath := filepath.Join(root, cfg.DatabaseID, "backup.db")
	if got, want := rows(t, backupPath), rows(t, path); len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("the backup holds %v, want %v", got, want)
	}

	// The increments of the chain bring the backup up to date.
	insert(t, d, "second")
	insert(t, d, "third")
	if !waitFor(t, func() bool { return len(rows(t, backupPath)) == 3 }) {
		t.Fatalf("the backup holds %v, want the three rows of the primary", rows(t, backupPath))
	}
	if got, want := rows(t, backupPath), rows(t, path); len(got) != len(want) {
		t.Errorf("the backup holds %v, want %v", got, want)
	}

	// The backup is paused, the business reads the backup database and the
	// chain catches up after the resume.
	if err := peer.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	insert(t, d, "fourth")
	time.Sleep(300 * time.Millisecond)
	if got := rows(t, backupPath); len(got) != 3 {
		t.Errorf("the backup holds %v, want the three rows of the pause", got)
	}
	if err := peer.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !waitFor(t, func() bool { return len(rows(t, backupPath)) == 4 }) {
		t.Fatalf("the backup holds %v, want the four rows of the primary", rows(t, backupPath))
	}
	if got, want := rows(t, backupPath), rows(t, path); len(got) != len(want) {
		t.Errorf("the backup holds %v, want %v", got, want)
	}
}
