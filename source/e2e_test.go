package source

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/backup"
	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/protocol"
)

// startBackup opens a backup and lets it connect to the source.
//
// The cleanup waits for the peer to stop: the peer writes into its
// directory, the removal of the directory must not race with it.
func startBackup(t *testing.T, root string, cfg Config, addr string) *backup.Peer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p, err := backup.Open(ctx, backup.Config{
		Root: root,
		Protocol: protocol.Config{
			TokenFile:  cfg.Protocol.TokenFile,
			DatabaseID: cfg.Protocol.DatabaseID,
			StreamID:   cfg.Protocol.StreamID,
			BackupID:   "backup-1",
		},
	})
	if err != nil {
		cancel()
		t.Fatalf("open the backup: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx, addr)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return p
}

// backupPath returns the path of the database of the backup.
func backupPath(root string) string {
	return filepath.Join(root, testDatabaseID, "backup.db")
}

// query reads the rows of a table of a database.
func query(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open the database: %v", err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT v FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query the database: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// TestTheRelayMovesTheIncrementsToTheBackup checks that the increments of
// the primary database end up in the database of the backup.
func TestTheRelayMovesTheIncrementsToTheBackup(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	src, addr := startPeerAt(t, cfg)
	backups := filepath.Join(dir, "backups")
	bp := startBackup(t, backups, cfg, addr)

	// The backup has no baseline, the source sends it a snapshot.
	if !waitFor(t, func() bool { return bp.Stats().Snapshots > 0 }) {
		t.Fatal("the backup did not take the snapshot of the source")
	}
	if rows := query(t, backupPath(backups)); len(rows) != 1 || rows[0] != "a" {
		t.Fatalf("the backup holds %v, want the rows of the primary database", rows)
	}

	// The increments that follow the baseline are applied.
	insert(t, d, "b")
	if !waitFor(t, func() bool { return len(query(t, backupPath(backups))) == 2 }) {
		t.Fatalf("the backup holds %v, want the rows of the primary database", query(t, backupPath(backups)))
	}

	// The backup acknowledges the increments and the source collects the
	// segments.
	if !waitFor(t, func() bool { return src.Stats().BackupSeq == src.Stats().CapturedSeq }) {
		t.Errorf("the backup acknowledged %d transactions, the source captured %d", src.Stats().BackupSeq, src.Stats().CapturedSeq)
	}
	st := src.Stats()
	if st.LagSeq != 0 {
		t.Errorf("the source lags %d transactions, want 0", st.LagSeq)
	}
	if st.Spool.UnackedSegments != 0 {
		t.Errorf("the spool holds %d unacknowledged segments, want 0", st.Spool.UnackedSegments)
	}
	// The segments that the backup acknowledged are collected.
	if st.Spool.Segments != 0 {
		t.Errorf("the spool holds %d segments after the backup acknowledged the chain", st.Spool.Segments)
	}
}

// TestTheRelayCapturesWithoutABackup checks that the primary side captures
// the increments when the backup is not connected.
//
// The capture acknowledgement does not wait for the backup: the primary
// database keeps the frames of the log only as long as the increments are
// not in the local spool.
func TestTheRelayCapturesWithoutABackup(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)

	p := startPeer(t, cfg)
	for i := range 10 {
		insert(t, d, string(rune('a'+i)))
	}

	if !waitFor(t, func() bool { return p.Stats().CapturedSeq >= 10 }) {
		t.Fatalf("the runtime captured %d transactions, want 10", p.Stats().CapturedSeq)
	}
	if st := p.Stats(); st.BackupSeq != 0 {
		t.Errorf("the backup acknowledged %d transactions, want 0", st.BackupSeq)
	}
	if st := p.Stats(); st.LagSeq != st.CapturedSeq {
		t.Errorf("the source lags %d transactions, want %d", st.LagSeq, st.CapturedSeq)
	}
	// The segments that the backup did not acknowledge are kept.
	if st := p.Stats(); st.Spool.UnackedSegments == 0 {
		t.Error("the spool collected the segments that the backup did not acknowledge")
	}
	// The driver writes the frames of the log back, the increments are
	// durable in the spool.
	if !waitFor(t, func() bool { return p.Stats().Driver.Checkpoint.Checkpointed > 0 }) {
		t.Errorf("the driver did not write the frames back: %+v", p.Stats().Driver.Checkpoint)
	}
}

// TestTheRelayResyncsTheBackupAfterAChainReset checks that the backup gets
// a new baseline when the chain of the source is reset.
//
// The segments of the old chain are gone and the sequence numbers of the
// new chain start over. The session of the backup has to end, otherwise it
// waits for segments that will never come.
func TestTheRelayResyncsTheBackupAfterAChainReset(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	src, addr := startPeerAt(t, cfg)
	backups := filepath.Join(dir, "backups")
	bp := startBackup(t, backups, cfg, addr)
	if !waitFor(t, func() bool { return bp.Stats().Snapshots > 0 }) {
		t.Fatal("the backup did not take the snapshot of the source")
	}

	// The chain of the source is reset.
	if err := src.recoverChain(context.Background(), driver.ErrSnapshotRequired); err != nil {
		t.Fatalf("recoverChain: %v", err)
	}

	// The backup asks for a new baseline and installs it.
	if !waitFor(t, func() bool { return bp.Stats().Snapshots > 1 }) {
		t.Fatalf("the backup installed %d snapshots, want 2", bp.Stats().Snapshots)
	}

	// The increments that follow the new baseline are applied.
	insert(t, d, "b")
	if !waitFor(t, func() bool { return len(query(t, backupPath(backups))) == 2 }) {
		t.Fatalf("the backup holds %v, want the rows of the primary database", query(t, backupPath(backups)))
	}
	if !waitFor(t, func() bool { return !src.Stats().SnapshotRequired }) {
		t.Errorf("the runtime still asks for a new baseline: %s", src.Stats().SnapshotReason)
	}
}

// TestTheRelayHealsTheBackupThatWasPausedWhenTheChainBroke checks that a
// backup that was paused when the chain of the source was reset gets a new
// baseline.
//
// The backup cannot ask for a snapshot while it is paused: the business
// holds the database. The resume ends the pause and the backup asks for the
// new baseline.
func TestTheRelayHealsTheBackupThatWasPausedWhenTheChainBroke(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	src, addr := startPeerAt(t, cfg)
	backups := filepath.Join(dir, "backups")
	bp := startBackup(t, backups, cfg, addr)
	if !waitFor(t, func() bool { return bp.Stats().Snapshots > 0 }) {
		t.Fatal("the backup did not take the snapshot of the source")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := bp.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	// The chain of the source is reset while the backup is paused.
	if err := src.recoverChain(context.Background(), driver.ErrSnapshotRequired); err != nil {
		t.Fatalf("recoverChain: %v", err)
	}

	// The backup notices that the chain of the source is not its chain.
	if !waitFor(t, func() bool { return bp.Stats().SnapshotRequired > 0 }) {
		t.Fatal("the backup did not notice that the chain of the source was reset")
	}

	// The resume ends the pause and reports that the backup needs a new
	// baseline.
	if err := bp.Resume(ctx); !errors.Is(err, backup.ErrSnapshotRequired) {
		t.Fatalf("the resume returned %v, want ErrSnapshotRequired", err)
	}
	if !waitFor(t, func() bool { return bp.Stats().Snapshots > 1 }) {
		t.Fatalf("the backup installed %d snapshots, want 2", bp.Stats().Snapshots)
	}
}

// TestTheRelayKeepsThePrimaryAliveWhileTheBackupIsPaused checks that the
// primary side keeps capturing and keeps checkpointing while the backup
// serves its database to the business.
func TestTheRelayKeepsThePrimaryAliveWhileTheBackupIsPaused(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	d, _ := newTestDriver(t, dir)
	cfg.Driver = d
	createTable(t, d)
	insert(t, d, "a")

	src, addr := startPeerAt(t, cfg)
	backups := filepath.Join(dir, "backups")
	bp := startBackup(t, backups, cfg, addr)
	if !waitFor(t, func() bool { return bp.Stats().Snapshots > 0 }) {
		t.Fatal("the backup did not take the snapshot of the source")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := bp.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	// The primary side keeps capturing while the backup is paused.
	insert(t, d, "b")
	if !waitFor(t, func() bool { return src.Stats().CapturedSeq >= 2 }) {
		t.Fatalf("the runtime captured %d transactions, want 2", src.Stats().CapturedSeq)
	}

	// The backup catches up when it is resumed.
	if err := bp.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !waitFor(t, func() bool { return len(query(t, backupPath(backups))) == 2 }) {
		t.Fatalf("the backup holds %v, want the rows of the primary database", query(t, backupPath(backups)))
	}
}
