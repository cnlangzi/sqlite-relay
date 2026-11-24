package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInstallRefusesASnapshotThatDoesNotMatchTheManifest(t *testing.T) {
	dir := t.TempDir()
	primary := newTestDatabase(t, dir, "primary.db", true)
	app := newTestApplier(t, dir, "app.db")

	meta := snapshotMeta(t, primary, "app.db", 3, 10)
	meta.Checksum[0] ^= 0xff
	w, err := app.BeginSnapshot(context.Background(), meta)
	if err != nil {
		t.Fatalf("BeginSnapshot: %v", err)
	}
	b, err := os.ReadFile(primary)
	if err != nil {
		t.Fatalf("read the primary: %v", err)
	}
	if _, err := w.Write(b); err != nil {
		t.Fatalf("write the snapshot: %v", err)
	}
	if err := app.CommitSnapshot(context.Background()); err == nil {
		t.Fatal("the backup installed a snapshot that does not match the manifest")
	}
	// The backup holds no database and its state did not change.
	if _, err := os.Stat(filepath.Join(dir, dbName)); !os.IsNotExist(err) {
		t.Error("the backup holds a database after a failed installation")
	}
	st, err := readState(app.statePath)
	if err != nil {
		t.Fatalf("readState: %v", err)
	}
	if st.Phase != PhaseEmpty {
		t.Errorf("the state is in the phase %s, want %s", st.Phase, PhaseEmpty)
	}
}

func TestOpenFinishesAnInterruptedInstallation(t *testing.T) {
	dir := t.TempDir()
	root := t.TempDir()
	backupDir := filepath.Join(root, "app.db")
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		t.Fatalf("create the directory: %v", err)
	}
	// The old database of the backup and the new one, the installation was
	// killed before the rename.
	old := filepath.Join(dir, "old.db")
	newTestDatabase(t, dir, "old.db", true)
	primary := newTestDatabase(t, dir, "primary.db", true)
	copyFile(t, old, filepath.Join(backupDir, dbName))
	copyFile(t, primary, filepath.Join(backupDir, dbName+".new"))
	st := &state{
		Version:        stateVersion,
		Phase:          PhaseInstalling,
		DatabaseID:     "app.db",
		StreamID:       1,
		Epoch:          3,
		AppliedSeq:     10,
		PageSize:       testPageSize,
		DBPath:         filepath.Join(backupDir, dbName),
		SnapshotBytes:  uint64(len(mustRead(t, primary))),
		SnapshotSHA256: checksum(t, primary),
	}
	if err := writeState(filepath.Join(backupDir, stateName), st); err != nil {
		t.Fatalf("writeState: %v", err)
	}

	cfg := testConfig(t, root)
	if _, err := Open(context.Background(), cfg); err != nil {
		t.Fatalf("Open: %v", err)
	}
	// The new database is the database of the backup.
	if checksum(t, filepath.Join(backupDir, dbName)) != checksum(t, primary) {
		t.Error("the backup does not hold the new database")
	}
	if _, err := os.Stat(filepath.Join(backupDir, dbName+".new")); !os.IsNotExist(err) {
		t.Error("the backup holds the temporary file of the installation")
	}
	st, err := readState(filepath.Join(backupDir, stateName))
	if err != nil {
		t.Fatalf("readState: %v", err)
	}
	if st.Phase != PhaseReady {
		t.Errorf("the state is in the phase %s, want %s", st.Phase, PhaseReady)
	}
}

func TestOpenFinishesAnInstallationWhoseRenameHappened(t *testing.T) {
	dir := t.TempDir()
	root := t.TempDir()
	backupDir := filepath.Join(root, "app.db")
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		t.Fatalf("create the directory: %v", err)
	}
	primary := newTestDatabase(t, dir, "primary.db", true)
	copyFile(t, primary, filepath.Join(backupDir, dbName))
	st := &state{
		Version:        stateVersion,
		Phase:          PhaseInstalling,
		DatabaseID:     "app.db",
		StreamID:       1,
		Epoch:          3,
		AppliedSeq:     10,
		PageSize:       testPageSize,
		DBPath:         filepath.Join(backupDir, dbName),
		SnapshotBytes:  uint64(len(mustRead(t, primary))),
		SnapshotSHA256: checksum(t, primary),
	}
	if err := writeState(filepath.Join(backupDir, stateName), st); err != nil {
		t.Fatalf("writeState: %v", err)
	}

	cfg := testConfig(t, root)
	if _, err := Open(context.Background(), cfg); err != nil {
		t.Fatalf("Open: %v", err)
	}
	st, err := readState(filepath.Join(backupDir, stateName))
	if err != nil {
		t.Fatalf("readState: %v", err)
	}
	if st.Phase != PhaseReady {
		t.Errorf("the state is in the phase %s, want %s", st.Phase, PhaseReady)
	}
}

func TestOpenRefusesAnInstallationThatDidNotFinish(t *testing.T) {
	dir := t.TempDir()
	root := t.TempDir()
	backupDir := filepath.Join(root, "app.db")
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		t.Fatalf("create the directory: %v", err)
	}
	old := newTestDatabase(t, dir, "old.db", true)
	other := newTestDatabase(t, dir, "other.db", true)
	write(t, other, "another row")
	copyFile(t, old, filepath.Join(backupDir, dbName))
	st := &state{
		Version:        stateVersion,
		Phase:          PhaseInstalling,
		DatabaseID:     "app.db",
		StreamID:       1,
		Epoch:          3,
		AppliedSeq:     10,
		PageSize:       testPageSize,
		DBPath:         filepath.Join(backupDir, dbName),
		SnapshotBytes:  4096,
		SnapshotSHA256: checksum(t, other),
	}
	if err := writeState(filepath.Join(backupDir, stateName), st); err != nil {
		t.Fatalf("writeState: %v", err)
	}

	cfg := testConfig(t, root)
	_, err := Open(context.Background(), cfg)
	if err == nil {
		t.Fatal("the backup opened an installation that did not finish")
	}
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Errorf("the error is %v, want %v", err, ErrSnapshotRequired)
	}
}

func TestInstallTakesTheEpochOfTheSnapshot(t *testing.T) {
	dir := t.TempDir()
	primary := newTestDatabase(t, dir, "primary.db", true)
	app := newTestApplier(t, dir, "app.db")
	installSnapshot(t, app, primary, "app.db", 7, 42)

	if app.Epoch() != 7 {
		t.Errorf("the epoch of the backup is %d, want 7", app.Epoch())
	}
	applied, err := app.AppliedSeq(context.Background())
	if err != nil {
		t.Fatalf("AppliedSeq: %v", err)
	}
	if applied != 42 {
		t.Errorf("the backup applied the sequence number %d, want the baseline 42", applied)
	}
	if app.NeedsSnapshot() {
		t.Error("the backup asks for a snapshot, it has a baseline")
	}
}
