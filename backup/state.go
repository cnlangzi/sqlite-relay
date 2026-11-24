package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// stateVersion is the version of the state file.
const stateVersion = 1

// Phase is the phase of a backup.
type Phase string

const (
	// PhaseEmpty means that the backup has no baseline yet. It holds no
	// database and it cannot apply the increments of the chain.
	PhaseEmpty Phase = "empty"
	// PhaseInstalling means that a snapshot is on its way to the backup.
	// The database of the backup is the old one until the installation
	// is finished.
	PhaseInstalling Phase = "installing"
	// PhaseReady means that the backup has a baseline and that it applies
	// the increments of the chain to it.
	PhaseReady Phase = "ready"
)

// state is the content of the state file of a backup.
//
// The state file is the memory of the backup: it names the database the
// backup holds, the generation of the chain it is part of and the sequence
// number it applied and persisted.
type state struct {
	// Version is the version of the format of the state file.
	Version int `json:"version"`
	// Phase is the phase of the backup.
	Phase Phase `json:"phase"`
	// DatabaseID is the name of the database the backup holds.
	DatabaseID string `json:"databaseID"`
	// StreamID is the number of the stream the backup belongs to.
	StreamID uint64 `json:"streamID"`
	// Epoch is the generation of the chain the backup is part of.
	Epoch uint64 `json:"epoch"`
	// AppliedSeq is the sequence number the backup applied and persisted.
	AppliedSeq uint64 `json:"appliedSeq"`
	// PageSize is the size of the pages of the database.
	PageSize uint32 `json:"pageSize"`
	// DBPath is the path of the file of the backup database.
	DBPath string `json:"dbPath"`
	// SnapshotBytes is the size of the snapshot of the baseline.
	SnapshotBytes uint64 `json:"snapshotBytes"`
	// SnapshotSHA256 is the checksum of the snapshot of the baseline.
	SnapshotSHA256 string `json:"snapshotSHA256"`
}

// readState reads the state file of a backup.
//
// A state file that does not exist means that the backup has no baseline
// yet. A state file that cannot be parsed is an error: the backup does not
// know what it holds and it may not overwrite it.
func readState(path string) (*state, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("backup: read the state: %w", err)
	}
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("backup: the state file is malformed: %w", err)
	}
	if st.Version != stateVersion {
		return nil, fmt.Errorf("backup: the state file has the version %d, want %d", st.Version, stateVersion)
	}
	return &st, nil
}

// writeState writes the state file of a backup.
//
// The file is written to a temporary file in the same directory and renamed
// over the target, so that a reader of the target never sees half a file.
// The file and the directory are synced.
func writeState(path string, st *state) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("backup: encode the state: %w", err)
	}
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("backup: create a temporary file: %w", err)
	}
	tmp := file.Name()
	defer func() { _ = os.Remove(tmp) }()

	if _, err := file.Write(b); err != nil {
		_ = file.Close()
		return fmt.Errorf("backup: write %s: %w", tmp, err)
	}
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return fmt.Errorf("backup: write %s: %w", tmp, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("backup: sync %s: %w", tmp, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("backup: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("backup: replace %s: %w", path, err)
	}
	return syncDir(dir)
}

// syncDir syncs a directory.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("backup: open %s: %w", path, err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("backup: sync %s: %w", path, err)
	}
	return nil
}
