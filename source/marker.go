package source

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// markerFile is the name of the file that holds the marker of a clean close.
const markerFile = "peer.json"

// Errors of the chain of the segments.
var (
	// errChainBroken reports that the chain of the segments does not cover
	// the primary database any more. The backup needs a new baseline.
	errChainBroken = errors.New("source: the chain of the segments is broken")
	// errChainIncomplete reports that the chain of the segments does not
	// end where the capture cursor of the driver ends. The chain may have
	// a hole and it may not be marked as complete.
	errChainIncomplete = errors.New("source: the chain of the segments does not cover the primary database")
)

// cleanMarker is the content of the file that holds the marker of a clean
// close.
//
// It says that the chain of the segments was complete at the position it
// names: the primary database held nothing that the chain did not hold.
type cleanMarker struct {
	// Seq is the sequence number of the end of the chain.
	Seq uint64 `json:"seq"`
	// Pos is the position of the end of the chain in the write-ahead log.
	Pos markerPos `json:"pos"`
}

// markerPos is the position of the end of the chain in the write-ahead log.
type markerPos struct {
	// Salt1 and Salt2 identify the generation of the log.
	Salt1 uint32 `json:"salt1"`
	Salt2 uint32 `json:"salt2"`
	// Frame is the number of frames that are committed in the generation.
	Frame uint64 `json:"frame"`
}

// verdict is the verdict about the continuity of the chain.
type verdict int

const (
	// verdictUnknown means that the chain cannot be judged yet.
	verdictUnknown verdict = iota
	// verdictContinuous means that the chain covers the primary database.
	verdictContinuous
	// verdictBroken means that the chain does not cover the primary
	// database and that the backup needs a new baseline.
	verdictBroken
)

// markerPathOf returns the path of the file that holds the marker of a clean
// close.
//
// The marker lives in the directory of the database of the spool: the spool
// does not know it and it does not touch it.
func markerPathOf(cfg Config) string {
	return filepath.Join(cfg.Spool.Root, cfg.Protocol.DatabaseID, markerFile)
}

// markerPath returns the path of the file that holds the marker of a clean
// close.
func (p *Peer) markerPath() string {
	return markerPathOf(p.cfg)
}

// readMarker reads the marker of a clean close.
//
// A marker that is missing or that cannot be parsed counts as no marker: the
// runtime cannot prove that the chain covers the primary database.
func readMarker(path string) *cleanMarker {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var m cleanMarker
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return &m
}

// writeMarker writes the marker of a clean close.
//
// The content is written to a temporary file, the file is synced and it is
// renamed over the target: a reader of the target never sees half a file.
func writeMarker(path string, m cleanMarker) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("source: encode the marker: %w", err)
	}
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("source: create the temporary file of the marker: %w", err)
	}
	tmp := file.Name()
	defer func() { _ = os.Remove(tmp) }()

	if _, err := file.Write(b); err != nil {
		_ = file.Close()
		return fmt.Errorf("source: write the marker: %w", err)
	}
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return fmt.Errorf("source: set the permissions of the marker: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("source: sync the marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("source: close the marker: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("source: replace the marker: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("source: open the directory of the marker: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("source: sync the directory of the marker: %w", err)
	}
	return nil
}

// removeMarker removes the marker of a clean close.
//
// A marker that does not describe the end of the chain any more is of no
// use: it may not prove that the chain covers the primary database.
func removeMarker(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("source: remove the marker: %w", err)
	}
	return nil
}

// markerPosOf turns the position of the spool into the position of a marker.
func markerPosOf(pos spool.Pos) markerPos {
	return markerPos{Salt1: pos.Salt1, Salt2: pos.Salt2, Frame: pos.Frame}
}

// judgeChain decides whether the chain of the segments still covers the
// primary database.
//
// The counters are read by the function counters: they are only needed when
// the chain and the generation of the log are known.
//
// The change counters tell whether the primary database is behind the chain:
// the counter of the chain and the counter of the primary database count the
// same writes of page 1, so the primary database is behind when its counter
// is the smaller one.
//
// The generation of the log tells whether the log was reset. A log that was
// reset while the runtime was down may have lost the transactions that the
// chain does not hold: the marker of the last clean close proves that it does
// not.
func judgeChain(ack spool.Ack, cursor driver.Position, marker *cleanMarker, counters func() (primary, chain changeCounter)) verdict {
	switch {
	case ack.Seq == 0:
		// The chain is empty, there is nothing to judge.
		return verdictUnknown
	case cursor.Salt1 == 0 && cursor.Salt2 == 0:
		// The driver does not know the generation of the log yet.
		return verdictUnknown
	}
	primary, chain := counters()
	switch {
	case primary.known && chain.known && primary.value < chain.value:
		// The primary database is behind the end of the chain: it lost the
		// transactions the chain holds.
		return verdictBroken
	case ack.Pos.Salt1 == cursor.Salt1 && ack.Pos.Salt2 == cursor.Salt2:
		if cursor.Frame < ack.Pos.Frame {
			// The log holds fewer frames than the chain: the tail of the
			// chain is gone and the primary database lost the
			// transactions it holds.
			return verdictBroken
		}
		// The log was not reset, the chain continues where it ended.
		return verdictContinuous
	case marker != nil && marker.Seq == ack.Seq && marker.Pos == markerPosOf(ack.Pos):
		// The log was reset after the runtime closed cleanly: the chain
		// was complete then and the reset lost nothing.
		return verdictContinuous
	default:
		return verdictBroken
	}
}
