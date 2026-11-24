package spool

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// streamState is the content of state/stream.json.
//
// It holds the identity of the database, the generation of the chain and the
// sequence number the next transaction gets. It is an accelerator, the
// segments are the truth.
type streamState struct {
	DatabaseID string    `json:"databaseID"`
	DBID       string    `json:"dbID"`
	Epoch      uint64    `json:"epoch"`
	NextSeq    uint64    `json:"nextSeq"`
	PageSize   uint32    `json:"pageSize"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// captureState is the content of state/capture.json.
//
// It holds the durable capture acknowledgement. It is an accelerator, the
// segments are the truth.
type captureState struct {
	Seq       uint64    `json:"seq"`
	Epoch     uint64    `json:"epoch"`
	WAL       walState  `json:"wal"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// walState is the position in the write-ahead log of an acknowledgement.
type walState struct {
	Salt1     uint32 `json:"salt1"`
	Salt2     uint32 `json:"salt2"`
	Frame     uint64 `json:"frame"`
	WALOffset int64  `json:"walOffset"`
}

// ackState is the content of peers/<peer-id>.ack.
//
// It holds the sequence number the backup applied.
type ackState struct {
	Seq       uint64    `json:"seq"`
	Epoch     uint64    `json:"epoch"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// streamPath returns the path of the file that holds the state of the
// stream.
func (s *Spool) streamPath() string {
	return filepath.Join(s.stateDir, "stream.json")
}

// capturePath returns the path of the file that holds the durable capture
// acknowledgement.
func (s *Spool) capturePath() string {
	return filepath.Join(s.stateDir, "capture.json")
}

// ackPath returns the path of the file that holds the acknowledgement of the
// backup.
func (s *Spool) ackPath() string {
	return filepath.Join(s.peerDir, s.cfg.PeerID+".ack")
}

// readStreamState reads the state of the stream.
//
// A state file that is missing or that cannot be parsed is no error: the
// segments are the truth and the state file is rebuilt from them.
func (s *Spool) readStreamState() (*streamState, error) {
	b, err := os.ReadFile(s.streamPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("spool: read the state of the stream: %w", err)
	}
	var st streamState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, nil
	}
	return &st, nil
}

// writeStreamState writes the state of the stream.
//
// The file is only written when it does not hold the state already, so that
// a spool that has nothing to say does not write.
func (s *Spool) writeStreamState() error {
	st := streamState{
		DatabaseID: s.cfg.DatabaseID,
		DBID:       hex.EncodeToString(s.dbID[:]),
		Epoch:      s.epoch,
		NextSeq:    s.nextSeq,
		PageSize:   s.pageSize,
		UpdatedAt:  time.Now(),
	}
	if s.createdAt.IsZero() {
		s.createdAt = st.UpdatedAt
	}
	st.CreatedAt = s.createdAt
	if cur, err := s.readStreamState(); err == nil && cur != nil &&
		cur.DatabaseID == st.DatabaseID && cur.DBID == st.DBID &&
		cur.Epoch == st.Epoch && cur.NextSeq == st.NextSeq && cur.PageSize == st.PageSize {
		return nil
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("spool: encode the state of the stream: %w", err)
	}
	return s.writeFileAtomic(s.streamPath(), b)
}

// writeCaptureState writes the durable capture acknowledgement.
//
// The file is only written when it does not hold the acknowledgement
// already.
func (s *Spool) writeCaptureState() error {
	st := captureState{
		Seq:   s.durable.Seq,
		Epoch: s.epoch,
		WAL: walState{
			Salt1:     s.durable.Pos.Salt1,
			Salt2:     s.durable.Pos.Salt2,
			Frame:     s.durable.Pos.Frame,
			WALOffset: s.durable.Pos.WALOffset,
		},
		UpdatedAt: time.Now(),
	}
	if cur, err := s.readCaptureState(); err == nil && cur != nil &&
		cur.Seq == st.Seq && cur.Epoch == st.Epoch && cur.WAL == st.WAL {
		return nil
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("spool: encode the capture acknowledgement: %w", err)
	}
	return s.writeFileAtomic(s.capturePath(), b)
}

// readCaptureState reads the durable capture acknowledgement.
func (s *Spool) readCaptureState() (*captureState, error) {
	b, err := os.ReadFile(s.capturePath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("spool: read the capture acknowledgement: %w", err)
	}
	var st captureState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, nil
	}
	return &st, nil
}

// readAckState reads the acknowledgement of the backup.
//
// A missing or a broken acknowledgement file counts as no acknowledgement:
// the backup did not confirm anything and the segments are all kept.
func (s *Spool) readAckState() (*ackState, error) {
	b, err := os.ReadFile(s.ackPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("spool: read the acknowledgement: %w", err)
	}
	var st ackState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, nil
	}
	return &st, nil
}

// writeAckState writes the acknowledgement of the backup.
func (s *Spool) writeAckState(seq uint64) error {
	st := ackState{Seq: seq, Epoch: s.epoch, UpdatedAt: time.Now()}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("spool: encode the acknowledgement: %w", err)
	}
	return s.writeFileAtomic(s.ackPath(), b)
}

// backupAckLocked returns the sequence number the backup acknowledged.
//
// The caller holds the lock of the spool.
func (s *Spool) backupAckLocked() (uint64, error) {
	state, err := s.readAckState()
	if err != nil {
		return 0, err
	}
	if state == nil || state.Epoch != s.epoch {
		return 0, nil
	}
	return state.Seq, nil
}

// writeFileAtomic replaces the file at path with the given content.
//
// The content is written to a temporary file in the same directory and
// renamed over the target, so that a reader of the target never sees half a
// file. The file and the directory are synced when the mode asks for it.
func (s *Spool) writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("spool: create a temporary file: %w", err)
	}
	tmp := file.Name()
	defer func() { _ = os.Remove(tmp) }()

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("spool: write %s: %w", tmp, err)
	}
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return fmt.Errorf("spool: write %s: %w", tmp, err)
	}
	if s.cfg.Mode.syncs() {
		if err := s.syncFile(tmp, file); err != nil {
			_ = file.Close()
			return fmt.Errorf("spool: sync %s: %w", tmp, err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("spool: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("spool: replace %s: %w", path, err)
	}
	if s.cfg.Mode.syncs() {
		if err := s.syncDir(dir); err != nil {
			return fmt.Errorf("spool: sync %s: %w", dir, err)
		}
	}
	return nil
}
