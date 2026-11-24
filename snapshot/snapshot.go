// Package snapshot produces the snapshots of a primary database.
//
// The source of a relay sends a snapshot to a backup that has no baseline.
// The snapshot is a consistent copy of the primary database and it is bound
// to the sequence number of the last transaction of the chain it holds.
//
// The bootstrap of a snapshot suspends the capture of the primary database:
// the snapshot and the drain that follows it have to see the same
// transactions. The business keeps writing while the snapshot is taken.
package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/segment"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// snapshotAttempts is the number of times a snapshot is tried before it
// gives up. The write-ahead log may be reset while the snapshot is taken.
const snapshotAttempts = 3

// The offsets of the header of page 1 of a SQLite database.
const (
	// headerPageSize is the offset of the size of the pages of the
	// database.
	headerPageSize = 16
	// headerSize is the size of the header of page 1.
	headerSize = 100
)

// errRetry reports that the snapshot has to be taken again.
var errRetry = errors.New("snapshot: the snapshot has to be taken again")

// Config configures a producer.
type Config struct {
	// Dir is the directory the snapshots are written into.
	Dir string
	// DatabaseID is the name of the database the snapshots belong to.
	DatabaseID string
	// StreamID is the number of the stream the snapshots belong to.
	StreamID uint64
}

// Capture is the capture of the primary database.
//
// The bootstrap of a snapshot needs the capture to stand still: the snapshot
// and the drain that follows it have to see the same transactions.
type Capture interface {
	// Suspend stops the capture and waits for the work in flight to end.
	// The returned function starts the capture again.
	Suspend(ctx context.Context) (resume func(), err error)
}

// Producer produces the snapshots of a primary database.
type Producer struct {
	cfg Config
	drv *driver.Driver
	sp  *spool.Spool
	cap Capture

	// mu serializes the snapshots: the capture is suspended while a
	// snapshot is taken.
	mu sync.Mutex
}

// New returns a producer of the snapshots of a primary database.
func New(cfg Config, d *driver.Driver, sp *spool.Spool, cap Capture) (*Producer, error) {
	if cfg.Dir == "" {
		return nil, errors.New("snapshot: the directory of the snapshots is empty")
	}
	if cfg.DatabaseID == "" {
		return nil, errors.New("snapshot: the database ID is empty")
	}
	if d == nil {
		return nil, errors.New("snapshot: the driver is nil")
	}
	if sp == nil {
		return nil, errors.New("snapshot: the spool is nil")
	}
	if cap == nil {
		return nil, errors.New("snapshot: the capture is nil")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("snapshot: create %s: %w", cfg.Dir, err)
	}
	return &Producer{cfg: cfg, drv: d, sp: sp, cap: cap}, nil
}

// Snapshot writes a consistent snapshot of the primary database.
//
// The snapshot is the state of the database at the boundary it is bound to.
// The chain of the spool is drained to that boundary, so that the increments
// that follow the baseline of the snapshot bring the database up to date
// without a gap and without a repeat.
//
// The reader of the snapshot removes its file when it is closed.
func (p *Producer) Snapshot(ctx context.Context) (protocol.SnapshotMeta, io.ReadCloser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	for attempt := 0; attempt < snapshotAttempts; attempt++ {
		var r io.ReadCloser
		var meta protocol.SnapshotMeta
		meta, r, err = p.snapshotOnce(ctx)
		if err == nil {
			return meta, r, nil
		}
		if r != nil {
			_ = r.Close()
		}
		if !errors.Is(err, errRetry) {
			return protocol.SnapshotMeta{}, nil, err
		}
	}
	return protocol.SnapshotMeta{}, nil, fmt.Errorf("snapshot: the snapshot failed after %d attempts: %w", snapshotAttempts, err)
}

// snapshotOnce takes a snapshot and drains the chain to its boundary.
func (p *Producer) snapshotOnce(ctx context.Context) (protocol.SnapshotMeta, io.ReadCloser, error) {
	resume, err := p.cap.Suspend(ctx)
	if err != nil {
		return protocol.SnapshotMeta{}, nil, fmt.Errorf("snapshot: suspend the capture: %w", err)
	}
	defer resume()

	// The chain holds every transaction that was captured.
	if err := p.sp.Flush(ctx); err != nil {
		return protocol.SnapshotMeta{}, nil, fmt.Errorf("snapshot: flush the spool: %w", err)
	}
	if err := p.sp.Sync(ctx); err != nil {
		return protocol.SnapshotMeta{}, nil, fmt.Errorf("snapshot: sync the spool: %w", err)
	}
	ack, err := p.sp.Durable()
	if err != nil {
		return protocol.SnapshotMeta{}, nil, fmt.Errorf("snapshot: the durable acknowledgement of the spool: %w", err)
	}

	// The snapshot is the state of the database at its boundary.
	path, err := p.snapshotPath()
	if err != nil {
		return protocol.SnapshotMeta{}, nil, err
	}
	snap, err := p.drv.Snapshot(ctx, path)
	if err != nil {
		_ = os.Remove(path)
		return protocol.SnapshotMeta{}, nil, fmt.Errorf("snapshot: take the snapshot: %w", err)
	}
	boundary := snap.Pos

	// The transactions that were committed while the snapshot was taken
	// are not in the chain yet. They are drained into the spool, so that
	// the chain ends at the boundary of the snapshot.
	if ack.Seq > 0 {
		if err := p.drain(ctx, ack, boundary); err != nil {
			_ = os.Remove(path)
			return protocol.SnapshotMeta{}, nil, err
		}
		ack, err = p.sp.Durable()
		if err != nil {
			_ = os.Remove(path)
			return protocol.SnapshotMeta{}, nil, fmt.Errorf("snapshot: the durable acknowledgement of the spool: %w", err)
		}
	}

	meta, err := p.meta(ctx, path, ack)
	if err != nil {
		_ = os.Remove(path)
		return protocol.SnapshotMeta{}, nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		_ = os.Remove(path)
		return protocol.SnapshotMeta{}, nil, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	return meta, &snapshotFile{path: path, file: file}, nil
}

// snapshotPath returns the path of the file of a snapshot.
//
// The name is unique, two snapshots of two databases may live in the same
// directory.
func (p *Producer) snapshotPath() (string, error) {
	f, err := os.CreateTemp(p.cfg.Dir, "snapshot-*.db")
	if err != nil {
		return "", fmt.Errorf("snapshot: create a temporary file: %w", err)
	}
	path := f.Name()
	_ = f.Close()
	return path, nil
}

// drain appends the transactions between the chain and the boundary of the
// snapshot to the spool.
//
// The chain is not continuous any more when the write-ahead log was reset
// while the snapshot was taken: the snapshot has to be taken again.
func (p *Producer) drain(ctx context.Context, ack spool.Ack, boundary driver.Position) error {
	if ack.Seq == 0 {
		// The chain is empty, the snapshot is the baseline of the chain
		// that follows it.
		return nil
	}
	if ack.Pos.Salt1 != boundary.Salt1 || ack.Pos.Salt2 != boundary.Salt2 {
		return fmt.Errorf("%w: the write-ahead log was reset while the snapshot was taken", errRetry)
	}
	if boundary.Frame <= ack.Pos.Frame {
		// The chain already ends at the boundary of the snapshot.
		return nil
	}
	batch, err := p.drv.ReadChanges(ctx, driver.Position{
		Salt1:     ack.Pos.Salt1,
		Salt2:     ack.Pos.Salt2,
		Frame:     ack.Pos.Frame,
		WALOffset: ack.Pos.WALOffset,
	})
	if err != nil {
		return fmt.Errorf("%w: read the changes: %w", errRetry, err)
	}
	for _, change := range batch.Changes {
		if change.Pos.Frame > boundary.Frame {
			// The transaction was committed after the boundary of the
			// snapshot, it belongs to the chain that follows it.
			break
		}
		tx := spool.Tx{
			Pos: spool.Pos{
				Salt1:     change.Pos.Salt1,
				Salt2:     change.Pos.Salt2,
				Frame:     change.Pos.Frame,
				WALOffset: change.Pos.WALOffset,
			},
			CommitPages: change.CommitPages,
		}
		for _, page := range change.Pages {
			tx.Pages = append(tx.Pages, spool.Page{Pgno: page.Pgno, Data: page.Data})
		}
		if err := p.sp.Append(ctx, tx); err != nil {
			return fmt.Errorf("snapshot: append a transaction to the spool: %w", err)
		}
	}
	if err := p.sp.Flush(ctx); err != nil {
		return fmt.Errorf("snapshot: flush the spool: %w", err)
	}
	if err := p.sp.Sync(ctx); err != nil {
		return fmt.Errorf("snapshot: sync the spool: %w", err)
	}
	return nil
}

// meta returns the manifest of the snapshot at path.
//
// The baseline of the snapshot is the sequence number of the last
// transaction the chain holds.
func (p *Producer) meta(ctx context.Context, path string, ack spool.Ack) (protocol.SnapshotMeta, error) {
	info, err := os.Stat(path)
	if err != nil {
		return protocol.SnapshotMeta{}, fmt.Errorf("snapshot: stat %s: %w", path, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return protocol.SnapshotMeta{}, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	var head [headerSize]byte
	if _, err := f.ReadAt(head[:], 0); err != nil {
		return protocol.SnapshotMeta{}, fmt.Errorf("snapshot: read the header of %s: %w", path, err)
	}
	pageSize := uint32(binary.BigEndian.Uint16(head[headerPageSize : headerPageSize+2]))
	if pageSize == 1 {
		// The header names the page size 65536 as the value one.
		pageSize = 65536
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return protocol.SnapshotMeta{}, fmt.Errorf("snapshot: read %s: %w", path, err)
	}
	var checksum [32]byte
	copy(checksum[:], h.Sum(nil))
	return protocol.SnapshotMeta{
		FormatVersion: 1,
		DatabaseID:    segment.DatabaseID(p.cfg.DatabaseID),
		StreamID:      p.cfg.StreamID,
		Epoch:         p.sp.Epoch(),
		BaselineSeq:   ack.Seq,
		PageSize:      pageSize,
		Bytes:         uint64(info.Size()),
		Checksum:      checksum,
	}, nil
}

// snapshotFile is the reader of the file of a snapshot.
//
// The file is removed when the reader is closed.
type snapshotFile struct {
	path string
	file *os.File
}

// Read reads a part of the file of the snapshot.
func (s *snapshotFile) Read(p []byte) (int, error) {
	return s.file.Read(p)
}

// Close closes the file of the snapshot and removes it.
func (s *snapshotFile) Close() error {
	err := s.file.Close()
	_ = os.Remove(s.path)
	return err
}
