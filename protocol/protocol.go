// Package protocol carries the segments of a spool to a backup peer.
//
// The backup dials the source, both sides prove that they share the token of
// the deployment and the source pushes the segments of the spool in the order
// of their sequence numbers. The backup applies the transactions of a segment
// one by one and acknowledges the sequence number it applied. A connection
// that breaks is resumed at the sequence number the backup applied and
// persisted, so that no segment is lost and no segment is applied twice.
//
// The wire format is a fixed frame header followed by the payload of a
// message. The messages are encoded by hand, there is no JSON and no
// protobuf on the wire.
package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// ProtocolVersion is the version of the protocol this package speaks.
//
// The version is 2: the hello of the backup carries the flag that asks for a
// snapshot and the snapshot travels in the messages of the version 2.
const ProtocolVersion = 2

// Defaults of the configuration.
const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultIdleTimeout       = 30 * time.Second
	defaultMaxBatchBytes     = 16 << 20
	defaultMaxUnackedBytes   = 8 << 20
	defaultBackoffMin        = time.Second
	defaultBackoffMax        = 60 * time.Second
)

// MaxSegmentsPerBatch is the number of segments one batch may hold.
const MaxSegmentsPerBatch = 64

// Errors of the package.
var (
	// ErrInvalidFrame reports a frame that does not hold what it says.
	ErrInvalidFrame = errors.New("protocol: invalid frame")
	// ErrInvalidMessage reports a message that does not hold what it says.
	ErrInvalidMessage = errors.New("protocol: invalid message")
	// ErrResyncRequired reports that the chain of the segments is not
	// continuous any more and that a new snapshot is needed.
	ErrResyncRequired = errors.New("protocol: a new snapshot is required")
)

// Config holds the settings of a source and of a backup.
type Config struct {
	// TokenFile is the path of the file that holds the shared token.
	TokenFile string
	// DatabaseID is the name of the database the segments belong to.
	DatabaseID string
	// StreamID is the number of the stream. The first generation of the
	// protocol carries one stream.
	StreamID uint64
	// BackupID is the name of the backup. The source checks the name
	// against the list of the backups that may connect.
	BackupID string
	// AllowedBackups holds the identifiers of the backups that may connect.
	// An empty list allows every backup that knows the token.
	AllowedBackups []string
	// HeartbeatInterval is the time between two heartbeats.
	HeartbeatInterval time.Duration
	// IdleTimeout is the time a connection may stay silent before it is
	// closed.
	IdleTimeout time.Duration
	// MaxBatchBytes is the size of the payload of a batch. A segment that
	// does not fit is sent in a batch of its own.
	MaxBatchBytes int64
	// MaxFrameBytes is the size of the payload of a frame.
	MaxFrameBytes int64
	// MaxUnackedBytes is the number of bytes the source may have in flight.
	// The backup may lower it with a flow control message.
	MaxUnackedBytes int64
	// BackoffMin is the shortest delay between two connection attempts.
	BackoffMin time.Duration
	// BackoffMax is the longest delay between two connection attempts.
	BackoffMax time.Duration
}

// withDefaults returns the configuration with the defaults filled in.
func (c Config) withDefaults() Config {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = defaultMaxBatchBytes
	}
	if c.MaxFrameBytes <= 0 {
		c.MaxFrameBytes = MaxFrameBytes
	}
	if c.MaxUnackedBytes <= 0 {
		c.MaxUnackedBytes = defaultMaxUnackedBytes
	}
	if c.BackoffMin <= 0 {
		c.BackoffMin = defaultBackoffMin
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = defaultBackoffMax
	}
	return c
}

// validate checks the settings that have no default.
func (c Config) validate() error {
	if c.TokenFile == "" {
		return errors.New("protocol: the path of the token file is empty")
	}
	if c.DatabaseID == "" {
		return errors.New("protocol: the database ID is empty")
	}
	if c.MaxBatchBytes > c.MaxFrameBytes {
		return fmt.Errorf("protocol: the batch holds %d bytes, the frame holds %d bytes", c.MaxBatchBytes, c.MaxFrameBytes)
	}
	if len(c.BackupID) > MaxBackupIDLen {
		return fmt.Errorf("protocol: the name of the backup is %d bytes long, the limit is %d bytes", len(c.BackupID), MaxBackupIDLen)
	}
	// The peer sends a heartbeat every heartbeat interval, the connection
	// has to survive a heartbeat that is late.
	if c.IdleTimeout < 2*c.HeartbeatInterval {
		return fmt.Errorf("protocol: the idle timeout of %s is shorter than twice the heartbeat interval of %s", c.IdleTimeout, c.HeartbeatInterval)
	}
	return nil
}

// SegmentSource provides the segments the source pushes.
//
// The sequence numbers and the epoch of the segments are the ones of the
// spool, the protocol does not number them again.
type SegmentSource interface {
	// Epoch returns the generation of the chain of the segments.
	Epoch() uint64
	// OldestSeq returns the lowest sequence number the source can still
	// provide. It is zero when the source holds no segment.
	OldestSeq() uint64
	// SegmentsAfter returns the published segments that hold a transaction
	// after seq, ordered by the sequence number of their first
	// transaction.
	//
	// The segments hold at most limit bytes. A segment that does not fit
	// into the limit is returned anyway, the source sends it in a batch of
	// its own. The source asks for the segments it can send at the moment,
	// so that a segment is read once and not once per batch.
	SegmentsAfter(ctx context.Context, seq uint64, limit int64) ([]Segment, error)
}

// Segment is a segment the source pushes.
type Segment struct {
	// SeqStart is the sequence number of the first transaction of the
	// segment.
	SeqStart uint64
	// SeqEnd is the sequence number of the last transaction of the segment.
	SeqEnd uint64
	// Epoch is the generation of the chain the segment is part of.
	Epoch uint64
	// Bytes holds the segment file as it is written on the disk.
	Bytes []byte
}

// SegmentSink receives the segments the backup applies.
//
// The sink applies a transaction and not a segment: the boundary of a segment
// and the boundary of the applied sequence do not line up, a resumed
// connection may start in the middle of a segment.
type SegmentSink interface {
	// Apply applies a transaction. It returns nil when the transaction is
	// applied. The transactions of a segment are durable when the sink is
	// flushed, see Flusher.
	Apply(ctx context.Context, tx segment.Tx) error
	// AppliedSeq returns the highest sequence number that is applied and
	// persisted.
	AppliedSeq(ctx context.Context) (uint64, error)
	// Epoch returns the generation of the chain the backup is part of. The
	// epoch of the sink and the epoch of its applied sequence belong
	// together.
	Epoch() uint64
}

// Flusher is implemented by a sink that makes the applied transactions
// durable at the boundary of a segment.
//
// The protocol calls it before it acknowledges a segment: the acknowledgement
// means that the transactions up to the sequence number it names are applied
// and that the sequence number is persisted.
type Flusher interface {
	// Flush makes the applied transactions durable.
	Flush(ctx context.Context) error
}

// SnapshotSink is implemented by a sink that takes a snapshot to establish
// its baseline.
//
// A sink without a baseline asks for a snapshot in its hello. The source
// answers with the manifest of the snapshot and with its bytes, the sink
// installs it and the increments of the chain that follow the baseline
// sequence bring the backup up to date.
type SnapshotSink interface {
	// NeedsSnapshot reports whether the sink has no baseline yet.
	NeedsSnapshot() bool
	// BeginSnapshot starts the installation of a snapshot. The returned
	// writer receives the bytes of the snapshot.
	BeginSnapshot(ctx context.Context, meta SnapshotMeta) (io.Writer, error)
	// CommitSnapshot finishes the installation of the snapshot.
	CommitSnapshot(ctx context.Context) error
}

// SnapshotProvider is implemented by the source of a deployment.
//
// The provider produces a consistent snapshot of the primary database and
// binds it to the sequence number of the last transaction the snapshot
// holds.
type SnapshotProvider interface {
	// Snapshot writes a consistent snapshot of the database and returns its
	// metadata and a reader of the snapshot. The reader removes the file
	// when it is closed.
	Snapshot(ctx context.Context) (SnapshotMeta, io.ReadCloser, error)
}
