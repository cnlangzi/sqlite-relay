package protocol

import (
	"context"
	"fmt"
	"os"

	"github.com/cnlangzi/sqlite-relay/spool"
)

// SpoolSource provides the segments of a spool to the source.
//
// The spool holds the segments that passed the barrier of its mode, a
// segment that waits for the barrier is not part of the list and is not
// pushed to the backup.
type SpoolSource struct {
	spool *spool.Spool
}

// NewSpoolSource returns the segments of the spool as a segment source.
func NewSpoolSource(s *spool.Spool) *SpoolSource {
	return &SpoolSource{spool: s}
}

// Epoch returns the generation of the chain of the spool.
func (s *SpoolSource) Epoch() uint64 {
	return s.spool.Epoch()
}

// OldestSeq returns the lowest sequence number the spool can still provide.
//
// It is zero when the spool holds no segment.
func (s *SpoolSource) OldestSeq() uint64 {
	refs, err := s.spool.Segments()
	if err != nil || len(refs) == 0 {
		return 0
	}
	return refs[0].SeqStart
}

// SegmentsAfter returns the segments of the spool that hold a transaction
// after seq.
//
// The bytes of a segment are read from the file of the segment, the segment
// is never changed and it is never written again.
func (s *SpoolSource) SegmentsAfter(ctx context.Context, seq uint64) ([]Segment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	refs, err := s.spool.Segments()
	if err != nil {
		return nil, err
	}
	var out []Segment
	for _, ref := range refs {
		if ref.SeqEnd <= seq {
			continue
		}
		b, err := os.ReadFile(ref.Path)
		if err != nil {
			return nil, fmt.Errorf("protocol: read the segment %s: %w", ref.Path, err)
		}
		out = append(out, Segment{
			SeqStart: ref.SeqStart,
			SeqEnd:   ref.SeqEnd,
			Epoch:    ref.Epoch,
			Bytes:    b,
		})
	}
	return out, nil
}
