package spool

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cnlangzi/sqlite-relay/segment"
)

// scanResult is what the scan of the segments directory found.
type scanResult struct {
	// segs holds the segments of the chain, ordered by the sequence number
	// of their first transaction.
	segs []SegmentRef
	// pos is the position in the write-ahead log of the last transaction
	// of the chain.
	pos Pos
	// pageSize is the size of the pages of the database.
	pageSize uint32
}

// recover reads the state of the spool from the disk.
//
// The segments are the truth: they decide which transactions are durable and
// which sequence number the next transaction gets. The state files are
// rebuilt from them, a state file that is ahead or behind them does not
// change anything.
func (s *Spool) recover() error {
	s.dbID = segment.DatabaseID(s.cfg.DatabaseID)

	stream, err := s.readStreamState()
	if err != nil {
		return err
	}
	if stream != nil && stream.DBID != "" && stream.DBID != hex.EncodeToString(s.dbID[:]) {
		return fmt.Errorf("%w: the directory holds the database %q with the identifier %s", ErrDatabaseMismatch, stream.DatabaseID, stream.DBID)
	}
	if stream != nil {
		s.createdAt = stream.CreatedAt
	}

	// The files in the temporary directory were never published. Their
	// data is captured from the write-ahead log again.
	if err := s.removeFiles(s.tmpDir); err != nil {
		return err
	}

	result, reason, err := s.scanSegments()
	if err != nil {
		return err
	}
	s.segs = result.segs
	s.pageSize = result.pageSize

	// The epoch of the chain comes from the segments, or from the state
	// file when the chain holds none. It is known even when the chain is
	// broken: a reset has to raise it.
	switch {
	case len(s.segs) > 0:
		s.epoch = s.segs[0].Epoch
	case stream != nil:
		s.epoch = stream.Epoch
	}
	if reason != "" {
		s.broken = true
		s.reason = reason
		return fmt.Errorf("%w: %s", ErrChainBroken, reason)
	}

	if len(s.segs) > 0 {
		last := s.segs[len(s.segs)-1]
		s.nextSeq = last.SeqEnd + 1
		s.durable = Ack{Seq: last.SeqEnd, Pos: result.pos}
	} else {
		if stream != nil {
			if stream.PageSize != 0 {
				s.pageSize = stream.PageSize
			}
			if stream.NextSeq > 0 {
				s.nextSeq = stream.NextSeq
			}
		}
		if s.nextSeq == 0 {
			s.nextSeq = 1
		}
	}
	s.lastBarrier = time.Now()

	if err := s.writeStreamState(); err != nil {
		return err
	}
	return s.writeCaptureState()
}

// scanSegments reads the segments of the spool.
//
// The chain of the segments has to be continuous: the first transaction of a
// segment has to follow the last one of its predecessor. A segment that does
// not hold what it says is dropped when it is the last one of the chain,
// the data of it is captured from the write-ahead log again. A hole in the
// chain is reported: the data of it was durable once and may be gone.
func (s *Spool) scanSegments() (scanResult, string, error) {
	entries, err := os.ReadDir(s.segDir)
	if err != nil {
		if os.IsNotExist(err) {
			return scanResult{}, "", nil
		}
		return scanResult{}, "", fmt.Errorf("spool: read the segments directory: %w", err)
	}

	var result scanResult
	type segmentFile struct {
		path     string
		seqStart uint64
		seqEnd   uint64
	}
	var files []segmentFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		seqStart, seqEnd, err := parseSegmentName(e.Name())
		if err != nil {
			return result, fmt.Sprintf("the file %s of the segments directory is not a segment", e.Name()), nil
		}
		files = append(files, segmentFile{
			path:     filepath.Join(s.segDir, e.Name()),
			seqStart: seqStart,
			seqEnd:   seqEnd,
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].seqStart < files[j].seqStart })

	for i, f := range files {
		seg, err := segment.ReadFile(f.path)
		if err != nil {
			if i == len(files)-1 {
				// The last segment of the chain is broken. The data of
				// it is captured from the write-ahead log again.
				_ = os.Remove(f.path)
				continue
			}
			return result, fmt.Sprintf("the segment %s does not hold what it says: %v", filepath.Base(f.path), err), nil
		}
		if seg.Header.DatabaseID != s.dbID {
			return scanResult{}, "", fmt.Errorf("%w: the segment %s belongs to another database", ErrDatabaseMismatch, filepath.Base(f.path))
		}
		if seg.Header.SeqStart != f.seqStart || seg.Trailer.SeqEnd != f.seqEnd {
			return result, fmt.Sprintf("the segment %s does not hold the range of sequence numbers of its name", filepath.Base(f.path)), nil
		}
		if len(result.segs) > 0 {
			prev := result.segs[len(result.segs)-1]
			if seg.Header.Epoch != prev.Epoch {
				return result, fmt.Sprintf("the segment %s belongs to another generation of the chain", filepath.Base(f.path)), nil
			}
			if seg.Header.SeqStart != prev.SeqEnd+1 {
				return result, fmt.Sprintf("the segment %s starts at the sequence number %d, the segment before it ends at %d", filepath.Base(f.path), seg.Header.SeqStart, prev.SeqEnd), nil
			}
		}
		info, err := os.Stat(f.path)
		if err != nil {
			return scanResult{}, "", fmt.Errorf("spool: stat %s: %w", f.path, err)
		}
		result.segs = append(result.segs, SegmentRef{
			Path:      f.path,
			SeqStart:  seg.Header.SeqStart,
			SeqEnd:    seg.Trailer.SeqEnd,
			Epoch:     seg.Header.Epoch,
			Bytes:     info.Size(),
			CreatedAt: seg.Header.CreatedAtTime(),
		})
		result.pos = spoolPos(seg.Trailer.Pos())
		result.pageSize = seg.Header.PageSize
	}
	return result, "", nil
}

// parseSegmentName reads the sequence numbers of the name of a segment.
func parseSegmentName(name string) (uint64, uint64, error) {
	base := strings.TrimSuffix(name, ".seg")
	parts := strings.Split(base, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("spool: %q is not the name of a segment", name)
	}
	seqStart, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("spool: %q is not the name of a segment: %w", name, err)
	}
	seqEnd, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("spool: %q is not the name of a segment: %w", name, err)
	}
	return seqStart, seqEnd, nil
}

// diskFree returns the number of bytes that are left on the file system the
// path lives on.
//
// The number is zero when it cannot be read.
func diskFree(path string) int64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}
	return int64(stat.Bavail) * int64(stat.Bsize)
}
