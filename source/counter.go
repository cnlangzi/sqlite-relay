package source

import (
	"encoding/binary"

	"github.com/cnlangzi/sqlite-relay/segment"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// changeCounterOffset is the offset of the change counter in page 1.
//
// The change counter is a field of the file format of SQLite: it sits at
// offset 24 of the first page of the database.
const changeCounterOffset = 24

// changeCounter is the change counter of a state of the primary database.
//
// The counter is unknown when the state cannot be read: the decision about
// the continuity of the chain skips the counter then.
type changeCounter struct {
	// value is the counter of the page 1 image.
	value uint32
	// known reports that the counter was read.
	known bool
}

// chainChangeCounter returns the change counter of the end of the chain.
//
// The counter is the one of the newest page 1 image the chain holds: the
// image of the last transaction that wrote page 1. A transaction that does
// not write page 1 does not move the counter.
//
// The segments are read from the newest to the oldest one: the chain may hold
// a page 1 image in a segment that is not the last one. The counter is
// unknown when the chain holds no segment, when the segments cannot be read
// and when the chain holds no page 1 image.
func chainChangeCounter(sp *spool.Spool) changeCounter {
	segs, err := sp.Segments()
	if err != nil {
		return changeCounter{}
	}
	for i := len(segs) - 1; i >= 0; i-- {
		seg, err := segment.ReadFile(segs[i].Path)
		if err != nil {
			continue
		}
		for j := len(seg.Txs) - 1; j >= 0; j-- {
			for _, page := range seg.Txs[j].Pages {
				if page.Pgno != 1 {
					continue
				}
				return changeCounter{
					value: binary.BigEndian.Uint32(page.Data[changeCounterOffset : changeCounterOffset+4]),
					known: true,
				}
			}
		}
	}
	return changeCounter{}
}
