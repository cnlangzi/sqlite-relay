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
// The counter is the one of the page 1 image of the last transaction of the
// last published segment. It is unknown when the chain holds no segment,
// when the segment cannot be read and when the last transaction does not
// hold a page 1 image.
func chainChangeCounter(sp *spool.Spool) changeCounter {
	segs, err := sp.Segments()
	if err != nil || len(segs) == 0 {
		return changeCounter{}
	}
	seg, err := segment.ReadFile(segs[len(segs)-1].Path)
	if err != nil || len(seg.Txs) == 0 {
		return changeCounter{}
	}
	tx := seg.Txs[len(seg.Txs)-1]
	for _, page := range tx.Pages {
		if page.Pgno != 1 {
			continue
		}
		return changeCounter{
			value: binary.BigEndian.Uint32(page.Data[changeCounterOffset : changeCounterOffset+4]),
			known: true,
		}
	}
	return changeCounter{}
}
