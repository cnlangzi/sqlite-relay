package driver

import (
	"fmt"
	"slices"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// Page is one database page.
type Page struct {
	// Pgno is the page number, starting at one.
	Pgno uint32
	// Data is the page image. It is exactly one page long.
	Data []byte
}

// Change is one committed transaction.
//
// A change is always a whole transaction. The consumer may merge several
// changes into one segment, but it must never split a change.
type Change struct {
	// Seq is a counter the driver keeps for the order of the changes it
	// produced. It is not a WAL frame number and not a sequence number that
	// survives a restart.
	Seq uint64
	// Pos is the capture position after the transaction.
	Pos Position
	// CommitPages is the size of the database in pages after the commit.
	CommitPages uint32
	// Pages holds the pages the transaction changed, ordered by page
	// number.
	Pages []Page
}

// Batch is the result of a capture round.
type Batch struct {
	// From is the position the batch starts at.
	From Position
	// To is the position after the last change of the batch.
	To Position
	// Changes holds the transactions that were committed since the last
	// capture round, in the order they were committed.
	Changes []Change
}

// changeFromTransaction turns a WAL transaction into a change.
//
// The pages are ordered by page number and a page that the transaction wrote
// more than once keeps its last version. Pages beyond the size the database
// has after the commit are dropped: they belong to a database that shrank.
func changeFromTransaction(tx wal.Transaction) Change {
	pages := pagesOfTransaction(tx)
	numbers := make([]uint32, 0, len(pages))
	for pgno := range pages {
		numbers = append(numbers, pgno)
	}
	slices.Sort(numbers)

	out := make([]Page, 0, len(numbers))
	for _, pgno := range numbers {
		out = append(out, Page{Pgno: pgno, Data: pages[pgno]})
	}
	return Change{CommitPages: tx.CommitPages, Pages: out}
}

// pagesOfTransaction collects the pages a transaction changed.
//
// A page that the transaction wrote more than once keeps its last version.
// A page beyond the size the database has after the commit is left out: it
// belongs to a database that shrank.
func pagesOfTransaction(tx wal.Transaction) map[uint32][]byte {
	pages := make(map[uint32][]byte, len(tx.Frames))
	for _, f := range tx.Frames {
		if f.Pgno > tx.CommitPages {
			continue
		}
		pages[f.Pgno] = f.Data
	}
	return pages
}

// checkFrames verifies that the frame count the driver derived from the WAL
// bytes is consistent with the frame count SQLite reports.
//
// The WAL may grow while it is read, so the count that was read before the
// scan is a lower bound and the count that was read after the scan is an
// upper bound of what the scan may find. A count outside of that window
// means the WAL was reset while it was read and the result has to be
// dropped.
func checkFrames(before, parsed, after int) error {
	if parsed < before || parsed > after {
		return fmt.Errorf("driver: WAL held %d committed frames while it was read, SQLite reported %d before and %d after, the WAL was reset while it was read", parsed, before, after)
	}
	return nil
}
