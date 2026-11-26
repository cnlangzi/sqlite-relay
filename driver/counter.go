package driver

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// changeCounterOffset is the offset of the change counter in page 1.
//
// The change counter is a field of the file format of SQLite: it sits at
// offset 24 of the first page of the database.
const changeCounterOffset = 24

// ChangeCounter returns the change counter of the primary database.
//
// The counter counts the writes of page 1: the pager writes the new counter
// into the page 1 image of a transaction that writes page 1. The counter of
// the primary database is the counter of the newest page 1 image it holds:
// the image of the last committed transaction of the log that wrote page 1,
// or the page 1 of the database file when the log holds no such image.
//
// The counter does not count the transactions: the page 1 images of one
// generation of the log carry the same counter, because the pager writes the
// counter of the database file plus one. The counter moves when the database
// file holds a newer page 1.
//
// The counter is 32 bits wide and it wraps around. Two counters are only
// comparable when fewer than 2^32 writes of page 1 lie between them.
func (d *Driver) ChangeCounter() (uint32, error) {
	d.mu.Lock()
	counter, ok := d.changeCounter, d.changeCounterSet
	d.mu.Unlock()
	if ok {
		return counter, nil
	}
	// The driver did not read the log yet.
	return d.changeCounterFromDisk()
}

// changeCounterFromDisk reads the change counter of the primary database
// from the log and from the database file.
func (d *Driver) changeCounterFromDisk() (uint32, error) {
	reader, err := wal.Open(d.walPath)
	if err != nil {
		if !errors.Is(err, wal.ErrNoWAL) {
			return 0, fmt.Errorf("driver: open WAL: %w", err)
		}
		// The log holds no frame: the database file holds the state of the
		// primary database.
		return fileChangeCounter(d.dbPath)
	}
	defer func() { _ = reader.Close() }()
	scan, err := reader.Scan()
	if err != nil {
		return 0, fmt.Errorf("driver: read WAL: %w", err)
	}
	if counter, ok := newestPageOneCounter(scan); ok {
		return counter, nil
	}
	return fileChangeCounter(d.dbPath)
}

// noteChangeCounter remembers the change counter of the primary database.
//
// The counter of the newest page 1 image of the log wins: the counter of the
// primary database never goes back. The pager writes the counter of the
// database file plus one and the file holds a newer page 1 after every
// checkpoint, so the cached counter stays valid when the generation of the
// log changes.
//
// The database file holds the counter when the log holds no page 1 image. The
// file is only read while the counter is unknown: its counter never goes back
// either and the file is written by a checkpoint that may tear it.
func (d *Driver) noteChangeCounter(scan *wal.Scan) {
	counter, ok := newestPageOneCounter(scan)
	if !ok {
		d.mu.Lock()
		known := d.changeCounterSet
		d.mu.Unlock()
		if known {
			return
		}
		// The log holds no page 1 image: the database file holds the
		// counter of the primary database.
		c, err := fileChangeCounter(d.dbPath)
		if err != nil {
			return
		}
		counter = c
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.changeCounterSet && d.changeCounter >= counter {
		return
	}
	d.changeCounter = counter
	d.changeCounterSet = true
}

// newestPageOneCounter returns the change counter of the newest page 1 image
// of a scan of the log.
//
// The scan holds the transactions in the order they were committed: the
// newest page 1 image is the one of the last transaction that wrote page 1.
func newestPageOneCounter(scan *wal.Scan) (uint32, bool) {
	var counter uint32
	var found bool
	for _, tx := range scan.Transactions {
		for _, f := range tx.Frames {
			if f.Pgno == 1 {
				counter, found = changeCounterOf(f.Data), true
			}
		}
	}
	return counter, found
}

// changeCounterOf reads the change counter of a page 1 image.
func changeCounterOf(page []byte) uint32 {
	return binary.BigEndian.Uint32(page[changeCounterOffset : changeCounterOffset+4])
}

// fileChangeCounter reads the change counter of the page 1 of the database
// file.
func fileChangeCounter(path string) (uint32, error) {
	b, err := readBaseline(path)
	if err != nil {
		return 0, err
	}
	return b.changeCounter, nil
}
