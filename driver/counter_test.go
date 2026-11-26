package driver

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/internal/wal"
)

// TestChangeCounterOfThePrimaryDatabase checks the change counter of the
// primary database.
//
// The counter is the counter of the newest page 1 image the primary holds:
// the image of the last committed transaction of the log that wrote page 1,
// or the page 1 of the database file when the log holds no such image.
func TestChangeCounterOfThePrimaryDatabase(t *testing.T) {
	d, path := newTestDriver(t, Config{})

	// The log holds no frame: the database file holds the counter.
	want := fileCounter(t, path)
	got, err := d.ChangeCounter()
	if err != nil {
		t.Fatalf("ChangeCounter: %v", err)
	}
	if got != want {
		t.Errorf("the change counter is %d, want %d", got, want)
	}

	// A transaction that writes page 1.
	createTable(t, d)
	want = logPageOneCounter(t, path)
	got, err = d.ChangeCounter()
	if err != nil {
		t.Fatalf("ChangeCounter: %v", err)
	}
	if got != want {
		t.Errorf("the change counter is %d, want %d", got, want)
	}

	// The counter does not change between two commits: a transaction that
	// does not write page 1 does not change it.
	insert(t, d, "a")
	got, err = d.ChangeCounter()
	if err != nil {
		t.Fatalf("ChangeCounter: %v", err)
	}
	if got != want {
		t.Errorf("the change counter is %d after a commit, want %d", got, want)
	}

	// The counter does not change when the driver reads the log.
	scanAll(t, d)
	got, err = d.ChangeCounter()
	if err != nil {
		t.Fatalf("ChangeCounter: %v", err)
	}
	if got != want {
		t.Errorf("the change counter is %d after the scan, want %d", got, want)
	}
}

// TestChangeCounterOfAnUnreadableDatabase checks that the change counter of
// a database file that cannot be read is an error.
func TestChangeCounterOfAnUnreadableDatabase(t *testing.T) {
	d, path := newTestDriver(t, Config{})

	// The database file is gone: the log holds no frame and the file holds
	// the counter.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove the database file: %v", err)
	}
	if _, err := d.ChangeCounter(); err == nil {
		t.Fatal("the change counter of a database file that does not exist was read")
	}
}

// fileCounter returns the change counter of the page 1 of the database file.
func fileCounter(t *testing.T, path string) uint32 {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the database file: %v", err)
	}
	if len(b) < 28 {
		t.Fatalf("the database file is %d bytes long", len(b))
	}
	return binary.BigEndian.Uint32(b[24:28])
}

// logPageOneCounter returns the change counter of the newest page 1 image
// of the log of the database.
func logPageOneCounter(t *testing.T, path string) uint32 {
	t.Helper()
	reader, err := wal.Open(path + "-wal")
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer reader.Close()
	scan, err := reader.Scan()
	if err != nil {
		t.Fatalf("scan WAL: %v", err)
	}
	var counter uint32
	var found bool
	for _, tx := range scan.Transactions {
		for _, f := range tx.Frames {
			if f.Pgno == 1 {
				counter, found = binary.BigEndian.Uint32(f.Data[24:28]), true
			}
		}
	}
	if !found {
		t.Fatal("the log holds no page 1 image")
	}
	return counter
}

// TestChangeCounterIsReadFromTheDiskBeforeTheFirstScan checks that the
// change counter is known before the driver read the log.
func TestChangeCounterIsReadFromTheDiskBeforeTheFirstScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	d, err := Open(context.Background(), Config{DSN: path + "?_journal_mode=WAL", ScanInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	createTable(t, d)
	want := logPageOneCounter(t, path)
	got, err := d.ChangeCounter()
	if err != nil {
		t.Fatalf("ChangeCounter: %v", err)
	}
	if got != want {
		t.Errorf("the change counter is %d, want %d", got, want)
	}
}
