package backup

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestApplierAppliesThePagesOfTheChain(t *testing.T) {
	dir := t.TempDir()
	primary := newTestDatabase(t, dir, "primary.db", true)
	app := newTestApplier(t, dir, "app.db")
	installSnapshot(t, app, primary, "app.db", 3, 10)

	write(t, primary, "second")
	tx := transaction(t, primary, 11)
	if err := app.Apply(context.Background(), tx); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := app.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The backup holds the same data as the primary.
	backup := filepath.Join(dir, dbName)
	if rows := query(t, backup); !slices.Equal(rows, []string{"first", "second"}) {
		t.Errorf("the backup holds %v, want the rows of the primary", rows)
	}
	// The relay writes the file itself, it leaves no journal behind.
	for _, name := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(backup + name); err == nil {
			t.Errorf("the backup has a %s file", name)
		}
	}
}

func TestApplierNormalizesTheHeaderOfTheBackup(t *testing.T) {
	dir := t.TempDir()
	primary := newTestDatabase(t, dir, "primary.db", true)
	app := newTestApplier(t, dir, "app.db")
	installSnapshot(t, app, primary, "app.db", 3, 10)

	// The primary runs in the mode of the write-ahead log, the backup is a
	// plain database with a rollback journal.
	b, err := os.ReadFile(filepath.Join(dir, dbName))
	if err != nil {
		t.Fatalf("read the backup: %v", err)
	}
	if b[18] != 1 || b[19] != 1 {
		t.Errorf("the backup holds the versions %d and %d, want 1 and 1", b[18], b[19])
	}
	pages := binary.BigEndian.Uint32(b[28:32])
	if uint64(pages) != uint64(len(b)/testPageSize) {
		t.Errorf("the header names %d pages, the file holds %d", pages, len(b)/testPageSize)
	}
	if binary.BigEndian.Uint32(b[92:96]) != binary.BigEndian.Uint32(b[24:28]) {
		t.Error("the header does not tell that the size of the database is valid")
	}
}

func TestApplierPersistsTheSequenceAtTheBoundaryOfASegment(t *testing.T) {
	dir := t.TempDir()
	primary := newTestDatabase(t, dir, "primary.db", true)
	app := newTestApplier(t, dir, "app.db")
	installSnapshot(t, app, primary, "app.db", 3, 10)

	write(t, primary, "second")
	if err := app.Apply(context.Background(), transaction(t, primary, 11)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// The pages are written but the segment did not end, the sequence
	// number is not persisted yet.
	st, err := readState(app.statePath)
	if err != nil {
		t.Fatalf("readState: %v", err)
	}
	if st.AppliedSeq != 10 {
		t.Errorf("the state holds the sequence number %d, want 10", st.AppliedSeq)
	}
	applied, err := app.AppliedSeq(context.Background())
	if err != nil {
		t.Fatalf("AppliedSeq: %v", err)
	}
	if applied != 10 {
		t.Errorf("the applier holds the sequence number %d, want 10", applied)
	}

	if err := app.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	st, err = readState(app.statePath)
	if err != nil {
		t.Fatalf("readState: %v", err)
	}
	if st.AppliedSeq != 11 {
		t.Errorf("the state holds the sequence number %d, want 11", st.AppliedSeq)
	}
}

func TestApplierReplaysTheSegmentThatWasNotPersisted(t *testing.T) {
	dir := t.TempDir()
	primary := newTestDatabase(t, dir, "primary.db", true)
	app := newTestApplier(t, dir, "app.db")
	installSnapshot(t, app, primary, "app.db", 3, 10)

	write(t, primary, "second")
	tx := transaction(t, primary, 11)
	if err := app.Apply(context.Background(), tx); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// The backup is killed before the state of the segment is written.

	// The backup starts again and reads the state of the file.
	st, err := readState(app.statePath)
	if err != nil {
		t.Fatalf("readState: %v", err)
	}
	if st.AppliedSeq != 10 {
		t.Fatalf("the state holds the sequence number %d, want 10", st.AppliedSeq)
	}
	// The segment is applied again, the pages are the same and the content
	// of the backup does not change.
	backup := filepath.Join(dir, dbName)
	before := checksum(t, backup)
	if err := app.Apply(context.Background(), tx); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := app.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	after := checksum(t, backup)
	if before != after {
		t.Error("the replay of the segment changed the backup")
	}
	applied, err := app.AppliedSeq(context.Background())
	if err != nil {
		t.Fatalf("AppliedSeq: %v", err)
	}
	if applied != 11 {
		t.Errorf("the applier holds the sequence number %d, want 11", applied)
	}
}

func TestApplierRefusesPagesOfAnotherSize(t *testing.T) {
	dir := t.TempDir()
	primary := newTestDatabase(t, dir, "primary.db", true)
	app := newTestApplier(t, dir, "app.db")
	installSnapshot(t, app, primary, "app.db", 3, 10)

	tx := transaction(t, primary, 11)
	tx.Pages[0].Data = make([]byte, 1024)
	err := app.Apply(context.Background(), tx)
	if err == nil {
		t.Fatal("the applier accepted pages of another size")
	}
}

func TestApplierRefusesPagesOutsideTheDatabase(t *testing.T) {
	dir := t.TempDir()
	primary := newTestDatabase(t, dir, "primary.db", true)
	app := newTestApplier(t, dir, "app.db")
	installSnapshot(t, app, primary, "app.db", 3, 10)

	tx := transaction(t, primary, 11)
	tx.Pages[0].Pgno = 0
	if err := app.Apply(context.Background(), tx); err == nil {
		t.Fatal("the applier accepted the page 0")
	}
	tx.Pages[0].Pgno = tx.CommitPages + 1
	if err := app.Apply(context.Background(), tx); err == nil {
		t.Fatal("the applier accepted a page beyond the size of the database")
	}
}
