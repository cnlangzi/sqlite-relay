package spool_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// toTx turns a change of the driver into a transaction of the spool.
//
// The peer of the driver does this conversion. The sequence number of the
// driver is dropped, the spool hands out its own.
func toTx(c driver.Change) spool.Tx {
	tx := spool.Tx{
		Pos: spool.Pos{
			Salt1:     c.Pos.Salt1,
			Salt2:     c.Pos.Salt2,
			Frame:     c.Pos.Frame,
			WALOffset: c.Pos.WALOffset,
		},
		CommitPages: c.CommitPages,
	}
	for _, p := range c.Pages {
		tx.Pages = append(tx.Pages, spool.Page{Pgno: p.Pgno, Data: append([]byte(nil), p.Data...)})
	}
	return tx
}

// toPosition turns the position of the spool into the position of the
// driver.
func toPosition(p spool.Pos) driver.Position {
	return driver.Position{
		Salt1:     p.Salt1,
		Salt2:     p.Salt2,
		Frame:     p.Frame,
		WALOffset: p.WALOffset,
	}
}

// newPrimary opens a driver on a fresh database.
func newPrimary(t *testing.T, dir string) (*driver.Driver, string) {
	t.Helper()
	path := filepath.Join(dir, "primary.db")
	d, err := driver.Open(context.Background(), driver.Config{
		DSN:          path + "?_journal_mode=WAL",
		ScanInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open the driver: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, path
}

// TestAcceptanceHandsTheDurableAckToTheDriver checks that the driver only
// writes the frames of the write-ahead log back when the spool says that
// the increments are durable.
func TestAcceptanceHandsTheDurableAckToTheDriver(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	d, _ := newPrimary(t, dir)

	if _, err := d.DB().Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create the table: %v", err)
	}
	for _, v := range []string{"a", "b", "c"} {
		if _, err := d.DB().Exec("INSERT INTO t(v) VALUES(?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	batch, err := d.Scan(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(batch.Changes) == 0 {
		t.Fatal("the driver did not capture the transactions")
	}

	s, err := spool.Open(ctx, spool.Config{
		Root:       filepath.Join(dir, "spool"),
		DatabaseID: "primary.db",
		Mode:       spool.ModeFull,
	})
	if err != nil {
		t.Fatalf("open the spool: %v", err)
	}
	defer s.Close()
	for _, c := range batch.Changes {
		if err := s.Append(ctx, toTx(c)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Without the acknowledgement the driver holds the frames back.
	before, err := d.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if before.Checkpointed != 0 {
		t.Errorf("the driver wrote %d frames back before the acknowledgement", before.Checkpointed)
	}

	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("durable: %v", err)
	}
	if ack.Seq == 0 {
		t.Fatal("the spool did not acknowledge the transactions")
	}
	if err := d.CaptureAck(ctx, toPosition(ack.Pos)); err != nil {
		t.Fatalf("capture ack: %v", err)
	}

	// The acknowledgement lets the driver write the frames back.
	after, err := d.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if after.Checkpointed == 0 {
		t.Error("the driver did not write the frames back after the acknowledgement")
	}
}

// TestAcceptanceResumesAfterRestart checks that the spool hands out the
// sequence numbers it persisted and that the driver continues at the
// position the spool acknowledged: no increment is lost and none is
// repeated.
func TestAcceptanceResumesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	d, _ := newPrimary(t, dir)

	if _, err := d.DB().Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create the table: %v", err)
	}
	for _, v := range []string{"a", "b"} {
		if _, err := d.DB().Exec("INSERT INTO t(v) VALUES(?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	batch, err := d.Scan(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	cfg := spool.Config{
		Root:       filepath.Join(dir, "spool"),
		DatabaseID: "primary.db",
		Mode:       spool.ModeFull,
	}
	s, err := spool.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open the spool: %v", err)
	}
	for _, c := range batch.Changes {
		if err := s.Append(ctx, toTx(c)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	ack, err := s.Durable()
	if err != nil {
		t.Fatalf("durable: %v", err)
	}
	if ack.Seq != uint64(len(batch.Changes)) {
		t.Fatalf("the spool acknowledged %d transactions, want %d", ack.Seq, len(batch.Changes))
	}
	if err := d.CaptureAck(ctx, toPosition(ack.Pos)); err != nil {
		t.Fatalf("capture ack: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close the spool: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close the driver: %v", err)
	}

	// The spool comes back with the acknowledgement it persisted.
	s2, err := spool.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open the spool again: %v", err)
	}
	defer s2.Close()
	ack2, err := s2.Durable()
	if err != nil {
		t.Fatalf("durable: %v", err)
	}
	if ack2 != ack {
		t.Errorf("the durable acknowledgement is %+v after the restart, want %+v", ack2, ack)
	}

	// The driver continues at the position the spool acknowledged.
	resume := toPosition(ack.Pos)
	d2, err := driver.Open(ctx, driver.Config{
		DSN:            filepath.Join(dir, "primary.db") + "?_journal_mode=WAL",
		ScanInterval:   20 * time.Millisecond,
		ResumePosition: &resume,
	})
	if err != nil {
		t.Fatalf("open the driver again: %v", err)
	}
	defer d2.Close()

	if _, err := d2.DB().Exec("INSERT INTO t(v) VALUES('c')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	batch2, err := d2.Scan(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(batch2.Changes) != 1 {
		t.Fatalf("the driver captured %d transactions after the restart, want 1", len(batch2.Changes))
	}
	for _, c := range batch2.Changes {
		if err := s2.Append(ctx, toTx(c)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := s2.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	ack3, err := s2.Durable()
	if err != nil {
		t.Fatalf("durable: %v", err)
	}
	if ack3.Seq != ack.Seq+1 {
		t.Errorf("the durable sequence number is %d after the restart, want %d", ack3.Seq, ack.Seq+1)
	}
}
