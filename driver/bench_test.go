package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// benchDriver opens a driver for a benchmark. The background work of the
// driver is switched off, so that the benchmark measures what it says it
// measures.
func benchDriver(b *testing.B) *Driver {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.db")
	d, err := Open(context.Background(), Config{
		DSN:          path + "?_journal_mode=WAL",
		ScanInterval: time.Hour,
	})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { d.Close() })
	if _, err := d.DB().Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}
	return d
}

// BenchmarkWalHook measures the cost the WAL hook adds to a commit.
//
// The hook may only do bounded work, so this number is the budget the
// business path spends on the notification.
func BenchmarkWalHook(b *testing.B) {
	d := benchDriver(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.walHook("main", i)
	}
}

// BenchmarkCommit measures a commit on a connection the driver manages.
func BenchmarkCommit(b *testing.B) {
	d := benchDriver(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('x')"); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
	b.StopTimer()
	// The WAL grows by the size of the pages a transaction wrote.
	if info, err := os.Stat(d.walPath); err == nil {
		b.ReportMetric(float64(info.Size())/float64(b.N), "wal-B/tx")
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tx/s")
}

// BenchmarkCaptureThroughput measures how many transactions the driver can
// capture per second.
func BenchmarkCaptureThroughput(b *testing.B) {
	d := benchDriver(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('x')"); err != nil {
			b.Fatalf("insert: %v", err)
		}
		if _, err := d.Scan(ctx); err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tx/s")
}

// BenchmarkScanEmpty measures the cost of a capture round that finds
// nothing.
func BenchmarkScanEmpty(b *testing.B) {
	d := benchDriver(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.Scan(ctx); err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
}

// BenchmarkCaptureLatency measures the delay between the commit of a
// transaction and the moment the driver hands it out.
//
// The delay has two parts: the time the commit itself takes and the time
// between the notification of the WAL hook and the capture that follows it.
func BenchmarkCaptureLatency(b *testing.B) {
	d := benchDriver(b)
	ctx := context.Background()
	var notifyToScan, commitToScan time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Drain the notifications of the earlier rounds.
		for len(d.Events()) > 0 {
			<-d.Events()
		}
		start := time.Now()
		if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('x')"); err != nil {
			b.Fatalf("insert: %v", err)
		}
		<-d.Events()
		notified := time.Now()
		if _, err := d.Scan(ctx); err != nil {
			b.Fatalf("scan: %v", err)
		}
		captured := time.Now()
		notifyToScan += captured.Sub(notified)
		commitToScan += captured.Sub(start)
	}
	b.ReportMetric(float64(notifyToScan.Nanoseconds())/float64(b.N), "ns/notify-to-scan")
	b.ReportMetric(float64(commitToScan.Nanoseconds())/float64(b.N), "ns/commit-to-scan")
}

// BenchmarkCheckpoint measures the cost of a checkpoint that writes a whole
// WAL back into the database file.
func BenchmarkCheckpoint(b *testing.B) {
	d := benchDriver(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('x')"); err != nil {
			b.Fatalf("insert: %v", err)
		}
		batch, err := d.Scan(ctx)
		if err != nil {
			b.Fatalf("scan: %v", err)
		}
		if len(batch.Changes) > 0 {
			if err := d.CaptureAck(ctx, batch.To); err != nil {
				b.Fatalf("ack: %v", err)
			}
		}
		if _, err := d.Checkpoint(ctx); err != nil {
			b.Fatalf("checkpoint: %v", err)
		}
	}
}

// BenchmarkSnapshot measures the cost of a snapshot.
func BenchmarkSnapshot(b *testing.B) {
	d := benchDriver(b)
	ctx := context.Background()
	dir := b.TempDir()
	for range 100 {
		if _, err := d.DB().Exec("INSERT INTO t(v) VALUES('x')"); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
	if _, err := d.Scan(ctx); err != nil {
		b.Fatalf("scan: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, fmt.Sprintf("snapshot-%d.db", i))
		if _, err := d.Snapshot(ctx, path); err != nil {
			b.Fatalf("snapshot: %v", err)
		}
		os.Remove(path)
	}
}
