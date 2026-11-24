package spool

import (
	"context"
	"testing"
)

// benchSpool opens a spool for a benchmark.
func benchSpool(b *testing.B, mode Mode) *Spool {
	b.Helper()
	s, err := Open(context.Background(), Config{
		Root:            b.TempDir(),
		DatabaseID:      "app.db",
		Mode:            mode,
		SegmentMaxBytes: 1 << 30,
	})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// BenchmarkAppend measures the cost of writing a transaction into the spool
// and the bandwidth it reaches.
func BenchmarkAppend(b *testing.B) {
	s := benchSpool(b, ModeFull)
	counter := &syncCounter{}
	counter.install(s)

	ctx := context.Background()
	tx := testTx(1, 4)
	b.SetBytes(txSize(tx))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Append(ctx, tx); err != nil {
			b.Fatalf("Append: %v", err)
		}
		if i%512 == 511 {
			if err := s.Flush(ctx); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(counter.total())/float64(b.N), "fsync/op")
	b.ReportMetric(float64(s.Stats().Bytes)/float64(b.N), "disk-B/op")
}

// BenchmarkPublish measures the cost of publishing a segment in every mode.
func BenchmarkPublish(b *testing.B) {
	for _, mode := range []Mode{ModeOff, ModeNormal, ModeFull} {
		b.Run(mode.String(), func(b *testing.B) {
			s := benchSpool(b, mode)
			counter := &syncCounter{}
			counter.install(s)

			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Append(ctx, testTx(uint64(i+1), 1)); err != nil {
					b.Fatalf("Append: %v", err)
				}
				if err := s.Flush(ctx); err != nil {
					b.Fatalf("Flush: %v", err)
				}
				if mode == ModeNormal {
					if err := s.Sync(ctx); err != nil {
						b.Fatalf("Sync: %v", err)
					}
				}
				if i%500 == 499 {
					// The measurement is of the publish, the segments
					// that pile up are of no interest to it.
					b.StopTimer()
					s.removeFiles(s.segDir)
					s.segs = nil
					b.StartTimer()
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(counter.total())/float64(b.N), "fsync/op")
			b.ReportMetric(float64(s.Stats().Bytes)/float64(b.N), "disk-B/op")
		})
	}
}

// BenchmarkRecover measures the time the scan of the segments directory
// takes after a restart.
func BenchmarkRecover(b *testing.B) {
	s := benchSpool(b, ModeFull)
	ctx := context.Background()
	const segments = 100
	for i := range segments {
		if err := s.Append(ctx, testTx(uint64(i+1), 1)); err != nil {
			b.Fatalf("Append: %v", err)
		}
		if err := s.Flush(ctx); err != nil {
			b.Fatalf("Flush: %v", err)
		}
	}

	b.ResetTimer()
	for range b.N {
		result, reason, err := s.scanSegments()
		if err != nil {
			b.Fatalf("scan the segments: %v", err)
		}
		if reason != "" {
			b.Fatalf("the chain is broken: %s", reason)
		}
		if len(result.segs) != segments {
			b.Fatalf("the scan found %d segments, want %d", len(result.segs), segments)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(segments), "segments")
}
