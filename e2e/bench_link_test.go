//go:build e2e

package e2e

import (
	"testing"
	"time"
)

// The loads of the benchmarks of the link.
//
// The benchmarks run a whole cycle: the numbers are constants and they do not
// scale with the machine, only the time the cycle takes does.
const (
	// linkBacklog is the number of the transactions that pile up while the
	// backup is away. The backlog is the load of the benchmarks of the
	// link: it is written in a burst.
	linkBacklog = 2000
)

// newLinkRelay starts the two sides of a relay in their own processes.
//
// The segments of the chain are kept: the benchmarks measure the segments
// that were pushed and their size.
func newLinkRelay(b *testing.B) *relay {
	b.Helper()
	r := newRelay(b)
	r.writeSettings(settings{Retention: 3600})
	r.startPrimary()
	r.createSchema()
	r.startBackup()
	r.waitCatchUp()
	return r
}

// backlog is the work the backup has to catch up with.
type backlog struct {
	// duration is the time the backup needed to apply the backlog.
	duration time.Duration
	// transactions is the number of the transactions of the backlog.
	transactions uint64
	// bytes is the number of the bytes of the segments of the backlog.
	bytes int64
	// segments is the number of the segments of the backlog.
	segments int
}

// buildBacklog writes a backlog of transactions and waits until the source
// holds them.
//
// It returns the number of the bytes of the segments of the backlog and the
// number of the segments.
func buildBacklog(b *testing.B, r *relay, before uint64) (int64, int) {
	b.Helper()
	r.insert(linkBacklog)
	r.waitForSeq(before + linkBacklog)
	return r.segmentBytesAfter(before)
}

// drainBacklog writes a backlog and measures the time the backup needs to
// apply it.
//
// The backup is paused while the primary writes: the source keeps the
// increments of the chain in its spool and it does not push them. The resume
// pushes them and the benchmark measures the time until the backup holds them
// all.
func drainBacklog(b *testing.B, r *relay) backlog {
	b.Helper()
	before := r.sourceStats().CapturedSeq
	if _, err := r.backup.do("pause"); err != nil {
		b.Fatalf("the pause: %v", err)
	}
	bytes, segments := buildBacklog(b, r, before)
	if segments == 0 {
		b.Fatal("the chain holds no segment")
	}
	start := time.Now()
	if _, err := r.backup.do("resume"); err != nil {
		b.Fatalf("the resume: %v", err)
	}
	r.waitCatchUp()
	return backlog{
		duration:     time.Since(start),
		transactions: linkBacklog,
		bytes:        bytes,
		segments:     segments,
	}
}

// BenchmarkTheTransferOfTheSegments measures the bandwidth of the segments
// the source pushes to the backup.
//
// The backup is paused while the primary writes: the increments of the chain
// pile up in the spool of the source. The resume pushes them as fast as the
// link allows. The benchmark reports the number of the bytes of the chain
// per second and the average size of a segment.
func BenchmarkTheTransferOfTheSegments(b *testing.B) {
	r := newLinkRelay(b)
	b.ResetTimer()
	res := drainBacklog(b, r)
	b.StopTimer()
	b.ReportMetric(float64(res.bytes)/res.duration.Seconds(), "transfer-B/s")
	b.ReportMetric(float64(res.bytes)/float64(res.segments), "segment-B")
	b.ReportMetric(float64(res.segments), "segments")
}

// BenchmarkTheApplyOfTheSegments measures the throughput of the applier of
// the backup.
//
// The backup applies the increments of the chain one by one. The benchmark
// reports the number of the transactions it applies per second and the time
// the whole backlog takes.
func BenchmarkTheApplyOfTheSegments(b *testing.B) {
	r := newLinkRelay(b)
	b.ResetTimer()
	res := drainBacklog(b, r)
	b.StopTimer()
	b.ReportMetric(float64(res.transactions)/res.duration.Seconds(), "apply-tx/s")
	b.ReportMetric(res.duration.Seconds(), "apply-s")
}

// BenchmarkTheCatchUpAfterADisconnect measures the recovery of a backup that
// was away.
//
// The backup is killed and the primary writes while it is away. The backup
// comes back and the benchmark measures the time it needs to hold every
// increment of the primary again and the rate of the catch-up.
func BenchmarkTheCatchUpAfterADisconnect(b *testing.B) {
	r := newLinkRelay(b)
	before := r.sourceStats().CapturedSeq
	if err := r.backup.crash(); err == nil {
		b.Fatal("the backup was killed, want the error of the wait")
	}
	bytes, _ := buildBacklog(b, r, before)
	start := time.Now()
	r.startBackup()
	r.waitCatchUp()
	duration := time.Since(start)
	b.ReportMetric(duration.Seconds(), "catchup-s")
	b.ReportMetric(float64(linkBacklog)/duration.Seconds(), "catchup-tx/s")
	b.ReportMetric(float64(bytes)/duration.Seconds(), "catchup-B/s")
}
