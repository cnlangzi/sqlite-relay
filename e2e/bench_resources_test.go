//go:build e2e

package e2e

import (
	"syscall"
	"testing"
	"time"
)

// The load of the benchmark of the resources.
const (
	// resourceRate is the number of the transactions the source writes per
	// second while the backup is paused.
	resourceRate = 50
	// resourceSeconds is the number of the seconds the load of the source
	// lasts.
	resourceSeconds = 4
)

// BenchmarkTheResourcesDuringTheBackupPause measures the resources the source
// uses while the backup is paused.
//
// The backup is paused and the source keeps working: it captures the
// increments of the primary database and it writes them into its spool. The
// resources are measured over the window of the pause: the primary of the
// window is started at the beginning of it and it is stopped at the end of
// it, so that the difference of the usage of the children of the benchmark
// covers the window and nothing else.
//
// The CPU time and the disk IO come from the usage of the children of the
// process, the peak of the memory comes from the usage of the primary
// itself.
func BenchmarkTheResourcesDuringTheBackupPause(b *testing.B) {
	r := newLinkRelay(b)
	if _, err := r.backup.do("pause"); err != nil {
		b.Fatalf("the pause: %v", err)
	}

	// The primary of the window ends here: its usage is part of the usage
	// of the children from now on.
	if err := r.primary.stop(); err != nil {
		b.Fatalf("the primary did not stop cleanly: %v", err)
	}
	before := childUsage(b)

	// The primary of the window starts here.
	r.startPrimary()
	window := time.Now()

	load := time.Now()
	if _, err := r.primary.do("insert-rate %d %d", resourceRate*resourceSeconds, resourceRate); err != nil {
		b.Fatalf("the load of the source: %v", err)
	}
	loadDuration := time.Since(load)

	if err := r.primary.stop(); err != nil {
		b.Fatalf("the primary did not stop cleanly: %v", err)
	}
	after := childUsage(b)
	delta := usageDelta(before, after)
	peak := r.primary.rusage()
	windowDuration := time.Since(window)

	b.ReportMetric(float64(delta.cpuNanos)/1e6, "cpu-ms")
	b.ReportMetric(float64(peak.Maxrss), "max-rss-kb")
	b.ReportMetric(float64(delta.inBlocks), "read-blocks")
	b.ReportMetric(float64(delta.outBlocks), "write-blocks")
	b.ReportMetric(windowDuration.Seconds(), "window-s")
	b.ReportMetric(loadDuration.Seconds(), "load-s")
	b.ReportMetric(float64(resourceRate*resourceSeconds)/loadDuration.Seconds(), "tx/s")
}

// childUsage returns the resources the children of the process used.
//
// The usage of the children is a sum over the children that ended: a child
// that is still running is not part of it.
func childUsage(b *testing.B) syscall.Rusage {
	b.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &ru); err != nil {
		b.Fatalf("getrusage: %v", err)
	}
	return ru
}

// usage is the difference of two usages of the children.
type usage struct {
	// cpuNanos is the time the children spent on the CPU.
	cpuNanos int64
	// inBlocks is the number of the blocks the children read.
	inBlocks int64
	// outBlocks is the number of the blocks the children wrote.
	outBlocks int64
}

// usageDelta returns the difference of two usages of the children.
func usageDelta(before, after syscall.Rusage) usage {
	return usage{
		cpuNanos: timevalNanos(after.Utime) - timevalNanos(before.Utime) +
			timevalNanos(after.Stime) - timevalNanos(before.Stime),
		inBlocks:  after.Inblock - before.Inblock,
		outBlocks: after.Oublock - before.Oublock,
	}
}

// timevalNanos returns the time a timeval holds in nanoseconds.
func timevalNanos(t syscall.Timeval) int64 {
	return t.Sec*1e9 + t.Usec*1000
}
