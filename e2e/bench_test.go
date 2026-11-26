//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/backup"
	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/source"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// benchConfig is one of the three configurations the benchmarks compare.
type benchConfig struct {
	// name is the name of the configuration.
	name string
	// relay reports that the source runtime runs with the primary.
	relay bool
	// backup reports that a backup applies the increments of the chain.
	backup bool
}

// benchConfigs are the three configurations the benchmarks compare.
//
// The primary database alone is the reference: the difference of the times is
// the cost of the relay.
var benchConfigs = []benchConfig{
	{name: "no-relay"},
	{name: "relay-without-backup", relay: true},
	{name: "relay-with-backup", relay: true, backup: true},
}

// benchPrimary is the primary side of a relay in a benchmark.
//
// The business writes into the database with the connection of the driver.
type benchPrimary struct {
	// db is the connection of the business.
	db *sql.DB
	// drv is the driver of the primary database.
	drv *driver.Driver
	// dbPath is the path of the primary database.
	dbPath string
}

// newBenchPrimary opens the primary database of a benchmark.
//
// The relay is the source runtime and the backup that applies the increments
// of the chain. The driver belongs to the business: the benchmark writes with
// its connection.
func newBenchPrimary(b *testing.B, cfg benchConfig) *benchPrimary {
	b.Helper()
	dir := b.TempDir()
	tokenFile := filepath.Join(dir, tokenName)
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		b.Fatalf("write the token: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	dbPath := filepath.Join(dir, databaseID)
	drv, err := driver.Open(ctx, driver.Config{
		DSN:          dbPath + "?_journal_mode=WAL",
		ScanInterval: 20 * time.Millisecond,
	})
	if err != nil {
		b.Fatalf("open the driver: %v", err)
	}
	b.Cleanup(func() { _ = drv.Close() })
	if _, err := drv.DB().Exec(createAccounts); err != nil {
		b.Fatalf("create the table: %v", err)
	}

	p := &benchPrimary{db: drv.DB(), drv: drv, dbPath: dbPath}
	if !cfg.relay {
		return p
	}
	peer, err := source.Open(ctx, source.Config{
		Driver: drv,
		Spool: spool.Config{
			Root: filepath.Join(dir, "spool"),
			Mode: spool.ModeFull,
		},
		Protocol: protocol.Config{
			TokenFile:  tokenFile,
			DatabaseID: databaseID,
			StreamID:   streamID,
		},
		SnapshotDir:  filepath.Join(dir, "snapshots"),
		ScanInterval: 20 * time.Millisecond,
		GCInterval:   20 * time.Millisecond,
	})
	if err != nil {
		b.Fatalf("open the source: %v", err)
	}
	b.Cleanup(func() { _ = peer.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	b.Cleanup(func() { _ = ln.Close() })
	go func() { _ = peer.Run(ctx, ln) }()

	if cfg.backup {
		bpeer, err := backup.Open(ctx, backup.Config{
			Root: filepath.Join(dir, "backups"),
			Protocol: protocol.Config{
				TokenFile:  tokenFile,
				DatabaseID: databaseID,
				StreamID:   streamID,
				BackupID:   backupID,
			},
		})
		if err != nil {
			b.Fatalf("open the backup: %v", err)
		}
		go func() { _ = bpeer.Run(ctx, ln.Addr().String()) }()
	}
	return p
}

// walPath returns the path of the write-ahead log of the primary database.
func (p *benchPrimary) walPath() string {
	return p.dbPath + "-wal"
}

// BenchmarkTheRelayUnderLoad measures the cost of the relay on the commit
// path of the business.
//
// The three configurations are the primary database alone, the primary
// database with the source runtime and the primary database with the source
// runtime and a backup that applies the increments. Every configuration
// commits the same transactions: the difference of the times is the cost of
// the relay.
func BenchmarkTheRelayUnderLoad(b *testing.B) {
	for _, cfg := range benchConfigs {
		b.Run(cfg.name, func(b *testing.B) {
			p := newBenchPrimary(b, cfg)
			db := p.db
			b.ResetTimer()
			for i := range b.N {
				if _, err := db.Exec("INSERT INTO accounts(name, balance) VALUES(?, ?)",
					fmt.Sprintf("account-%d", i), i*100); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
			b.StopTimer()

			// The write-ahead log is not truncated: its size is the high
			// water mark of the benchmark.
			var walBytes int64
			if info, err := os.Stat(p.walPath()); err == nil {
				walBytes = info.Size()
			}
			b.ReportMetric(float64(walBytes), "wal-bytes")
			b.ReportMetric(float64(p.drv.Stats().WALFrames), "wal-frames")
		})
	}
}

// BenchmarkTheCommitLatencyOfTheRelay measures the latency of the commits of
// the business in the three configurations.
//
// The framework reports the mean of the commits. The percentiles are
// collected in the benchmark: the samples are taken while the timer runs and
// they are sorted after it stopped.
func BenchmarkTheCommitLatencyOfTheRelay(b *testing.B) {
	for _, cfg := range benchConfigs {
		b.Run(cfg.name, func(b *testing.B) {
			p := newBenchPrimary(b, cfg)
			db := p.db
			samples := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := range b.N {
				start := time.Now()
				if _, err := db.Exec("INSERT INTO accounts(name, balance) VALUES(?, ?)",
					fmt.Sprintf("account-%d", i), i*100); err != nil {
					b.Fatalf("insert: %v", err)
				}
				samples = append(samples, time.Since(start))
			}
			b.StopTimer()
			reportLatency(b, samples)
		})
	}
}

// reportLatency reports the percentiles of the samples of a latency.
//
// The samples are sorted: the benchmark reports the median, the ninety-fifth
// and the ninety-ninth percentile in nanoseconds.
func reportLatency(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	slices.Sort(samples)
	b.ReportMetric(float64(percentile(samples, 50)), "p50-ns")
	b.ReportMetric(float64(percentile(samples, 95)), "p95-ns")
	b.ReportMetric(float64(percentile(samples, 99)), "p99-ns")
}

// percentile returns the sample at the given percentile of the samples.
//
// The samples have to be sorted.
func percentile(samples []time.Duration, p int) time.Duration {
	index := (len(samples)*p + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(samples) {
		index = len(samples)
	}
	return samples[index-1]
}

// benchRows is the number of the rows the statements that change a row work
// on.
//
// The rows are written before the benchmark runs.
const benchRows = 1000

// benchBulkRows is the number of the rows a big transaction writes.
//
// A big transaction holds many rows: it is the load of the business that
// writes a lot at once.
const benchBulkRows = 100

// benchStatement is one statement of the business.
type benchStatement struct {
	// name is the name of the statement.
	name string
	// rows reports that the statement works on rows that have to be
	// written before the benchmark runs.
	rows bool
	// run writes one transaction of the statement.
	run func(db *sql.DB, i int) error
}

// benchStatements are the statements the benchmarks compare.
var benchStatements = []benchStatement{
	{
		name: "insert",
		run: func(db *sql.DB, i int) error {
			_, err := db.Exec("INSERT INTO accounts(name, balance) VALUES(?, ?)",
				fmt.Sprintf("account-%d", i), i*100)
			return err
		},
	},
	{
		name: "update",
		rows: true,
		run: func(db *sql.DB, i int) error {
			_, err := db.Exec("UPDATE accounts SET balance = balance + 1 WHERE id = ?", i%benchRows+1)
			return err
		},
	},
	{
		name: "delete",
		rows: true,
		run: func(db *sql.DB, i int) error {
			// The row is written back in the same transaction: the
			// workload of the statement stays the same for every
			// iteration.
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			id := int64(i%benchRows + 1)
			if _, err := tx.Exec("DELETE FROM accounts WHERE id = ?", id); err != nil {
				_ = tx.Rollback()
				return err
			}
			if _, err := tx.Exec("INSERT INTO accounts(id, name, balance) VALUES(?, ?, ?)",
				id, fmt.Sprintf("account-%d", id), id*100); err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		},
	},
	{
		name: "bulk",
		run: func(db *sql.DB, i int) error {
			// A big transaction: it writes many rows at once.
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			for j := range benchBulkRows {
				if _, err := tx.Exec("INSERT INTO accounts(name, balance) VALUES(?, ?)",
					fmt.Sprintf("bulk-%d-%d", i, j), j*100); err != nil {
					_ = tx.Rollback()
					return err
				}
			}
			return tx.Commit()
		},
	},
}

// prepare writes the rows the statement works on.
func (s benchStatement) prepare(b *testing.B, db *sql.DB) {
	b.Helper()
	if !s.rows {
		return
	}
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("open the transaction: %v", err)
	}
	for i := 1; i <= benchRows; i++ {
		if _, err := tx.Exec("INSERT INTO accounts(id, name, balance) VALUES(?, ?, ?)",
			i, fmt.Sprintf("account-%d", i), i*100); err != nil {
			_ = tx.Rollback()
			b.Fatalf("write the row %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("commit the rows: %v", err)
	}
}

// BenchmarkTheStatementThroughputOfTheRelay measures the throughput of the
// statements of the business in the three configurations.
//
// Every iteration is one transaction of one statement. The benchmark reports
// the number of the transactions of the statement per second.
func BenchmarkTheStatementThroughputOfTheRelay(b *testing.B) {
	for _, stmt := range benchStatements {
		for _, cfg := range benchConfigs {
			b.Run(stmt.name+"/"+cfg.name, func(b *testing.B) {
				p := newBenchPrimary(b, cfg)
				stmt.prepare(b, p.db)
				b.ResetTimer()
				for i := range b.N {
					if err := stmt.run(p.db, i); err != nil {
						b.Fatalf("the %s: %v", stmt.name, err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tx/s")
			})
		}
	}
}
