//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/backup"
	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/source"
	"github.com/cnlangzi/sqlite-relay/spool"
)

// BenchmarkTheRelayUnderLoad measures the cost of the relay on the commit
// path of the business.
//
// The three configurations are the primary database alone, the primary
// database with the source runtime and the primary database with the source
// runtime and a backup that applies the increments. Every configuration
// commits the same transactions: the difference of the times is the cost of
// the relay.
func BenchmarkTheRelayUnderLoad(b *testing.B) {
	cases := []struct {
		name   string
		relay  bool
		backup bool
	}{
		{name: "no-relay"},
		{name: "relay-without-backup", relay: true},
		{name: "relay-with-backup", relay: true, backup: true},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			dir := b.TempDir()
			tokenFile := filepath.Join(dir, tokenName)
			if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
				b.Fatalf("write the token: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			dbPath := filepath.Join(dir, databaseID)
			walPath := dbPath + "-wal"
			drv, err := driver.Open(ctx, driver.Config{
				DSN:          dbPath + "?_journal_mode=WAL",
				ScanInterval: 20 * time.Millisecond,
			})
			if err != nil {
				b.Fatalf("open the driver: %v", err)
			}
			defer drv.Close()
			if _, err := drv.DB().Exec(createAccounts); err != nil {
				b.Fatalf("create the table: %v", err)
			}

			if c.relay {
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
				defer peer.Close()
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					b.Fatalf("listen: %v", err)
				}
				defer ln.Close()
				go peer.Run(ctx, ln)

				if c.backup {
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
					go bpeer.Run(ctx, ln.Addr().String())
				}
			}

			db := drv.DB()
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
			if info, err := os.Stat(walPath); err == nil {
				walBytes = info.Size()
			}
			b.ReportMetric(float64(walBytes), "wal-bytes")
			b.ReportMetric(float64(drv.Stats().WALFrames), "wal-frames")
		})
	}
}
