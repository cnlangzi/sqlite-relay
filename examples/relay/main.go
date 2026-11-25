// Command relay shows how an application uses SQLiteRelay.
//
// The program opens a primary database, attaches the source runtime to it,
// connects a backup to the runtime, writes rows, waits for the backup to
// catch up, pauses the backup, reads the database of the backup while it is
// paused and resumes the backup.
//
// Run it with:
//
//	go run ./examples/relay
//
// The work directory is a temporary directory unless the flag -dir names one.
// A work directory that the caller names is kept, a temporary one is removed
// when the program ends. A work directory that the caller names has to be
// empty: the program writes the database of the relay into it and it cannot
// run twice in the same directory.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/cnlangzi/sqlite-relay/backup"
	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/source"
	"github.com/cnlangzi/sqlite-relay/spool"

	_ "github.com/mattn/go-sqlite3"
)

// The identity of the relay of the example.
const (
	// databaseID is the name of the database of the relay.
	databaseID = "app.db"
	// streamID is the number of the stream of the relay.
	streamID = 1
	// backupID is the name of the backup of the relay.
	backupID = "backup-1"
	// token is the shared token of the two sides of the relay.
	token = "the shared token of the example"
)

// schema is the schema of the primary database.
const schema = "CREATE TABLE accounts(id INTEGER PRIMARY KEY, name TEXT NOT NULL, balance INTEGER NOT NULL)"

// catchUpTimeout is the time the example waits for the backup.
const catchUpTimeout = 30 * time.Second

// The transactions of the chain and the rows of the business.
//
// The schema of the primary database is one transaction of the chain and
// every row the business writes is a transaction of its own. The sequence
// numbers of the chain count them.
const (
	// schemaSeq is the sequence number of the transaction of the schema.
	schemaSeq = 1
	// firstRows is the number of the rows the business writes before the
	// backup is paused.
	firstRows = 100
	// secondRows is the number of the rows the business writes while the
	// backup is paused.
	secondRows = 50
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "relay: %v\n", err)
		os.Exit(1)
	}
}

// run runs the whole life of a relay.
func run() error {
	dir := flag.String("dir", "", "the work directory of the relay, a temporary directory by default, an empty one")
	addr := flag.String("addr", "127.0.0.1:0", "the address the primary listens on")
	flag.Parse()

	root, cleanup, err := workDir(*dir)
	if err != nil {
		return err
	}
	defer cleanup()
	fmt.Printf("the work directory is %s\n", root)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tokenFile := filepath.Join(root, "token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		return fmt.Errorf("write the token: %w", err)
	}

	// The business opens the driver and it writes with the connection of
	// the driver. The driver owns every connection to the primary database
	// inside the process.
	drv, err := driver.Open(ctx, driver.Config{
		DSN: filepath.Join(root, databaseID) + "?_journal_mode=WAL",
	})
	if err != nil {
		return fmt.Errorf("open the driver: %w", err)
	}
	defer func() { _ = drv.Close() }()
	if _, err := drv.DB().Exec(schema); err != nil {
		return fmt.Errorf("create the schema: %w", err)
	}

	// The relay: the source runtime captures the increments of the primary
	// database, it keeps them in its spool and it pushes them to the backup.
	peer, err := source.Open(ctx, source.Config{
		Driver: drv,
		Spool: spool.Config{
			Root: filepath.Join(root, "spool"),
			Mode: spool.ModeFull,
		},
		Protocol: protocol.Config{
			TokenFile:  tokenFile,
			DatabaseID: databaseID,
			StreamID:   streamID,
		},
		SnapshotDir: filepath.Join(root, "snapshots"),
	})
	if err != nil {
		return fmt.Errorf("open the source: %w", err)
	}
	defer func() { _ = peer.Close() }()
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}
	defer func() { _ = ln.Close() }()

	// The errors of the two sides of the relay: a side that ends tells why
	// it ended.
	runErr := make(chan error, 2)
	go func() { runErr <- peer.Run(ctx, ln) }()

	// The backup side of the relay. It has no baseline when it connects:
	// it asks the source for a snapshot of the primary database.
	bpeer, err := backup.Open(ctx, backup.Config{
		Root: filepath.Join(root, "backups"),
		Protocol: protocol.Config{
			TokenFile:  tokenFile,
			DatabaseID: databaseID,
			StreamID:   streamID,
			BackupID:   backupID,
		},
	})
	if err != nil {
		return fmt.Errorf("open the backup: %w", err)
	}
	go func() { runErr <- bpeer.Run(ctx, ln.Addr().String()) }()

	// The business writes. The relay does not hold the transaction up: it
	// captures the increment in the background.
	if err := insert(drv.DB(), firstRows, 0); err != nil {
		return err
	}
	if err := waitForCatchUp(peer, bpeer, schemaSeq+firstRows, runErr); err != nil {
		return err
	}
	fmt.Println("the backup caught up with the primary")

	// The backup is paused: the file of the backup database belongs to the
	// business until the backup is resumed.
	if err := bpeer.Pause(ctx); err != nil {
		return fmt.Errorf("pause the backup: %w", err)
	}
	primaryRows, err := countRows(drv.DB())
	if err != nil {
		return err
	}
	backupRows, err := countBackupRows(root)
	if err != nil {
		return err
	}
	fmt.Printf("the primary holds %d rows, the backup holds %d rows\n", primaryRows, backupRows)

	// The business writes while the backup is paused. The backup does not
	// touch its database in that time.
	if err := insert(drv.DB(), secondRows, firstRows); err != nil {
		return err
	}
	backupRows, err = countBackupRows(root)
	if err != nil {
		return err
	}
	fmt.Printf("the backup holds %d rows while it is paused\n", backupRows)

	// The backup is resumed: it applies the increments of the chain that
	// were written while it was paused.
	if err := bpeer.Resume(ctx); err != nil && !errors.Is(err, backup.ErrSnapshotRequired) {
		return fmt.Errorf("resume the backup: %w", err)
	}
	if err := waitForCatchUp(peer, bpeer, schemaSeq+firstRows+secondRows, runErr); err != nil {
		return err
	}
	fmt.Println("the backup caught up with the primary again")

	// The relay writes the database of the backup itself while the backup
	// is in sync: the business reads the copy of the database while the
	// backup is paused.
	if err := bpeer.Pause(ctx); err != nil {
		return fmt.Errorf("pause the backup: %w", err)
	}
	primaryRows, err = countRows(drv.DB())
	if err != nil {
		return err
	}
	backupRows, err = countBackupRows(root)
	if err != nil {
		return err
	}
	fmt.Printf("the primary holds %d rows, the backup holds %d rows\n", primaryRows, backupRows)
	return nil
}

// workDir returns the work directory of the relay.
//
// A temporary directory is created when the caller does not name one. The
// cleanup of a temporary directory removes it, the cleanup of a directory of
// the caller keeps it.
func workDir(dir string) (string, func(), error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", nil, fmt.Errorf("create the work directory %s: %w", dir, err)
		}
		return dir, func() {}, nil
	}
	tmp, err := os.MkdirTemp("", "sqlite-relay-example-")
	if err != nil {
		return "", nil, fmt.Errorf("create the work directory: %w", err)
	}
	return tmp, func() { _ = os.RemoveAll(tmp) }, nil
}

// insert writes n rows into the accounts table.
//
// The names of the rows start at first: two batches of the business do not
// share a name.
func insert(db *sql.DB, n, first int) error {
	for i := range n {
		id := first + i
		if _, err := db.Exec("INSERT INTO accounts(name, balance) VALUES(?, ?)",
			fmt.Sprintf("account-%d", id), id*100); err != nil {
			return fmt.Errorf("write the row %d: %w", id, err)
		}
	}
	return nil
}

// countRows returns the number of the rows of the accounts table.
func countRows(db *sql.DB) (int, error) {
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM accounts").Scan(&n); err != nil {
		return 0, fmt.Errorf("count the rows of the accounts table: %w", err)
	}
	return n, nil
}

// countBackupRows returns the number of the rows of the accounts table of
// the database of the backup.
//
// The business opens the database of the backup while the backup is paused:
// the relay does not write the file in that time.
func countBackupRows(root string) (int, error) {
	path := filepath.Join(root, "backups", databaseID, "backup.db")
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		return 0, fmt.Errorf("open the database of the backup: %w", err)
	}
	defer func() { _ = db.Close() }()
	return countRows(db)
}

// waitForCatchUp waits until the backup holds every increment of the primary.
//
// The condition has to hold for a moment: the capture of the source runs
// after the commit of the business and a sample may be taken before the last
// transaction reached the spool.
//
// A side of the relay that ends ends the wait: the error it reports is the
// reason the backup does not catch up.
func waitForCatchUp(peer *source.Peer, bpeer *backup.Peer, seq uint64, runErr <-chan error) error {
	deadline := time.Now().Add(catchUpTimeout)
	var since time.Time
	for {
		select {
		case err := <-runErr:
			if err == nil {
				return errors.New("a side of the relay ended")
			}
			return fmt.Errorf("a side of the relay ended: %w", err)
		default:
		}
		captured := peer.Stats().CapturedSeq
		applied := bpeer.Stats().AppliedSeq
		if captured >= seq && applied >= captured {
			if since.IsZero() {
				since = time.Now()
			} else if time.Since(since) > 100*time.Millisecond {
				return nil
			}
		} else {
			since = time.Time{}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the backup applied %d of the %d transactions of the primary in %s",
				applied, captured, catchUpTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
