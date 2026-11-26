//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite-relay/backup"
	"github.com/cnlangzi/sqlite-relay/driver"
	"github.com/cnlangzi/sqlite-relay/internal/wal"
	"github.com/cnlangzi/sqlite-relay/protocol"
	"github.com/cnlangzi/sqlite-relay/source"
	"github.com/cnlangzi/sqlite-relay/spool"

	_ "github.com/mattn/go-sqlite3"
)

// The environment variables of the harness.
const (
	// envRole names the side of the relay a process runs.
	envRole = "SQLITE_RELAY_E2E_ROLE"
	// envDir is the work directory of the relay.
	envDir = "SQLITE_RELAY_E2E_DIR"
	// envListen is the address the primary listens on.
	envListen = "SQLITE_RELAY_E2E_LISTEN"
	// envRemote is the address of the primary the backup connects to.
	envRemote = "SQLITE_RELAY_E2E_REMOTE"
	// envRoot is the directory the work directories are created in.
	envRoot = "SQLITE_RELAY_E2E_ROOT"
)

// The roles of the processes.
const (
	rolePrimary = "primary"
	roleBackup  = "backup"
)

// The names of the files of a work directory.
const (
	// tokenName is the name of the file that holds the shared token.
	tokenName = "token"
	// settingsName is the name of the file that holds the settings of the
	// relay.
	settingsName = "relay.json"
)

// The identity of the relay.
const (
	// databaseID is the name of the database of the relay.
	databaseID = "primary.db"
	// streamID is the number of the stream of the relay.
	streamID = 1
	// backupID is the name of the backup of the relay.
	backupID = "backup-1"
	// token is the shared token of the relay.
	token = "the shared token of the end-to-end tests"
)

// The timeouts of the harness.
const (
	// readyTimeout is the time the parent waits for the ready file of a
	// process.
	readyTimeout = 60 * time.Second
	// commandTimeout is the time the parent waits for the answer of a
	// command.
	commandTimeout = 120 * time.Second
	// catchUpTimeout is the time the parent waits for the backup to catch
	// up with the primary.
	catchUpTimeout = 120 * time.Second
)

// The schema of the primary database.
const (
	createAccounts = "CREATE TABLE accounts(id INTEGER PRIMARY KEY, name TEXT NOT NULL, balance INTEGER NOT NULL)"
	createEntries  = "CREATE TABLE entries(id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, amount INTEGER NOT NULL, created_at INTEGER NOT NULL)"
	createIndex    = "CREATE INDEX entries_by_account ON entries(account_id)"
)

// The queries the harness compares.
const (
	queryAccounts = "SELECT id, name, balance FROM accounts ORDER BY id"
	queryEntries  = "SELECT id, account_id, amount, created_at FROM entries ORDER BY id"
	querySchema   = "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
)

// settings are the settings of a relay.
//
// The parent writes them into the work directory before it starts a
// process: the process configures its side of the relay with them.
type settings struct {
	// SpoolMode is the persistence barrier of the spool: OFF, NORMAL or
	// FULL.
	SpoolMode string `json:"spoolMode"`
	// SpoolMaxBytes is the size the spool may not grow beyond. Zero means
	// no limit.
	SpoolMaxBytes int64 `json:"spoolMaxBytes"`
	// SegmentMaxBytes is the size a segment of the spool may not grow
	// beyond. Zero means the default of the spool.
	SegmentMaxBytes int64 `json:"segmentMaxBytes"`
	// Retention is the time a segment of the spool is kept after the
	// backup acknowledged it. It is given in seconds.
	Retention int64 `json:"retention"`
	// ReclaimInterval is the interval of the reclaim of the write-ahead log
	// of the primary database. It is given in seconds. Zero means the
	// default of the driver.
	//
	// A reclaim writes the whole log back into the database file and it
	// empties the log: the frames of the log are gone afterwards. A test
	// that truncates the log needs the frames of the log.
	ReclaimInterval int64 `json:"reclaimInterval"`
	// Synchronous is the synchronous pragma of the primary database: OFF,
	// NORMAL or FULL.
	Synchronous string `json:"synchronous"`
}

// spoolMode returns the persistence barrier of the settings.
func (s settings) spoolMode() spool.Mode {
	switch strings.ToUpper(s.SpoolMode) {
	case "NORMAL":
		return spool.ModeNormal
	case "FULL":
		return spool.ModeFull
	}
	return spool.ModeOff
}

// rows is the answer of a query: the names of the columns and the rows.
type rows struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// relay is the harness of one test.
//
// The relay holds the work directory of the test and the processes that run
// the two sides of the relay in it. A process is a re-execution of the test
// binary: the role in the environment names the side it runs.
type relay struct {
	t   testing.TB
	dir string
	// listen is the address the primary listens on. The address survives
	// the restart of the primary: a backup that is connected finds it
	// again.
	listen string
	// remote is the address of the primary the backup connects to.
	remote string
	// primary is the process of the primary side.
	primary *child
	// backup is the process of the backup side.
	backup *child
}

// newRelay creates the work directory of a test.
//
// The parameter is a testing.TB: the benchmarks of the package use the same
// harness as the tests.
func newRelay(t testing.TB) *relay {
	t.Helper()
	root := os.Getenv(envRoot)
	if root == "" {
		root = os.TempDir()
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create the root directory %s: %v", root, err)
	}
	dir, err := os.MkdirTemp(root, "relay-e2e-")
	if err != nil {
		t.Fatalf("create the work directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, tokenName), []byte(token+"\n"), 0o600); err != nil {
		t.Fatalf("write the token: %v", err)
	}
	r := &relay{t: t, dir: dir}
	t.Cleanup(func() {
		r.stopAll()
		_ = os.RemoveAll(dir)
	})
	return r
}

// stopAll kills the processes that are still running.
func (r *relay) stopAll() {
	for _, c := range []*child{r.primary, r.backup} {
		if c != nil {
			c.kill()
		}
	}
}

// writeSettings writes the settings of the relay into the work directory.
func (r *relay) writeSettings(s settings) {
	r.t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		r.t.Fatalf("encode the settings: %v", err)
	}
	if err := os.WriteFile(filepath.Join(r.dir, settingsName), b, 0o644); err != nil {
		r.t.Fatalf("write the settings: %v", err)
	}
}

// readSettings reads the settings of the relay from the work directory.
func readSettings(dir string) settings {
	var s settings
	b, err := os.ReadFile(filepath.Join(dir, settingsName))
	if err != nil {
		return s
	}
	_ = json.Unmarshal(b, &s)
	return s
}

// startPrimary starts the primary side of the relay.
func (r *relay) startPrimary() *child {
	r.t.Helper()
	c := r.spawn(rolePrimary)
	addr := r.waitReady(c, rolePrimary)
	r.listen = addr
	r.remote = addr
	r.primary = c
	return c
}

// startBackup starts the backup side of the relay.
func (r *relay) startBackup() *child {
	r.t.Helper()
	if r.remote == "" {
		r.t.Fatal("the backup needs the address of the primary")
	}
	c := r.spawn(roleBackup)
	r.waitReady(c, roleBackup)
	r.backup = c
	return c
}

// spawn starts a process of the relay.
func (r *relay) spawn(role string) *child {
	r.t.Helper()
	ready := filepath.Join(r.dir, role+".addr")
	if err := os.Remove(ready); err != nil && !os.IsNotExist(err) {
		r.t.Fatalf("remove the ready file: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "-test.timeout=0")
	cmd.Env = append(os.Environ(), envRole+"="+role, envDir+"="+r.dir)
	switch role {
	case rolePrimary:
		if r.listen != "" {
			cmd.Env = append(cmd.Env, envListen+"="+r.listen)
		}
	case roleBackup:
		cmd.Env = append(cmd.Env, envRemote+"="+r.remote)
	}

	in, err := cmd.StdinPipe()
	if err != nil {
		r.t.Fatalf("the stdin of the %s: %v", role, err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		r.t.Fatalf("the stdout of the %s: %v", role, err)
	}
	c := &child{t: r.t, cmd: cmd, in: in, out: make(chan string, 16), exited: make(chan struct{})}
	cmd.Stderr = &c.stderr
	if err := cmd.Start(); err != nil {
		r.t.Fatalf("start the %s: %v", role, err)
	}
	if cmd.Process.Pid == os.Getpid() {
		r.t.Fatalf("the %s runs in the process of the test", role)
	}
	go c.wait()
	go c.read(out)
	return c
}

// waitReady waits for the ready file of a process and returns the address
// it holds.
func (r *relay) waitReady(c *child, role string) string {
	r.t.Helper()
	path := filepath.Join(r.dir, role+".addr")
	deadline := time.Now().Add(readyTimeout)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			if addr := strings.TrimSpace(string(b)); addr != "" {
				if _, _, err := net.SplitHostPort(addr); err != nil {
					r.t.Fatalf("the %s wrote the address %q, it is not an address: %v", role, addr, err)
				}
				return addr
			}
		}
		select {
		case <-c.exited:
			r.t.Fatalf("the %s ended before it was ready: %s", role, c.output())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.t.Fatalf("the %s did not become ready in %s: %s", role, readyTimeout, c.output())
	return ""
}

// primaryDBPath returns the path of the primary database.
func (r *relay) primaryDBPath() string {
	return filepath.Join(r.dir, databaseID)
}

// walPath returns the path of the write-ahead log of the primary
// database.
func (r *relay) walPath() string {
	return r.primaryDBPath() + "-wal"
}

// backupDBPath returns the path of the database of the backup.
func (r *relay) backupDBPath() string {
	return filepath.Join(r.dir, "backups", databaseID, "backup.db")
}

// segmentFiles returns the paths of the segments of the spool.
func (r *relay) segmentFiles() []string {
	r.t.Helper()
	dir := filepath.Join(r.dir, "spool", databaseID, "segments")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		r.t.Fatalf("read the segments directory: %v", err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".seg") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

// segmentBytesAfter returns the size of the segments of the chain that hold
// a transaction after seq.
//
// The name of a segment holds the sequence numbers of its first and of its
// last transaction.
func (r *relay) segmentBytesAfter(seq uint64) (int64, int) {
	r.t.Helper()
	var bytes int64
	var count int
	for _, path := range r.segmentFiles() {
		name := filepath.Base(path)
		parts := strings.Split(strings.TrimSuffix(name, ".seg"), "-")
		if len(parts) != 2 {
			r.t.Fatalf("the segment %q does not hold the sequence numbers of its transactions", name)
		}
		end, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			r.t.Fatalf("the segment %q does not hold the sequence number of its last transaction: %v", name, err)
		}
		if end <= seq {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			r.t.Fatalf("stat the segment %s: %v", path, err)
		}
		bytes += info.Size()
		count++
	}
	return bytes, count
}

// waitForSeq waits until the source holds the given sequence number.
//
// The condition has to hold for a moment: the capture of the source runs
// after the commit of the business, a sample may be taken before the last
// transaction reached the spool.
func (r *relay) waitForSeq(seq uint64) {
	r.t.Helper()
	if !waitFor(catchUpTimeout, func() bool {
		return r.sourceStats().CapturedSeq >= seq
	}) {
		r.t.Fatalf("the source did not capture the sequence number %d, it holds %d", seq, r.sourceStats().CapturedSeq)
	}
}

// truncateWAL cuts the tail of the write-ahead log of the primary database.
//
// The frames of the log are lost: the primary database lost the transactions
// that were committed at the end of the log.
func truncateWAL(t *testing.T, path string) error {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	size := int64(wal.HeaderSize + 16)
	if info.Size() < size {
		size = info.Size()
	}
	return os.Truncate(path, size)
}

// do sends a command to a process and returns the payload of the answer.
//
// A command that failed is an error: the message of the failure holds the
// output of the process.
func (c *child) do(format string, args ...any) (string, error) {
	c.t.Helper()
	line := fmt.Sprintf(format, args...)
	if _, err := c.in.Write([]byte(line + "\n")); err != nil {
		return "", fmt.Errorf("write %q: %w", line, err)
	}
	var answer string
	select {
	case a, ok := <-c.out:
		if !ok {
			return "", fmt.Errorf("the process ended: %s", c.output())
		}
		answer = a
	case <-time.After(commandTimeout):
		return "", fmt.Errorf("the process did not answer %q in %s", line, commandTimeout)
	}
	switch {
	case answer == "ok":
		return "", nil
	case strings.HasPrefix(answer, "ok "):
		return strings.TrimPrefix(answer, "ok "), nil
	case strings.HasPrefix(answer, "err "):
		return "", fmt.Errorf("%s: %s", strings.TrimPrefix(answer, "err "), c.output())
	}
	return "", fmt.Errorf("the answer %q of %q is not known", answer, line)
}

// child is a process of the relay.
type child struct {
	t   testing.TB
	cmd *exec.Cmd
	in  io.WriteCloser
	// out holds the answers of the process.
	out chan string
	// exited is closed when the process ended.
	exited chan struct{}
	// stderr holds the output of the process.
	stderr safeBuffer

	mu sync.Mutex
	// waitErr is the error of the wait of the process.
	waitErr error
}

// safeBuffer collects the output of a process.
//
// The process writes into the buffer while the parent reads it.
type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

// Write appends to the buffer.
func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

// String returns the content of the buffer.
func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// read reads the answers of the process.
func (c *child) read(r io.Reader) {
	defer close(c.out)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for sc.Scan() {
		select {
		case c.out <- sc.Text():
		case <-c.exited:
			return
		}
	}
}

// wait waits for the process to end.
func (c *child) wait() {
	err := c.cmd.Wait()
	c.mu.Lock()
	c.waitErr = err
	c.mu.Unlock()
	close(c.exited)
}

// stop stops the process with SIGTERM and waits for it to end.
//
// The process stops writing, closes the peer and closes the driver before
// it ends: it exits with the code zero.
func (c *child) stop() error {
	c.t.Helper()
	if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal the process: %w", err)
	}
	return c.waitFor()
}

// crash kills the process with SIGKILL and waits for it to end.
//
// The process dies without cleaning up: the write-ahead log of the primary
// database survives.
func (c *child) crash() error {
	c.t.Helper()
	if err := c.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		return fmt.Errorf("signal the process: %w", err)
	}
	return c.waitFor()
}

// kill ends the process if it is still running.
func (c *child) kill() {
	select {
	case <-c.exited:
		return
	default:
	}
	_ = c.cmd.Process.Signal(syscall.SIGKILL)
	<-c.exited
}

// waitFor waits for the process to end and returns the error of the wait.
func (c *child) waitFor() error {
	<-c.exited
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waitErr
}

// exitCode returns the code the process exited with.
//
// A process that was killed by a signal has the code -1.
func (c *child) exitCode() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd.ProcessState == nil {
		return -1
	}
	return c.cmd.ProcessState.ExitCode()
}

// rusage returns the resources the process used.
//
// The call is only valid after the process ended: the usage of a process is
// known when it was waited for.
func (c *child) rusage() syscall.Rusage {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd.ProcessState == nil {
		c.t.Fatal("the process did not end yet")
	}
	usage, ok := c.cmd.ProcessState.SysUsage().(*syscall.Rusage)
	if !ok {
		c.t.Fatalf("the usage of the process is %T, want a rusage", c.cmd.ProcessState.SysUsage())
	}
	return *usage
}

// output returns the output of the process.
func (c *child) output() string {
	return strings.TrimSpace(c.stderr.String())
}

// TestHelperProcess runs the side of the relay the environment names.
//
// The parent re-executes the test binary with the role in the environment.
// The test does nothing when the environment names no role: the binary is
// the parent then.
func TestHelperProcess(t *testing.T) {
	switch os.Getenv(envRole) {
	case rolePrimary:
		runPrimary()
	case roleBackup:
		runBackup()
	}
}

// fatal ends the process with an error.
//
// The parent reads the message of the failure from the standard error of
// the process.
func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// runPrimary runs the primary side of the relay.
//
// The process serves the backup, it listens on the address of the work
// directory and it answers the commands of the parent.
func runPrimary() {
	dir := os.Getenv(envDir)
	set := readSettings(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := filepath.Join(dir, databaseID) + "?_journal_mode=WAL"
	if set.Synchronous != "" {
		dsn += "&_synchronous=" + set.Synchronous
	}
	reclaimInterval := time.Duration(set.ReclaimInterval) * time.Second
	if reclaimInterval <= 0 {
		reclaimInterval = 20 * time.Millisecond
	}
	drv, err := driver.Open(ctx, driver.Config{
		DSN:             dsn,
		ScanInterval:    20 * time.Millisecond,
		ReclaimBytes:    1 << 20,
		ReclaimInterval: reclaimInterval,
		ReclaimTimeout:  10 * time.Second,
	})
	if err != nil {
		fatal("open the driver: %v", err)
	}
	peer, err := source.Open(ctx, source.Config{
		Driver: drv,
		Spool: spool.Config{
			Root:            filepath.Join(dir, "spool"),
			Mode:            set.spoolMode(),
			SyncInterval:    20 * time.Millisecond,
			MaxBytes:        set.SpoolMaxBytes,
			SegmentMaxBytes: set.SegmentMaxBytes,
			Retention:       time.Duration(set.Retention) * time.Second,
		},
		Protocol: protocol.Config{
			TokenFile:  filepath.Join(dir, tokenName),
			DatabaseID: databaseID,
			StreamID:   streamID,
		},
		SnapshotDir:  filepath.Join(dir, "snapshots"),
		ScanInterval: 20 * time.Millisecond,
		GCInterval:   20 * time.Millisecond,
	})
	if err != nil {
		fatal("open the source: %v", err)
	}

	listen := os.Getenv(envListen)
	if listen == "" {
		listen = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		fatal("listen on %s: %v", listen, err)
	}
	go func() {
		if err := peer.Run(ctx, ln); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "the source ended: %v\n", err)
		}
	}()
	writeReady(filepath.Join(dir, rolePrimary+".addr"), ln.Addr().String())

	p := &primary{drv: drv, peer: peer}
	p.serve()
}

// primary is the primary side of the relay inside a process.
type primary struct {
	drv  *driver.Driver
	peer *source.Peer
}

// serve answers the commands of the parent until the process is stopped.
//
// The order of the shutdown matters: the process stops writing, it closes
// the peer and it closes the driver. The driver is closed last: the write
// ahead log of the primary database is deleted when the last connection to
// it closes.
func (p *primary) serve() {
	lines := make(chan string)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case <-sig:
			p.close()
			return
		case line, ok := <-lines:
			if !ok {
				p.close()
				return
			}
			if line == "stop" {
				p.close()
				return
			}
			_, _ = fmt.Fprintln(os.Stdout, p.handle(line))
		}
	}
}

// close ends the primary side of the relay.
//
// The peer is closed before the driver: the peer writes the transactions
// that are still pending into the spool and it marks the chain of the
// segments as complete.
func (p *primary) close() {
	if err := p.peer.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close the source: %v\n", err)
	}
	if err := p.drv.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close the driver: %v\n", err)
	}
	os.Exit(0)
}

// handle runs a command of the parent and returns the answer.
func (p *primary) handle(line string) string {
	fields := strings.SplitN(line, " ", 2)
	switch fields[0] {
	case "insert":
		n, err := count(fields)
		if err != nil {
			return fmt.Sprintf("err %v", err)
		}
		if err := p.insert(n, 1); err != nil {
			return fmt.Sprintf("err %v", err)
		}
	case "bulk":
		n, err := count(fields)
		if err != nil {
			return fmt.Sprintf("err %v", err)
		}
		if err := p.insert(n, n); err != nil {
			return fmt.Sprintf("err %v", err)
		}
	case "insert-rate":
		nums, err := numbers(fields)
		if err != nil {
			return fmt.Sprintf("err %v", err)
		}
		if len(nums) != 2 {
			return "err the command needs a number of rows and a rate"
		}
		if err := p.insertAtRate(nums[0], nums[1]); err != nil {
			return fmt.Sprintf("err %v", err)
		}
	case "entries":
		n, err := count(fields)
		if err != nil {
			return fmt.Sprintf("err %v", err)
		}
		if err := p.insertEntries(n); err != nil {
			return fmt.Sprintf("err %v", err)
		}
	case "ddl":
		stmt, ok := argument(fields)
		if !ok {
			return "err the command needs a statement"
		}
		if _, err := p.drv.DB().Exec(stmt); err != nil {
			return fmt.Sprintf("err %v", err)
		}
	case "delete":
		if _, err := p.drv.DB().Exec("DELETE FROM accounts"); err != nil {
			return fmt.Sprintf("err %v", err)
		}
	case "vacuum":
		// The driver reads the log while the database is vacuumed: the
		// vacuum waits for the readers of the database.
		var err error
		for range 20 {
			if _, err = p.drv.DB().Exec("VACUUM"); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			return fmt.Sprintf("err %v", err)
		}
	case "query":
		query, ok := argument(fields)
		if !ok {
			return "err the command needs a query"
		}
		out, err := p.query(query)
		if err != nil {
			return fmt.Sprintf("err %v", err)
		}
		return "ok " + out
	case "stats":
		out, err := json.Marshal(p.peer.Stats())
		if err != nil {
			return fmt.Sprintf("err %v", err)
		}
		return "ok " + string(out)
	default:
		return "err the command is not known"
	}
	return "ok"
}

// argument returns the argument of a command.
func argument(fields []string) (string, bool) {
	if len(fields) < 2 || fields[1] == "" {
		return "", false
	}
	return fields[1], true
}

// count returns the number of rows a command names.
func count(fields []string) (int, error) {
	arg, ok := argument(fields)
	if !ok {
		return 0, errors.New("the command needs a number of rows")
	}
	return strconv.Atoi(arg)
}

// numbers returns the numbers a command names.
func numbers(fields []string) ([]int, error) {
	arg, ok := argument(fields)
	if !ok {
		return nil, errors.New("the command needs numbers")
	}
	parts := strings.Fields(arg)
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", p)
		}
		out = append(out, n)
	}
	return out, nil
}

// insert writes n rows into the accounts table.
//
// The rows are written in the given number of transactions: one transaction
// per row for a single transaction, one transaction for all of them
// otherwise. The ids of the rows follow the rows that are already there, so
// that a primary that was restarted does not write an id twice.
//
// The ids are read before the transaction is opened. A transaction that
// reads first holds a snapshot of the log, and a snapshot may not be
// written any more when the log was reset in the meantime.
func (p *primary) insert(n, perTx int) error {
	if perTx < 1 {
		perTx = 1
	}
	db := p.drv.DB()
	written := 0
	for written < n {
		count := perTx
		if count > n-written {
			count = n - written
		}
		if perTx == 1 {
			if err := p.insertOne(); err != nil {
				return err
			}
			written++
			continue
		}
		first, err := nextAccountID(db)
		if err != nil {
			return err
		}
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		for i := range count {
			id := first + int64(i)
			if _, err := tx.Exec("INSERT INTO accounts(id, name, balance) VALUES(?, ?, ?)",
				id, accountName(id), id*100); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		written += count
	}
	return nil
}

// insertAtRate writes n rows into the accounts table at a fixed rate.
//
// Every row is written in a transaction of its own and every row waits for
// the time its turn comes: the load has the rate it asks for and not the rate
// the disk reaches. A row that takes longer than its turn delays the rows
// that follow it.
func (p *primary) insertAtRate(n, rate int) error {
	if rate <= 0 {
		rate = 1
	}
	interval := time.Second / time.Duration(rate)
	start := time.Now()
	for i := range n {
		if i > 0 {
			time.Sleep(time.Until(start.Add(time.Duration(i) * interval)))
		}
		if err := p.insertOne(); err != nil {
			return err
		}
	}
	return nil
}

// insertOne writes a single row into the accounts table.
//
// The id of the row follows the rows that are already there, so that a
// primary that was restarted does not write an id twice. The id is read
// before the transaction is opened: a transaction that reads first holds a
// snapshot of the log, and a snapshot may not be written any more when the
// log was reset in the meantime.
func (p *primary) insertOne() error {
	db := p.drv.DB()
	first, err := nextAccountID(db)
	if err != nil {
		return err
	}
	if _, err := db.Exec("INSERT INTO accounts(id, name, balance) VALUES(?, ?, ?)",
		first, accountName(first), first*100); err != nil {
		return err
	}
	return nil
}

// insertEntries writes n rows into the entries table in one transaction.
func (p *primary) insertEntries(n int) error {
	db := p.drv.DB()
	first, err := nextEntryID(db)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for i := range n {
		id := first + int64(i)
		_, err = tx.Exec("INSERT INTO entries(id, account_id, amount, created_at) VALUES(?, ?, ?, ?)",
			id, id%100+1, id*7, 1700000000+id)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// nextAccountID returns the id the next row of the accounts table gets.
func nextAccountID(db *sql.DB) (int64, error) {
	var id int64
	if err := db.QueryRow("SELECT COALESCE(MAX(id), 0) + 1 FROM accounts").Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// nextEntryID returns the id the next row of the entries table gets.
func nextEntryID(db *sql.DB) (int64, error) {
	var id int64
	if err := db.QueryRow("SELECT COALESCE(MAX(id), 0) + 1 FROM entries").Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// accountName returns the name of an account.
func accountName(id int64) string {
	return fmt.Sprintf("account-%d", id)
}

// query runs a query on the primary database and returns the rows.
func (p *primary) query(query string) (string, error) {
	rows, err := readRows(p.drv.DB(), query)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// readRows runs a query and reads its rows.
//
// The rows are encoded and decoded again: the values of the two databases
// have the same types then, the rows of the primary come from the process
// of the primary.
func readRows(db *sql.DB, query string) (rows, error) {
	res, err := db.Query(query)
	if err != nil {
		return rows{}, err
	}
	defer func() { _ = res.Close() }()
	columns, err := res.Columns()
	if err != nil {
		return rows{}, err
	}
	out := rows{Columns: columns}
	for res.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := res.Scan(pointers...); err != nil {
			return rows{}, err
		}
		out.Rows = append(out.Rows, values)
	}
	if err := res.Err(); err != nil {
		return rows{}, err
	}
	return out.normalize()
}

// normalize encodes and decodes the rows again.
//
// The values of a row have the same types after the round trip: the rows of
// the primary come from the process of the primary and the rows of the
// backup come from the process of the test.
func (r rows) normalize() (rows, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return rows{}, err
	}
	var out rows
	if err := json.Unmarshal(b, &out); err != nil {
		return rows{}, err
	}
	return out, nil
}

// writeReady writes the file that tells the parent that the process is
// ready.
//
// The file is written to a temporary file and renamed over the target: the
// parent never reads half of it.
func writeReady(path, addr string) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(addr+"\n"), 0o644); err != nil {
		fatal("write the ready file: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		fatal("write the ready file: %v", err)
	}
}

// runBackup runs the backup side of the relay.
//
// The process connects to the primary, it writes the file that tells the
// parent that it is ready and it answers the commands of the parent.
func runBackup() {
	dir := os.Getenv(envDir)
	remote := os.Getenv(envRemote)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer, err := backup.Open(ctx, backup.Config{
		Root: filepath.Join(dir, "backups"),
		Protocol: protocol.Config{
			TokenFile:  filepath.Join(dir, tokenName),
			DatabaseID: databaseID,
			StreamID:   streamID,
			BackupID:   backupID,
		},
	})
	if err != nil {
		fatal("open the backup: %v", err)
	}
	// The connection ends when the primary is stopped. The backup opens it
	// again: a relay that is restarted finds its peer.
	go func() {
		for ctx.Err() == nil {
			if err := peer.Run(ctx, remote); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "the backup ended the session: %v\n", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	writeReady(filepath.Join(dir, roleBackup+".addr"), remote)

	b := &backupSide{peer: peer, ctx: ctx, cancel: cancel}
	b.serve()
}

// backupSide is the backup side of the relay inside a process.
type backupSide struct {
	peer   *backup.Peer
	ctx    context.Context
	cancel context.CancelFunc
}

// serve answers the commands of the parent until the process is stopped.
func (b *backupSide) serve() {
	lines := make(chan string)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case <-sig:
			b.close()
			return
		case line, ok := <-lines:
			if !ok {
				b.close()
				return
			}
			if line == "stop" {
				b.close()
				return
			}
			_, _ = fmt.Fprintln(os.Stdout, b.handle(line))
		}
	}
}

// close ends the backup side of the relay.
func (b *backupSide) close() {
	b.cancel()
	os.Exit(0)
}

// handle runs a command of the parent and returns the answer.
func (b *backupSide) handle(line string) string {
	switch line {
	case "pause":
		if err := b.peer.Pause(b.ctx); err != nil {
			return fmt.Sprintf("err %v", err)
		}
	case "resume":
		if err := b.peer.Resume(b.ctx); err != nil && !errors.Is(err, backup.ErrSnapshotRequired) {
			return fmt.Sprintf("err %v", err)
		}
	case "stats":
		out, err := json.Marshal(b.peer.Stats())
		if err != nil {
			return fmt.Sprintf("err %v", err)
		}
		return "ok " + string(out)
	default:
		return "err the command is not known"
	}
	return "ok"
}

// stats returns the statistics of the source.
func (r *relay) sourceStats() source.Stats {
	r.t.Helper()
	out, err := r.primary.do("stats")
	if err != nil {
		r.t.Fatalf("the statistics of the source: %v", err)
	}
	var st source.Stats
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		r.t.Fatalf("decode the statistics of the source: %v", err)
	}
	return st
}

// stats returns the statistics of the backup.
func (r *relay) backupStats() backup.Stats {
	r.t.Helper()
	out, err := r.backup.do("stats")
	if err != nil {
		r.t.Fatalf("the statistics of the backup: %v", err)
	}
	var st backup.Stats
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		r.t.Fatalf("decode the statistics of the backup: %v", err)
	}
	return st
}

// backfillChecker checks that the driver does not write a frame of the log
// back before the capture acknowledged it.
//
// The backfill of the checkpoint belongs to the generation of the log the
// checkpoint ran in. SQLite restarts the log when it writes it back
// completely: the frames of the generation before the restart say nothing
// about the frames of the new one.
type backfillChecker struct {
	// generation is the generation of the log the last sample saw.
	generation string
	// restarted is the time the last change of the generation was seen.
	restarted time.Time
}

// check returns an error when the driver wrote a frame back that the
// capture did not acknowledge.
func (c *backfillChecker) check(st source.Stats) error {
	gen := fmt.Sprintf("%d/%d", st.Driver.Generation.Salt1, st.Driver.Generation.Salt2)
	if gen != c.generation {
		c.generation = gen
		c.restarted = time.Now()
	}
	if st.Driver.Checkpoint.At.Before(c.restarted) {
		// The checkpoint ran before the log was restarted: its backfill
		// belongs to the generation before it.
		return nil
	}
	ack := st.Driver.AckPosition
	if ack.Salt1 != st.Driver.Generation.Salt1 || ack.Salt2 != st.Driver.Generation.Salt2 {
		// The acknowledgement belongs to another generation: it says
		// nothing about the frames of this one.
		return nil
	}
	if st.Driver.Checkpoint.Checkpointed > int(ack.Frame) {
		return fmt.Errorf("the driver wrote %d frames back, the capture acknowledged %d of them",
			st.Driver.Checkpoint.Checkpointed, ack.Frame)
	}
	return nil
}

// waitFor waits until the condition holds.
//
// The condition is checked every few milliseconds: the tests do not sleep
// for a fixed time.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitCatchUp waits until the backup holds the increments of the primary.
//
// The condition has to hold for a moment: the capture of the source runs
// after the commit of the business, a sample may be taken before the last
// transaction reached the spool. The source must not capture anything during
// that moment: a transaction that is committed but not captured yet is
// missing on the backup.
//
// The source has to capture the whole log before the two sides are in line:
// the transactions that are still in the log are not on the backup. The
// statistics of the source are a snapshot: a capture round that runs while
// the source writes into the spool is not visible in them.
//
// The source holds the transactions that did not fit into the spool in its
// memory: they are not durable and the backup does not have them yet. The
// source reports the spool as full while it holds them.
func (r *relay) waitCatchUp() {
	r.t.Helper()
	since := time.Time{}
	seq := uint64(0)
	frame := uint64(0)
	if !waitFor(catchUpTimeout, func() bool {
		src := r.sourceStats()
		bck := r.backupStats()
		if src.CapturedSeq != bck.AppliedSeq || src.BackupSeq != src.CapturedSeq || src.SpoolFull {
			since = time.Time{}
			return false
		}
		if src.Driver.CaptureCursor.Frame < uint64(src.Driver.WALFrames) {
			// The source did not capture the whole log yet.
			since = time.Time{}
			return false
		}
		if src.CapturedSeq != seq || src.Driver.CaptureCursor.Frame != frame {
			// The source captured more: the backup has to apply it before
			// the two sides are in line.
			seq = src.CapturedSeq
			frame = src.Driver.CaptureCursor.Frame
			since = time.Time{}
			return false
		}
		if since.IsZero() {
			since = time.Now()
			return false
		}
		return time.Since(since) > 50*time.Millisecond
	}) {
		src := r.sourceStats()
		bck := r.backupStats()
		r.t.Fatalf("the backup did not catch up: the source captured %d, the backup applied %d, the source holds the acknowledgement %d, the spool is full: %t",
			src.CapturedSeq, bck.AppliedSeq, src.BackupSeq, src.SpoolFull)
	}
}

// queryPrimary runs a query on the primary database.
//
// The query runs in the process of the primary: the parent does not open
// the database of the primary, the driver owns it.
func (r *relay) queryPrimary(query string) rows {
	r.t.Helper()
	out, err := r.primary.do("query %s", query)
	if err != nil {
		r.t.Fatalf("the query %q on the primary: %v", query, err)
	}
	var res rows
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		r.t.Fatalf("decode the rows of the query %q: %v", query, err)
	}
	return res
}

// openBackupDB opens the database of the backup for the business.
//
// The business reads the database of the backup while the backup is paused.
// The database is not opened with SQLite by the relay in that state: the
// file belongs to the business.
func (r *relay) openBackupDB() *sql.DB {
	r.t.Helper()
	db, err := sql.Open("sqlite3", "file:"+r.backupDBPath()+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		r.t.Fatalf("open the database of the backup: %v", err)
	}
	return db
}

// queryBackup runs a query on the database of the backup.
func (r *relay) queryBackup(query string) rows {
	r.t.Helper()
	db := r.openBackupDB()
	defer func() { _ = db.Close() }()
	res, err := readRows(db, query)
	if err != nil {
		r.t.Fatalf("the query %q on the backup: %v", query, err)
	}
	return res
}

// compare checks that the backup holds the data and the schema of the
// primary database.
//
// The comparison is a logical one: the same query runs on the primary
// database and on the database of the backup and the rows are compared one
// by one. The bytes of the two files are not the same: the backup rewrites
// the header of the first page.
//
// A mismatch reports the state of the relay: the sequence numbers of the two
// sides and the epoch of the chain say where the chain stopped.
func (r *relay) compare() {
	r.t.Helper()
	for _, query := range []string{queryAccounts, queryEntries, querySchema} {
		want := r.queryPrimary(query)
		got := r.queryBackup(query)
		if !reflect.DeepEqual(want, got) {
			r.t.Errorf("the backup does not hold the rows of the primary database for the query %q:\nprimary: %v\nbackup:  %v\nstate:%s",
				query, want.Rows, got.Rows, r.state())
		}
	}
	r.checkIntegrity()
}

// state returns the statistics of the two sides of the relay.
//
// The state is best effort: a process that is gone has no statistics.
func (r *relay) state() string {
	var b strings.Builder
	b.WriteString("\n  source: ")
	if out, err := r.primary.do("stats"); err == nil {
		b.WriteString(out)
	} else {
		b.WriteString(err.Error())
	}
	b.WriteString("\n  backup: ")
	if out, err := r.backup.do("stats"); err == nil {
		b.WriteString(out)
	} else {
		b.WriteString(err.Error())
	}
	return b.String()
}

// checkIntegrity checks the database of the backup.
func (r *relay) checkIntegrity() {
	r.t.Helper()
	db := r.openBackupDB()
	defer func() { _ = db.Close() }()
	var result string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&result); err != nil {
		r.t.Fatalf("the integrity check of the backup: %v", err)
	}
	if result != "ok" {
		r.t.Errorf("the integrity check of the backup reported %q, want %q", result, "ok")
	}
}

// createSchema writes the schema of the primary database.
func (r *relay) createSchema() {
	r.t.Helper()
	for _, ddl := range []string{createAccounts, createEntries, createIndex} {
		if _, err := r.primary.do("ddl %s", ddl); err != nil {
			r.t.Fatalf("the ddl %q: %v", ddl, err)
		}
	}
}

// insert writes n rows into the accounts table of the primary database.
func (r *relay) insert(n int) {
	r.t.Helper()
	if _, err := r.primary.do("insert %d", n); err != nil {
		r.t.Fatalf("the insert of %d rows: %v", n, err)
	}
}

// bulk writes n rows into the accounts table in one transaction.
func (r *relay) bulk(n int) {
	r.t.Helper()
	if _, err := r.primary.do("bulk %d", n); err != nil {
		r.t.Fatalf("the insert of %d rows: %v", n, err)
	}
}

// entries writes n rows into the entries table of the primary database.
func (r *relay) entries(n int) {
	r.t.Helper()
	if _, err := r.primary.do("entries %d", n); err != nil {
		r.t.Fatalf("the insert of %d entries: %v", n, err)
	}
}
