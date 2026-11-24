package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/mattn/go-sqlite3"
)

// connector opens managed connections.
//
// Every physical connection of the pool passes through Connect, so the
// per-connection invariants are installed on all of them, not only on the
// first one.
type connector struct {
	d   *Driver
	drv *sqlite3.SQLiteDriver
	// gated reports whether the quiesce gate applies to the connection. The
	// control connections of the driver are never gated, otherwise a
	// quiesce would stop the driver from checkpointing.
	gated bool
}

func newConnector(d *Driver, gated bool) *connector {
	return &connector{
		d:     d,
		drv:   &sqlite3.SQLiteDriver{ConnectHook: d.connectHook},
		gated: gated,
	}
}

// Connect opens a new physical connection and installs the invariants on it.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.d.cfg.DSN)
	if err != nil {
		return nil, err
	}
	sc, ok := conn.(*sqlite3.SQLiteConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("driver: unexpected connection type %T", conn)
	}
	mc := &managedConn{conn: sc, d: c.d, gated: c.gated}
	c.d.register(mc)
	return mc, nil
}

// Driver implements driver.Connector.
func (c *connector) Driver() driver.Driver {
	return c.drv
}

// managedConn is a physical connection the driver owns.
//
// It is the only way into the primary database inside the process, which is
// what makes the managed-ness contract enforceable.
type managedConn struct {
	conn  *sqlite3.SQLiteConn
	d     *Driver
	gated bool

	mu   sync.Mutex
	inTx bool

	// useMu guards the raw use of conn against the close of the
	// connection. The invariants of the connection are checked outside of
	// database/sql, which owns the lifecycle of the connection and closes
	// it when it discards it.
	useMu sync.RWMutex
}

// Prepare implements driver.Conn.
func (m *managedConn) Prepare(query string) (driver.Stmt, error) {
	if m.gated {
		if err := m.d.gate.waitStatement(context.Background(), query, m); err != nil {
			return nil, err
		}
	}
	stmt, err := m.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &managedStmt{stmt: stmt, query: query, d: m.d, conn: m}, nil
}

// Close implements driver.Conn.
func (m *managedConn) Close() error {
	m.useMu.Lock()
	defer m.useMu.Unlock()
	m.d.unregister(m)
	return m.conn.Close()
}

// Begin implements driver.Conn.
func (m *managedConn) Begin() (driver.Tx, error) {
	return m.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx.
//
// The context of the caller is passed on, so that a writer that waits for
// the resume can be cancelled.
func (m *managedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if m.gated {
		if err := m.d.gate.wait(ctx, true); err != nil {
			return nil, err
		}
	}
	tx, err := m.conn.Begin()
	if err != nil {
		return nil, err
	}
	m.setInTx(true)
	m.d.txOpened()
	return &managedTx{tx: tx, m: m}, nil
}

// managedStmt applies the quiesce gate when a statement runs.
//
// The gate has to be applied when the statement runs and not when it is
// prepared: the statement pool of database/sql keeps prepared statements
// around and reuses them without preparing them again.
type managedStmt struct {
	stmt  driver.Stmt
	query string
	d     *Driver
	conn  *managedConn
}

// Close implements driver.Stmt.
func (s *managedStmt) Close() error {
	return s.stmt.Close()
}

// NumInput implements driver.Stmt.
func (s *managedStmt) NumInput() int {
	return s.stmt.NumInput()
}

// Exec implements driver.Stmt.
func (s *managedStmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.d.gate.waitStatement(context.Background(), s.query, s.conn); err != nil {
		return nil, err
	}
	return s.stmt.Exec(args) //nolint:staticcheck // a driver has to implement the deprecated methods of driver.Stmt
}

// Query implements driver.Stmt.
func (s *managedStmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.d.gate.waitStatement(context.Background(), s.query, s.conn); err != nil {
		return nil, err
	}
	return s.stmt.Query(args) //nolint:staticcheck // a driver has to implement the deprecated methods of driver.Stmt
}

// ExecContext implements driver.StmtExecContext.
func (s *managedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.d.gate.waitStatement(ctx, s.query, s.conn); err != nil {
		return nil, err
	}
	return s.stmt.Exec(namedToValue(args)) //nolint:staticcheck // the statement is held as a driver.Stmt, whose methods are deprecated
}

// QueryContext implements driver.StmtQueryContext.
func (s *managedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.d.gate.waitStatement(ctx, s.query, s.conn); err != nil {
		return nil, err
	}
	return s.stmt.Query(namedToValue(args)) //nolint:staticcheck // the statement is held as a driver.Stmt, whose methods are deprecated
}

// namedToValue turns the arguments of a statement into the values the
// driver of SQLite understands.
func namedToValue(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

func (m *managedConn) setInTx(inTx bool) {
	m.mu.Lock()
	m.inTx = m.inTx || inTx
	m.mu.Unlock()
}

func (m *managedConn) clearInTx() {
	m.mu.Lock()
	m.inTx = false
	m.mu.Unlock()
}

func (m *managedConn) inTransaction() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inTx
}

// managedTx tracks the transaction state of a managed connection so that the
// quiesce gate can tell a statement that starts a transaction from one that
// continues an existing transaction.
type managedTx struct {
	tx driver.Tx
	m  *managedConn
}

func (t *managedTx) Commit() error {
	err := t.tx.Commit()
	t.m.clearInTx()
	t.m.d.txClosed()
	return err
}

func (t *managedTx) Rollback() error {
	err := t.tx.Rollback()
	t.m.clearInTx()
	t.m.d.txClosed()
	return err
}

// register records a managed connection.
func (d *Driver) register(c *managedConn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conns == nil {
		d.conns = make(map[*managedConn]struct{})
	}
	d.conns[c] = struct{}{}
}

// unregister forgets a managed connection.
func (d *Driver) unregister(c *managedConn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.conns, c)
}

// Managed returns the number of physical connections the driver owns.
func (d *Driver) Managed() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

// connectHook installs the per-connection invariants.
//
// The order matters: SQLite replaces a registered WAL hook whenever
// wal_autocheckpoint is set, so autocheckpoint has to be disabled before the
// hook is installed.
func (d *Driver) connectHook(conn *sqlite3.SQLiteConn) error {
	if _, err := conn.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", d.cfg.BusyTimeout.Milliseconds()), nil); err != nil {
		return fmt.Errorf("driver: set busy_timeout: %w", err)
	}
	mode, err := pragmaString(conn, "PRAGMA journal_mode")
	if err != nil {
		return fmt.Errorf("driver: read journal_mode: %w", err)
	}
	if mode != "wal" {
		if _, err := conn.Exec("PRAGMA journal_mode = WAL", nil); err != nil {
			return fmt.Errorf("driver: set journal_mode: %w", err)
		}
		if mode, err = pragmaString(conn, "PRAGMA journal_mode"); err != nil {
			return fmt.Errorf("driver: read journal_mode: %w", err)
		}
	}
	if mode != "wal" {
		return fmt.Errorf("driver: journal_mode is %q, want \"wal\"", mode)
	}
	if err := d.disableAutocheckpoint(conn); err != nil {
		return err
	}
	conn.RegisterWalHook(d.walHook)
	return nil
}

// disableAutocheckpoint turns the automatic checkpoint off and verifies that
// the WAL hook is still the one the driver installed.
//
// SQLite answers PRAGMA wal_autocheckpoint with the threshold of the
// automatic checkpoint hook. As long as the driver owns the hook the answer
// is zero.
func (d *Driver) disableAutocheckpoint(conn *sqlite3.SQLiteConn) error {
	if _, err := conn.Exec("PRAGMA wal_autocheckpoint = 0", nil); err != nil {
		return fmt.Errorf("driver: set wal_autocheckpoint: %w", err)
	}
	n, err := pragmaInt(conn, "PRAGMA wal_autocheckpoint")
	if err != nil {
		return fmt.Errorf("driver: read wal_autocheckpoint: %w", err)
	}
	if n != 0 {
		return fmt.Errorf("driver: wal_autocheckpoint is %d, want 0", n)
	}
	return nil
}

// checkConnInvariants verifies the connection level invariants of a managed
// connection and reinstalls the WAL hook if it was replaced.
//
// A connection that was taken over by somebody who set wal_autocheckpoint
// runs the automatic checkpoint of SQLite, which would backfill the WAL
// behind the back of the driver. The driver reports the condition and puts
// the hook back.
func (d *Driver) checkConnInvariants(conn *sqlite3.SQLiteConn) error {
	n, err := pragmaInt(conn, "PRAGMA wal_autocheckpoint")
	if err != nil {
		return fmt.Errorf("driver: read wal_autocheckpoint: %w", err)
	}
	if n == 0 {
		return nil
	}
	// Switching the automatic checkpoint off removes the hook of the
	// driver as well, so the hook has to be put back.
	if err := d.disableAutocheckpoint(conn); err != nil {
		return err
	}
	conn.RegisterWalHook(d.walHook)
	return fmt.Errorf("driver: wal_autocheckpoint was set to %d on a managed connection, the WAL hook was replaced", n)
}

// checkInvariants verifies the connection level invariants of every managed
// connection.
//
// The control connections belong to the driver alone, so the business
// connections are the ones that can lose their hook.
func (d *Driver) checkInvariants() error {
	d.mu.Lock()
	conns := make([]*managedConn, 0, len(d.conns))
	for c := range d.conns {
		conns = append(conns, c)
	}
	d.mu.Unlock()
	for _, c := range conns {
		c.useMu.RLock()
		err := d.checkConnInvariants(c.conn)
		c.useMu.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// pragmaInt runs a pragma that answers with a single integer.
func pragmaInt(conn *sqlite3.SQLiteConn, query string) (int64, error) {
	v, err := pragmaValue(conn, query)
	if err != nil {
		return 0, err
	}
	switch t := v.(type) {
	case int64:
		return t, nil
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("driver: %q returned %T", query, v)
	}
}

// pragmaString runs a pragma that answers with a single string.
func pragmaString(conn *sqlite3.SQLiteConn, query string) (string, error) {
	v, err := pragmaValue(conn, query)
	if err != nil {
		return "", err
	}
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	default:
		return "", fmt.Errorf("driver: %q returned %T", query, v)
	}
}

// pragmaValue runs a pragma and returns the value of its first column.
func pragmaValue(conn *sqlite3.SQLiteConn, query string) (any, error) {
	rows, err := conn.Query(query, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return nil, fmt.Errorf("driver: %q: %w", query, err)
	}
	return dest[0], nil
}

// openPool opens a pool of managed connections.
func (d *Driver) openPool(gated bool, maxOpen int) (*sql.DB, error) {
	db := sql.OpenDB(newConnector(d, gated))
	if maxOpen > 0 {
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxOpen)
	}
	return db, nil
}
