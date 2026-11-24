package driver

import (
	"context"
	"strings"
	"sync"
)

// gate blocks new write transactions while the driver is quiesced.
//
// A blocked writer waits for the resume instead of failing with SQLITE_BUSY,
// so that the window in which no new write starts stays free of errors.
type gate struct {
	mu         sync.Mutex
	quiesced   bool
	blockReads bool
	resume     chan struct{}
}

func newGate() *gate {
	return &gate{resume: make(chan struct{})}
}

// quiesce installs the gate.
func (g *gate) quiesce(blockReads bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.quiesced {
		g.blockReads = g.blockReads || blockReads
		return
	}
	g.quiesced = true
	g.blockReads = blockReads
	g.resume = make(chan struct{})
}

// resume opens the gate again.
func (g *gate) resumeGate() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.quiesced {
		return
	}
	g.quiesced = false
	g.blockReads = false
	close(g.resume)
}

// active reports whether the gate is closed.
func (g *gate) active() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.quiesced
}

// wait blocks until the gate opens. It returns the context error if the
// context is done first.
func (g *gate) wait(ctx context.Context, write bool) error {
	for {
		g.mu.Lock()
		if !g.quiesced || (!write && !g.blockReads) {
			g.mu.Unlock()
			return nil
		}
		resume := g.resume
		g.mu.Unlock()

		select {
		case <-resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// waitWrite blocks a statement that would start a write transaction.
//
// A statement that continues a transaction that was opened before the
// quiesce is not blocked, otherwise the transaction could never finish.
func (g *gate) waitWrite(query string, conn *managedConn) error {
	if !g.active() {
		return nil
	}
	if !isWriteStatement(query) {
		return nil
	}
	if conn != nil && conn.inTransaction() {
		return nil
	}
	return g.wait(context.Background(), true)
}

// writeKeywords are the leading keywords of statements that change the
// database.
var writeKeywords = map[string]bool{
	"BEGIN":   true,
	"INSERT":  true,
	"UPDATE":  true,
	"DELETE":  true,
	"REPLACE": true,
	"CREATE":  true,
	"DROP":    true,
	"ALTER":   true,
	"VACUUM":  true,
	"REINDEX": true,
}

// isWriteStatement reports whether the statement changes the database.
//
// The classification looks at the first keyword only. It is a heuristic: a
// statement that hides a write behind a different keyword is not recognised.
func isWriteStatement(query string) bool {
	word := firstKeyword(query)
	if word == "" {
		return false
	}
	return writeKeywords[word]
}

// firstKeyword returns the first SQL keyword of a statement.
func firstKeyword(query string) string {
	s := query
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s, "*/"); i >= 0 {
				s = s[i+2:]
				continue
			}
			return ""
		}
		break
	}
	end := 0
	for end < len(s) && (isLetter(s[end]) || s[end] == '_') {
		end++
	}
	return strings.ToUpper(s[:end])
}

func isLetter(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
