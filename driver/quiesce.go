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
//
// A write is held back until the gate opens. A read is only held back when
// the caller asked for it.
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

// waitStatement blocks a statement while the driver is quiesced.
//
// A statement that continues a transaction that was opened before the
// quiesce is not blocked, otherwise the transaction could never finish.
func (g *gate) waitStatement(ctx context.Context, query string, conn *managedConn) error {
	if !g.active() {
		return nil
	}
	write := isWriteStatement(query)
	if !write {
		if !g.readsBlocked() {
			return nil
		}
	} else if conn != nil && conn.inTransaction() {
		return nil
	}
	return g.wait(ctx, write)
}

// readsBlocked reports whether reads are held back as well.
func (g *gate) readsBlocked() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.blockReads
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
// The classification looks at the first keyword only. A statement that
// hides a write behind a common table expression is recognised by the
// keyword that follows the expression.
func isWriteStatement(query string) bool {
	word := firstKeyword(query)
	if word == "" {
		return false
	}
	if word != "WITH" {
		return writeKeywords[word]
	}
	// A common table expression is followed by the statement it feeds. Look
	// for it outside of the parentheses of the expression.
	return withStatementIsWrite(query)
}

// withStatementIsWrite reports whether the statement behind a common table
// expression changes the database.
func withStatementIsWrite(query string) bool {
	depth := 0
	words := strings.FieldsFunc(query, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_')
	})
	for _, w := range words {
		switch w {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		switch strings.ToUpper(w) {
		case "INSERT", "UPDATE", "DELETE", "REPLACE":
			return true
		case "AS", "MATERIALIZED", "NOT", "RECURSIVE", "WITH", "SELECT":
			// Part of the expression, not the statement behind it.
		default:
			if writeKeywords[strings.ToUpper(w)] {
				return true
			}
		}
	}
	return false
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
