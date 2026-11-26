package driver

import "fmt"

// walHook is the callback SQLite runs after a transaction committed.
//
// SQLite calls it while it holds the database mutex, so the callback may
// only do bounded work. It never parses the WAL, never touches the disk and
// never talks to the network: it hands a notification to the consumer and
// returns.
func (d *Driver) walHook(dbName string, frames int) (rc int) {
	defer func() {
		if r := recover(); r != nil {
			// A panic in the hook must never reach the statement that
			// provoked the commit. The commit itself has already happened
			// at this point, so the only thing left to do is to report the
			// panic and let the statement succeed.
			d.recordHookPanic(r)
			rc = 0
		}
	}()
	d.announce(Notification{DBName: dbName, Frames: frames})
	return 0
}

// announce hands the notification of a commit to the consumer.
//
// The channel is allowed to be full: a dropped notification only costs a
// wake-up, the periodic scan finds the same commit anyway. The channel may
// be closed already, a notification that nobody waits for is dropped.
//
// The counters tell how many commits the hook announced and how many of the
// announcements the full channel swallowed.
func (d *Driver) announce(n Notification) {
	d.eventsMu.Lock()
	defer d.eventsMu.Unlock()
	if d.eventsClosed {
		// The driver is closed: nobody waits for the notification any
		// more, it is not a lost announcement.
		return
	}
	select {
	case d.events <- n:
		d.notifications.Add(1)
	default:
		d.droppedNotifications.Add(1)
	}
}

// notify delivers a wake-up hint to the consumer.
//
// The hint of the periodic scan is not the announcement of a commit: it
// points at the frames the consumer did not capture yet. It is not counted.
func (d *Driver) notify(n Notification) {
	d.eventsMu.Lock()
	defer d.eventsMu.Unlock()
	if d.eventsClosed {
		return
	}
	select {
	case d.events <- n:
	default:
	}
}

// recordHookPanic remembers a panic that happened inside the WAL hook.
func (d *Driver) recordHookPanic(r any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hookPanics++
	d.lastErr = &hookPanicError{value: r}
}

// hookPanicError reports a panic that was caught inside the WAL hook.
type hookPanicError struct {
	value any
}

func (e *hookPanicError) Error() string {
	return fmt.Sprintf("driver: panic in WAL hook: %v", e.value)
}
