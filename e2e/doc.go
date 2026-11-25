// Package e2e holds the end-to-end tests of the relay.
//
// The tests of this package run the two sides of a relay in two processes:
// the sides talk to each other over a real TCP connection, they are stopped
// with real signals and they write into a real file system.
//
// The tests are guarded by the build tag "e2e". They are slow and they need
// a file system that honors fsync, so they run in their own target:
//
//	make e2e
//
// The work directories are created in the directory of the environment
// variable SQLITE_RELAY_E2E_ROOT and in the temporary directory of the
// operating system when the variable is not set. A tmpfs does not honor
// fsync: the persistence barriers of the relay are not told apart on it.
package e2e
