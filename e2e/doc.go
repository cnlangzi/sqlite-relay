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
package e2e
