module github.com/cnlangzi/sqlite-relay

go 1.27

// The WAL hook lives in a fork of mattn/go-sqlite3 until the upstream patch
// (PR #1353) is merged. The import path stays github.com/mattn/go-sqlite3 so
// that a single SQLite amalgamation ends up in the binary.
//
// The fork carries its own module path, so the test files of the fork
// import a package that go mod tidy cannot place. Keep the require line
// below by hand, go mod tidy reports "used for two different module paths"
// and changes nothing else.
replace github.com/mattn/go-sqlite3 => github.com/cnlangzi/go-sqlite3 v1.14.53-relay.1

require github.com/mattn/go-sqlite3 v1.14.53
