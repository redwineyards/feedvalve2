// Package fakedb is a database/sql driver for tests: it records every
// statement, commit and rollback instead of talking to Oracle. Each test gets
// its own DB through Open.
package fakedb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
)

// Exec is one statement the driver ran, with copies of its arguments.
type Exec struct {
	SQL  string
	Args []any
}

// DB records what was done through one connection pool. Set FailAt, Block
// and Missing before use; read results through the methods, which are safe
// while the pool is in use.
type DB struct {
	FailAt  int   // fail the nth statement (from 1); 0 never fails
	Block   bool  // make every statement wait for its context to end
	Missing int64 // rows of each statement that match no table row

	mu        sync.Mutex
	execs     []Exec
	commits   int
	rollbacks int
}

// Execs returns a copy of the statements run so far.
func (f *DB) Execs() []Exec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Exec(nil), f.execs...)
}

// Commits returns how many transactions committed.
func (f *DB) Commits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits
}

// Rollbacks returns how many transactions rolled back.
func (f *DB) Rollbacks() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rollbacks
}

var (
	mu  sync.Mutex
	dbs = map[string]*DB{}
)

func init() { sql.Register("fakedb", fakeDriver{}) }

// Open returns a *sql.DB backed by a fresh DB, closed when t ends.
func Open(t *testing.T) (*sql.DB, *DB) {
	t.Helper()
	f := &DB{}
	mu.Lock()
	dbs[t.Name()] = f
	mu.Unlock()
	conn, err := sql.Open("fakedb", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, f
}

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	mu.Lock()
	defer mu.Unlock()
	return &fakeConn{db: dbs[name]}, nil
}

type fakeConn struct{ db *DB }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return fakeTx{c.db}, nil }

// CheckNamedValue accepts slices, as go-ora does for array binds.
func (c *fakeConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c *fakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	f := c.db
	if f.Block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	e := Exec{SQL: query}
	for _, a := range args {
		if v, ok := a.Value.([]int64); ok {
			a.Value = append([]int64(nil), v...)
		}
		e.Args = append(e.Args, a.Value)
	}
	f.execs = append(f.execs, e)
	if len(f.execs) == f.FailAt {
		return nil, errors.New("ORA-00001: unique constraint violated")
	}
	rows := int64(1)
	if n := len(args); n > 0 {
		if keys, ok := args[n-1].Value.([]int64); ok {
			rows = int64(len(keys))
		}
	}
	return driver.RowsAffected(rows - f.Missing), nil
}

type fakeTx struct{ db *DB }

func (t fakeTx) Commit() error {
	t.db.mu.Lock()
	t.db.commits++
	t.db.mu.Unlock()
	return nil
}

func (t fakeTx) Rollback() error {
	t.db.mu.Lock()
	t.db.rollbacks++
	t.db.mu.Unlock()
	return nil
}
