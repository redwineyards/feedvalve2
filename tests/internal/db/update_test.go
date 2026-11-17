package db_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/db"
	"feedvalve/internal/mirror"
	"feedvalve/internal/schema"
)

// exec is one statement the fake driver ran.
type exec struct {
	sql  string
	args []any
}

// fakeDB records what a commit does. failAt fails the nth statement (from 1);
// block makes every statement wait for its context to end; missing is how many
// rows of each statement match no table row.
type fakeDB struct {
	mu        sync.Mutex
	execs     []exec
	commits   int
	rollbacks int
	failAt    int
	block     bool
	missing   int64
}

var (
	fakesMu sync.Mutex
	fakes   = map[string]*fakeDB{}
)

func init() { sql.Register("fakemerge", fakeDriver{}) }

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	fakesMu.Lock()
	defer fakesMu.Unlock()
	return &fakeConn{db: fakes[name]}, nil
}

type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return fakeTx{c.db}, nil }

// CheckNamedValue accepts slices, as go-ora does for array binds.
func (c *fakeConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c *fakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	f := c.db
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	e := exec{sql: query}
	for _, a := range args {
		if v, ok := a.Value.([]int64); ok {
			a.Value = append([]int64(nil), v...)
		}
		e.args = append(e.args, a.Value)
	}
	f.execs = append(f.execs, e)
	if len(f.execs) == f.failAt {
		return nil, errors.New("ORA-00001: unique constraint violated")
	}
	rows := int64(1)
	if n := len(args); n > 0 {
		if keys, ok := args[n-1].Value.([]int64); ok {
			rows = int64(len(keys))
		}
	}
	return driver.RowsAffected(rows - f.missing), nil
}

type fakeTx struct{ db *fakeDB }

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

// openFake returns a *sql.DB backed by a fresh fakeDB.
func openFake(t *testing.T) (*sql.DB, *fakeDB) {
	t.Helper()
	f := &fakeDB{}
	fakesMu.Lock()
	fakes[t.Name()] = f
	fakesMu.Unlock()
	conn, err := sql.Open("fakemerge", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, f
}

// loadSchema writes body to a file and loads it.
func loadSchema(t *testing.T, body string) *schema.Schema {
	t.Helper()
	p := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := schema.Load(p)
	if err != nil {
		t.Fatalf("schema.Load: %v", err)
	}
	return s
}

// row builds a batch row setting slot → value pairs.
func row(dest uint8, key int64, vals map[int]int64) mirror.BatchRow {
	r := mirror.BatchRow{Dest: dest, Key: key}
	for slot, v := range vals {
		r.Row.Mask |= 1 << slot
		r.Row.Cols[slot] = v
	}
	return r
}

// batchOf returns a batch holding rows.
func batchOf(rows ...mirror.BatchRow) *mirror.Batch {
	b := mirror.NewBatch(0, len(rows))
	b.Rows = append(b.Rows, rows...)
	return b
}

const quoteSchema = `{"destinations": [{"table": "Q", "key": "TOKEN", "columns": [
  {"column": "P", "field": "price", "type": "decimal", "scale": 2},
  {"column": "V", "field": "volume", "type": "number"},
  {"column": "D", "field": "tradeTime", "type": "date"},
  {"column": "I", "field": "indicator", "type": "char"},
  {"column": "U", "type": "sysdate"}]}]}`

func TestCommitOneArrayBoundUpdate(t *testing.T) {
	conn, f := openFake(t)
	commit := db.NewCommit(conn, loadSchema(t, quoteSchema), time.Second, new(db.Stats))

	err := commit(batchOf(
		row(0, 7, map[int]int64{0: 12345, 1: 10}),
		row(0, 9, map[int]int64{0: 500, 1: 20}),
	))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(f.execs) != 1 {
		t.Fatalf("ran %d statements, want 1: %+v", len(f.execs), f.execs)
	}
	wantSQL := "UPDATE Q SET P = :1 / 100, V = :2, U = SYSDATE WHERE TOKEN = :3"
	if f.execs[0].sql != wantSQL {
		t.Errorf("SQL =\n%s\nwant\n%s", f.execs[0].sql, wantSQL)
	}
	wantArgs := []any{[]int64{12345, 500}, []int64{10, 20}, []int64{7, 9}}
	if !reflect.DeepEqual(f.execs[0].args, wantArgs) {
		t.Errorf("args = %v, want %v (values in slot order, then keys)", f.execs[0].args, wantArgs)
	}
	if f.commits != 1 || f.rollbacks != 0 {
		t.Errorf("(commits, rollbacks) = (%d, %d), want (1, 0)", f.commits, f.rollbacks)
	}
}

func TestCommitTypeConversions(t *testing.T) {
	conn, f := openFake(t)
	commit := db.NewCommit(conn, loadSchema(t, quoteSchema), time.Second, new(db.Stats))
	if err := commit(batchOf(row(0, 1, map[int]int64{2: 1451610123, 3: '+'}))); err != nil {
		t.Fatalf("commit: %v", err)
	}
	wantSQL := "UPDATE Q SET D = DATE '1980-01-01' + NUMTODSINTERVAL(:1, 'SECOND'), I = CHR(:2), U = SYSDATE WHERE TOKEN = :3"
	if len(f.execs) != 1 || f.execs[0].sql != wantSQL {
		t.Errorf("SQL =\n%v\nwant\n%s", f.execs, wantSQL)
	}
}

const threeSchema = `{"destinations": [
  {"table": "A", "key": "K", "columns": [{"column": "X", "field": "x", "type": "number"}, {"column": "Y", "field": "y", "type": "number"}]},
  {"table": "B", "key": "K", "columns": [{"column": "X", "field": "x", "type": "number"}]},
  {"table": "C", "key": "K", "columns": [{"column": "X", "field": "x", "type": "number"}]}]}`

func TestCommitDestinationOrderAndGrouping(t *testing.T) {
	conn, f := openFake(t)
	commit := db.NewCommit(conn, loadSchema(t, threeSchema), time.Second, new(db.Stats))
	err := commit(batchOf(
		row(0, 1, map[int]int64{0: 1}),
		row(0, 2, map[int]int64{0: 2, 1: 2}),
		row(0, 3, map[int]int64{0: 3}),
		row(1, 1, map[int]int64{0: 4}),
		row(2, 5, map[int]int64{0: 5}),
	))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	var tables []string
	var keys []any
	for _, e := range f.execs {
		tables = append(tables, strings.Fields(e.sql)[1])
		keys = append(keys, e.args[len(e.args)-1])
	}
	if want := []string{"A", "A", "B", "C"}; !reflect.DeepEqual(tables, want) {
		t.Errorf("tables in order = %v, want %v", tables, want)
	}
	wantKeys := []any{[]int64{1, 3}, []int64{2}, []int64{1}, []int64{5}}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("keys per statement = %v, want %v (one statement per column set, first seen first)", keys, wantKeys)
	}
	if f.commits != 1 {
		t.Errorf("commits = %d, want 1: one transaction per batch", f.commits)
	}
}

func TestCommitIgnoresColumnsNotInSchema(t *testing.T) {
	conn, f := openFake(t)
	commit := db.NewCommit(conn, loadSchema(t, threeSchema), time.Second, new(db.Stats))
	err := commit(batchOf(
		row(1, 1, map[int]int64{0: 1, 10: 99}), // slot 10 has no column in B
		row(1, 2, map[int]int64{10: 99}),       // nothing writable: skipped
	))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(f.execs) != 1 || !reflect.DeepEqual(f.execs[0].args, []any{[]int64{1}, []int64{1}}) {
		t.Errorf("execs = %+v, want one statement for row 1, column X only", f.execs)
	}
}

func TestCommitCountsUnmatchedTokens(t *testing.T) {
	conn, f := openFake(t)
	f.missing = 1
	st := new(db.Stats)
	commit := db.NewCommit(conn, loadSchema(t, quoteSchema), time.Second, st)
	err := commit(batchOf(row(0, 1, map[int]int64{1: 1}), row(0, 2, map[int]int64{1: 2})))
	if err != nil {
		t.Fatalf("commit: %v (an unmatched token is not an error)", err)
	}
	if n := st.Unmatched.Load(); n != 1 {
		t.Errorf("Unmatched = %d, want 1", n)
	}
	if f.commits != 1 {
		t.Errorf("commits = %d, want 1", f.commits)
	}
}

func TestCommitFailureRollsBack(t *testing.T) {
	conn, f := openFake(t)
	f.failAt = 2
	commit := db.NewCommit(conn, loadSchema(t, threeSchema), time.Second, new(db.Stats))
	err := commit(batchOf(row(0, 1, map[int]int64{0: 1}), row(1, 1, map[int]int64{0: 1})))
	if err == nil {
		t.Fatal("commit succeeded, want the driver's error")
	}
	for _, want := range []string{"B", "ORA-00001"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if f.commits != 0 || f.rollbacks != 1 {
		t.Errorf("(commits, rollbacks) = (%d, %d), want (0, 1)", f.commits, f.rollbacks)
	}
}

func TestCommitUnknownDestination(t *testing.T) {
	conn, f := openFake(t)
	commit := db.NewCommit(conn, loadSchema(t, quoteSchema), time.Second, new(db.Stats))
	err := commit(batchOf(row(2, 1, map[int]int64{0: 1})))
	if err == nil || !strings.Contains(err.Error(), "destination 2") {
		t.Fatalf("err = %v, want one naming destination 2", err)
	}
	if f.commits != 0 {
		t.Errorf("commits = %d, want 0", f.commits)
	}
}

func TestCommitTimesOut(t *testing.T) {
	conn, f := openFake(t)
	f.block = true
	commit := db.NewCommit(conn, loadSchema(t, quoteSchema), 50*time.Millisecond, new(db.Stats))
	start := time.Now()
	err := commit(batchOf(row(0, 1, map[int]int64{0: 1})))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want a deadline error", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("commit took %v, want it bounded by the 50ms timeout", d)
	}
}

// TestUpdateIntegration needs a real Oracle instance and a user that may
// create tables. Set FEEDVALVE_TEST_CONFIG and FEEDVALVE_DB_PASSWORD to run it.
func TestUpdateIntegration(t *testing.T) {
	path, password := os.Getenv("FEEDVALVE_TEST_CONFIG"), os.Getenv("FEEDVALVE_DB_PASSWORD")
	if path == "" || password == "" {
		t.Skip("FEEDVALVE_TEST_CONFIG and FEEDVALVE_DB_PASSWORD not set")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	target := db.Target{Host: cfg.DB.Host, Port: cfg.DB.Port, Service: cfg.DB.Service, User: cfg.DB.User}
	conn, err := db.Open(context.Background(), target, password, time.Duration(cfg.DB.ConnectTimeoutMs)*time.Millisecond)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	const table = "FV_UPDATE_IT"
	conn.Exec("DROP TABLE " + table)
	if _, err := conn.Exec("CREATE TABLE " + table + " (TOKEN NUMBER(10) UNIQUE, P NUMBER(12,2), V NUMBER(10), D DATE, I CHAR(1), U DATE)"); err != nil {
		t.Fatalf("create scratch table: %v", err)
	}
	defer conn.Exec("DROP TABLE " + table)
	if _, err := conn.Exec("INSERT INTO " + table + " (TOKEN) SELECT LEVEL FROM dual CONNECT BY LEVEL <= 2"); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	// 01-Jan-2026 01:02:03, exchange wall-clock, as seconds since 1980.
	secs := int64(time.Date(2026, 1, 1, 1, 2, 3, 0, time.UTC).Sub(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)).Seconds())
	st := new(db.Stats)
	commit := db.NewCommit(conn, loadSchema(t, strings.Replace(quoteSchema, `"Q"`, `"`+table+`"`, 1)), 10*time.Second, st)
	b := batchOf(
		row(0, 1, map[int]int64{0: 12345, 1: 10, 2: secs, 3: '+'}),
		row(0, 2, map[int]int64{0: 67890, 1: 20, 2: secs, 3: '-'}),
		row(0, 3, map[int]int64{0: 1, 1: 1, 2: secs, 3: '+'}), // no such row: never inserted
	)
	if err := commit(b); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := st.Unmatched.Load(); n != 1 {
		t.Errorf("Unmatched = %d, want 1", n)
	}
	var n int
	var p float64
	var d, ind string
	q := "SELECT COUNT(*), MAX(P), MAX(TO_CHAR(D, 'DD-Mon-YYYY HH24:MI:SS')), MIN(I) FROM " + table + " WHERE U IS NOT NULL"
	if err := conn.QueryRow(q).Scan(&n, &p, &d, &ind); err != nil {
		t.Fatal(err)
	}
	if n != 2 || fmt.Sprint(p) != "678.9" || d != "01-Jan-2026 01:02:03" || ind != "+" {
		t.Errorf("(rows, max price, date, indicator) = (%d, %v, %q, %q), want (2, 678.9, %q, %q)", n, p, d, ind, "01-Jan-2026 01:02:03", "+")
	}
}
