package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/bits"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"feedvalve/internal/mirror"
	"feedvalve/internal/schema"
)

// Stats counts what the commits found. Safe to read concurrently.
type Stats struct {
	Unmatched atomic.Uint64 // rows whose key matched no table row; not written, not retried
}

// NewCommit returns the commit for one writer. It writes a batch as one
// transaction, destinations in schema order (HLD §8.3), with one array-bound
// UPDATE per destination per distinct set of columns. Rows are only updated,
// never inserted: the tables' rows are pre-loaded, and a key that matches no
// row is counted in st. A commit that takes longer than timeout is abandoned
// and rolled back. Each writer needs its own: the statement cache it keeps is
// not shared.
func NewCommit(conn *sql.DB, s *schema.Schema, timeout time.Duration, st *Stats) func(b *mirror.Batch) error {
	var (
		cache []map[uint64]string // per destination: column mask → UPDATE text
	)

	cache = make([]map[uint64]string, len(s.Dests))
	for i := range cache {
		cache[i] = make(map[uint64]string)
	}
	return func(b *mirror.Batch) error {
		return commit(conn, s, cache, timeout, st, b)
	}
}

// commit writes b in one transaction, or rolls it back and returns why not.
func commit(conn *sql.DB, s *schema.Schema, cache []map[uint64]string, timeout time.Duration, st *Stats, b *mirror.Batch) error {
	var (
		ctx    context.Context    // bounds the whole transaction
		cancel context.CancelFunc // releases ctx
		tx     *sql.Tx            // the batch's transaction
		err    error              // result of each step
	)

	for i := range b.Rows {
		if int(b.Rows[i].Dest) >= len(s.Dests) {
			return fmt.Errorf("batch has a row for destination %d, but the schema defines %d", b.Rows[i].Dest, len(s.Dests))
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	for d := range s.Dests {
		if err = writeDest(ctx, tx, &s.Dests[d], uint8(d), cache[d], st, b.Rows); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// writeDest updates b's rows for destination dest, one statement per distinct
// column set, column sets in the order they first appear.
func writeDest(ctx context.Context, tx *sql.Tx, d *schema.Dest, dest uint8, cache map[uint64]string, st *Stats, rows []mirror.BatchRow) error {
	var (
		valid  uint64           // mask of d's columns that hold a field
		mask   uint64           // a row's columns that d has
		masks  []uint64         // distinct masks, first seen first
		groups map[uint64][]int // mask → indices of its rows
		query  string           // the UPDATE for one mask
		ok     bool             // whether query was cached
		args   []any            // one []int64 per bind
		keys   []int64          // the group's row keys
		vals   []int64          // one column's values across the group
		res    sql.Result       // the statement's outcome
		n      int64            // rows it updated
		err    error            // result of the statement
	)

	for i := range d.Columns {
		if d.Columns[i].Type != schema.Sysdate {
			valid |= 1 << i
		}
	}
	groups = make(map[uint64][]int)
	for i := range rows {
		if rows[i].Dest != dest {
			continue
		}
		mask = rows[i].Row.Mask & valid
		if mask == 0 {
			continue
		}
		if _, ok = groups[mask]; !ok {
			masks = append(masks, mask)
		}
		groups[mask] = append(groups[mask], i)
	}

	for _, mask = range masks {
		query, ok = cache[mask]
		if !ok {
			query = updateSQL(d, mask)
			cache[mask] = query
		}
		args = args[:0]
		for m := mask; m != 0; m &= m - 1 {
			vals = make([]int64, 0, len(groups[mask]))
			for _, i := range groups[mask] {
				vals = append(vals, rows[i].Row.Cols[bits.TrailingZeros64(m)])
			}
			args = append(args, vals)
		}
		keys = make([]int64, 0, len(groups[mask]))
		for _, i := range groups[mask] {
			keys = append(keys, rows[i].Key)
		}
		args = append(args, keys)
		res, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("update %s (%d rows): %w", d.Table, len(keys), err)
		}
		if n, err = res.RowsAffected(); err == nil && n < int64(len(keys)) {
			st.Unmatched.Add(uint64(int64(len(keys)) - n))
			slog.Warn("tokens matched no row, not written", "table", d.Table, "rows", len(keys), "updated", n)
		}
	}
	return nil
}

// updateSQL builds the UPDATE of d's columns in mask, plus every sysdate
// column. Every value is bound as an integer and converted to its column type
// in the SQL, which keeps scaled decimals exact. Binds are positional and each
// is used once: the values in slot order, then the key.
func updateSQL(d *schema.Dest, mask uint64) string {
	var (
		set  strings.Builder // the SET assignments
		bind int             // next bind position
		slot int             // column being added
	)

	bind = 1
	for m := mask; m != 0; m &= m - 1 {
		slot = bits.TrailingZeros64(m)
		if set.Len() > 0 {
			set.WriteString(", ")
		}
		fmt.Fprintf(&set, "%s = %s", d.Columns[slot].Name, convert(&d.Columns[slot], ":"+strconv.Itoa(bind)))
		bind++
	}
	for i := range d.Columns {
		if d.Columns[i].Type == schema.Sysdate {
			fmt.Fprintf(&set, ", %s = SYSDATE", d.Columns[i].Name)
		}
	}
	return "UPDATE " + d.Table + " SET " + set.String() + " WHERE " + d.Key + " = :" + strconv.Itoa(bind)
}

// convert returns the SQL that turns bound integer ref into c's column type.
func convert(c *schema.Column, ref string) string {
	switch c.Type {
	case schema.Decimal:
		return ref + " / 1" + strings.Repeat("0", c.Scale)
	case schema.Date:
		// The exchange's own wall-clock: no timezone shift (bible §2.5).
		return "DATE '1980-01-01' + NUMTODSINTERVAL(" + ref + ", 'SECOND')"
	case schema.Char:
		return "CHR(" + ref + ")"
	default:
		return ref
	}
}
