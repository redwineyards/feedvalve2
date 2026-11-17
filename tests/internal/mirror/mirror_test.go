package mirror_test

import (
	"reflect"
	"testing"

	"feedvalve/internal/mirror"
)

// contrib builds a contribution at arrival seq from transcode code to row
// (dest, key), setting the given (column, value) pairs.
func contrib(seq uint64, code uint16, dest uint8, key int64, cols ...[2]int64) mirror.Contribution {
	c := mirror.Contribution{Seq: seq, Code: code, Dest: dest, RowKey: key}
	for _, p := range cols {
		c.Set(int(p[0]), p[1])
	}
	return c
}

// drained is one row as a batch carried it.
type drained struct {
	key  int64
	mask uint64
	cols map[int]int64 // the set columns only
}

// toDrained converts a batch row for comparison.
func toDrained(r *mirror.BatchRow) drained {
	d := drained{key: r.Key, mask: r.Row.Mask, cols: map[int]int64{}}
	for i := range mirror.MaxCols {
		if r.Row.Mask&(1<<i) != 0 {
			d.cols[i] = r.Row.Cols[i]
		}
	}
	return d
}

// flushAll runs one whole epoch in a single batch that commits, and returns
// the rows written per destination, in batch order.
func flushAll(m *mirror.Mirror) [mirror.Dests][]drained {
	var got [mirror.Dests][]drained
	if !m.OpenEpoch() {
		return got
	}
	b := mirror.NewBatch(0, 1<<12)
	m.Fill(b, 1<<12)
	for i := range b.Rows {
		got[b.Rows[i].Dest] = append(got[b.Rows[i].Dest], toDrained(&b.Rows[i]))
	}
	m.Complete(b)
	return got
}

// drainAll flushes every destination and returns dest's rows.
func drainAll(m *mirror.Mirror, dest uint8) []drained {
	return flushAll(m)[dest]
}

// col is shorthand for a (column, value) pair.
func col(i int, v int64) [2]int64 { return [2]int64{int64(i), v} }

func TestMirrorApply(t *testing.T) {
	tests := []struct {
		name  string
		in    []mirror.Contribution
		want  [mirror.Dests][]drained
		stats [5]uint64 // Applied, Stale, NewRows, BadDest, SlotsFull
	}{
		{
			name:  "new row is created and dirtied",
			in:    []mirror.Contribution{contrib(1, 7208, 0, 10, col(0, 5))},
			want:  [mirror.Dests][]drained{{{10, 1, map[int]int64{0: 5}}}},
			stats: [5]uint64{1, 0, 1, 0, 0},
		},
		{
			name: "updates to one row coalesce, latest values win",
			in: []mirror.Contribution{
				contrib(1, 7208, 0, 10, col(0, 5), col(1, 6)),
				contrib(2, 7208, 0, 10, col(0, 7), col(1, 8)),
			},
			want:  [mirror.Dests][]drained{{{10, 3, map[int]int64{0: 7, 1: 8}}}},
			stats: [5]uint64{2, 0, 1, 0, 0},
		},
		{
			name: "masked merge keeps other transcodes' columns",
			in: []mirror.Contribution{
				contrib(1, 7208, 0, 10, col(0, 1), col(1, 2)),
				contrib(2, 7202, 0, 10, col(1, 20), col(2, 30)),
			},
			want:  [mirror.Dests][]drained{{{10, 7, map[int]int64{0: 1, 1: 20, 2: 30}}}},
			stats: [5]uint64{2, 0, 1, 0, 0},
		},
		{
			name: "shared column: latest arrival wins even with an older stamp",
			in: []mirror.Contribution{
				contrib(9, 7208, 0, 10, col(1, 900)),
				contrib(3, 7202, 0, 10, col(1, 300)),
			},
			want:  [mirror.Dests][]drained{{{10, 2, map[int]int64{1: 300}}}},
			stats: [5]uint64{2, 0, 1, 0, 0},
		},
		{
			name: "older stamp from the same transcode is stale",
			in: []mirror.Contribution{
				contrib(5, 7208, 0, 10, col(0, 50)),
				contrib(4, 7208, 0, 10, col(0, 40)),
			},
			want:  [mirror.Dests][]drained{{{10, 1, map[int]int64{0: 50}}}},
			stats: [5]uint64{1, 1, 1, 0, 0},
		},
		{
			name: "one record's stamp applies to each row it touches",
			in: []mirror.Contribution{
				contrib(5, 7208, 0, 10, col(0, 1)),
				contrib(5, 7208, 1, 10, col(0, 2)),
				contrib(5, 7208, 0, 10, col(1, 3)),
			},
			want: [mirror.Dests][]drained{
				{{10, 3, map[int]int64{0: 1, 1: 3}}},
				{{10, 1, map[int]int64{0: 2}}},
			},
			stats: [5]uint64{3, 0, 2, 0, 0},
		},
		{
			name: "dirty rows drain first-dirtied first",
			in: []mirror.Contribution{
				contrib(1, 7208, 0, 30, col(0, 1)),
				contrib(2, 7208, 0, 20, col(0, 2)),
				contrib(3, 7208, 0, 30, col(0, 3)),
			},
			want: [mirror.Dests][]drained{{
				{30, 1, map[int]int64{0: 3}},
				{20, 1, map[int]int64{0: 2}},
			}},
			stats: [5]uint64{3, 0, 2, 0, 0},
		},
		{
			name:  "unknown destination is dropped",
			in:    []mirror.Contribution{contrib(1, 7208, mirror.Dests, 10, col(0, 1))},
			stats: [5]uint64{0, 0, 0, 1, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := new(mirror.Stats)
			m := mirror.New(st)
			for i := range tt.in {
				m.Apply(&tt.in[i])
			}
			all := flushAll(m)
			for d := range mirror.Dests {
				got := all[d]
				if !reflect.DeepEqual(got, tt.want[d]) && !(len(got) == 0 && len(tt.want[d]) == 0) {
					t.Errorf("dest %d drained %+v, want %+v", d, got, tt.want[d])
				}
			}
			s := [5]uint64{st.Applied.Load(), st.Stale.Load(), st.NewRows.Load(), st.BadDest.Load(), st.SlotsFull.Load()}
			if s != tt.stats {
				t.Errorf("(Applied, Stale, NewRows, BadDest, SlotsFull) = %v, want %v", s, tt.stats)
			}
		})
	}
}

func TestMirrorDrainResetsDirty(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	a, b := contrib(1, 7208, 0, 10, col(0, 1)), contrib(2, 7208, 0, 11, col(0, 2))
	m.Apply(&a)
	m.Apply(&b)
	if got := drainAll(m, 0); len(got) != 2 {
		t.Fatalf("first drain got %d rows, want 2", len(got))
	}
	if got := drainAll(m, 0); len(got) != 0 {
		t.Fatalf("second drain got %+v, want nothing dirty", got)
	}

	c := contrib(3, 7208, 0, 11, col(1, 9))
	m.Apply(&c)
	want := []drained{{11, 3, map[int]int64{0: 2, 1: 9}}}
	if got := drainAll(m, 0); !reflect.DeepEqual(got, want) {
		t.Errorf("re-dirtied row drained %+v, want %+v (image kept across drains)", got, want)
	}
}

// Stale state survives a drain: the row is clean, but the stamp is remembered.
func TestMirrorStaleAfterDrain(t *testing.T) {
	st := new(mirror.Stats)
	m := mirror.New(st)
	a, b := contrib(5, 7208, 0, 10, col(0, 5)), contrib(4, 7208, 0, 10, col(0, 4))
	m.Apply(&a)
	drainAll(m, 0)
	m.Apply(&b)
	if got := drainAll(m, 0); len(got) != 0 || st.Stale.Load() != 1 {
		t.Errorf("drained %+v, Stale = %d; want nothing and 1", got, st.Stale.Load())
	}
}

func TestMirrorSlotsFull(t *testing.T) {
	st := new(mirror.Stats)
	m := mirror.New(st)
	const codes = 9 // one more than a row's transcode slots
	for i := range codes {
		c := contrib(uint64(i+1), uint16(7000+i), 0, 10, col(i, int64(i)))
		m.Apply(&c)
	}
	if n := st.SlotsFull.Load(); n != 1 {
		t.Errorf("SlotsFull = %d, want 1", n)
	}
	got := drainAll(m, 0)
	if len(got) != 1 || got[0].cols[codes-1] != codes-1 {
		t.Errorf("drained %+v, want the overflowing transcode's column applied", got)
	}
}

// Applying to a row that already exists is the hot path and must not
// allocate (ADR-006).
func TestMirrorApplyAllocatesNothing(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	c := contrib(1, 7208, 0, 10, col(0, 1), col(5, 2))
	m.Apply(&c)
	allocs := testing.AllocsPerRun(100, func() {
		c.Seq++
		m.Apply(&c)
	})
	if allocs != 0 {
		t.Errorf("Apply allocates %.0f times, want 0", allocs)
	}
}

func TestRowIsPointerFree(t *testing.T) {
	assertPointerFree(t, "Row", reflect.TypeOf(mirror.Row{}))
}

// Apply reports whether it merged, so a shard can tell when a flush is
// warranted.
func TestMirrorApplyReportsMerge(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	steps := []struct {
		name string
		c    mirror.Contribution
		want bool
	}{
		{"new row", contrib(5, 6511, 0, 1, col(0, 1)), true},
		{"newer stamp", contrib(6, 6511, 0, 1, col(0, 2)), true},
		{"stale stamp", contrib(4, 6511, 0, 1, col(0, 3)), false},
		{"bad destination", contrib(7, 6511, mirror.Dests, 1, col(0, 4)), false},
	}
	for _, s := range steps {
		if got := m.Apply(&s.c); got != s.want {
			t.Errorf("%s: Apply = %v, want %v", s.name, got, s.want)
		}
	}
}
