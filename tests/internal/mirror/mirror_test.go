package mirror_test

import (
	"reflect"
	"testing"

	"feedvalve/internal/mirror"
)

// contrib builds a contribution at arrival seq from transcode code for token,
// whose quote carries v as its last traded price.
func contrib(seq uint64, code uint16, token int32, v int64) mirror.Contribution {
	return mirror.Contribution{Seq: seq, Code: code, Token: token, Quote: mirror.Quote{LastTradedPrice: v}}
}

// drained is one row as a batch carried it: its token and last traded price.
type drained struct {
	token int32
	ltp   int64
}

// flushAll runs one whole epoch in a single batch that commits, and returns
// the rows written, in batch order.
func flushAll(m *mirror.Mirror) []drained {
	var got []drained
	if !m.OpenEpoch() {
		return got
	}
	b := mirror.NewBatch(0, 1<<12)
	m.Fill(b, 1<<12)
	for _, r := range b.Rows {
		got = append(got, drained{r.Token, r.Quote.LastTradedPrice})
	}
	m.Complete(b)
	return got
}

func TestMirrorApply(t *testing.T) {
	tests := []struct {
		name  string
		in    []mirror.Contribution
		want  []drained
		stats [3]uint64 // Applied, Stale, NewRows
	}{
		{
			name:  "new row is created and dirtied",
			in:    []mirror.Contribution{contrib(1, 7208, 10, 5)},
			want:  []drained{{10, 5}},
			stats: [3]uint64{1, 0, 1},
		},
		{
			name:  "updates to one row coalesce, latest wins",
			in:    []mirror.Contribution{contrib(1, 7208, 10, 5), contrib(2, 7208, 10, 7)},
			want:  []drained{{10, 7}},
			stats: [3]uint64{2, 0, 1},
		},
		{
			name:  "older stamp is stale",
			in:    []mirror.Contribution{contrib(5, 7208, 10, 50), contrib(4, 7208, 10, 40)},
			want:  []drained{{10, 50}},
			stats: [3]uint64{1, 1, 1},
		},
		{
			name:  "equal stamp is applied",
			in:    []mirror.Contribution{contrib(5, 7208, 10, 50), contrib(5, 7208, 10, 51)},
			want:  []drained{{10, 51}},
			stats: [3]uint64{2, 0, 1},
		},
		{
			name:  "tokens are independent",
			in:    []mirror.Contribution{contrib(5, 7208, 10, 50), contrib(4, 7208, 11, 40)},
			want:  []drained{{10, 50}, {11, 40}},
			stats: [3]uint64{2, 0, 2},
		},
		{
			name:  "dirty rows drain first-dirtied first",
			in:    []mirror.Contribution{contrib(1, 7208, 30, 1), contrib(2, 7208, 20, 2), contrib(3, 7208, 30, 3)},
			want:  []drained{{30, 3}, {20, 2}},
			stats: [3]uint64{3, 0, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := new(mirror.Stats)
			m := mirror.New(st)
			for i := range tt.in {
				m.Apply(&tt.in[i])
			}
			if got := flushAll(m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("drained %+v, want %+v", got, tt.want)
			}
			s := [3]uint64{st.Applied.Load(), st.Stale.Load(), st.NewRows.Load()}
			if s != tt.stats {
				t.Errorf("(Applied, Stale, NewRows) = %v, want %v", s, tt.stats)
			}
		})
	}
}

// The whole quote is replaced, not merged field by field.
func TestMirrorApplyReplacesQuote(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	a := mirror.Contribution{Seq: 1, Token: 1, Quote: mirror.Quote{LastTradedPrice: 1, OpenPrice: 2, BuyPrice: [5]int64{3}}}
	b := mirror.Contribution{Seq: 2, Token: 1, Quote: mirror.Quote{LastTradedPrice: 9}}
	m.Apply(&a)
	m.Apply(&b)
	m.OpenEpoch()
	bt := mirror.NewBatch(0, 4)
	m.Fill(bt, 4)
	if len(bt.Rows) != 1 || bt.Rows[0].Quote != b.Quote {
		t.Errorf("batch %+v, want one row holding the second quote %+v", bt.Rows, b.Quote)
	}
}

func TestMirrorDrainResetsDirty(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	a, b := contrib(1, 7208, 10, 1), contrib(2, 7208, 11, 2)
	m.Apply(&a)
	m.Apply(&b)
	if got := flushAll(m); len(got) != 2 {
		t.Fatalf("first drain got %d rows, want 2", len(got))
	}
	if got := flushAll(m); len(got) != 0 {
		t.Fatalf("second drain got %+v, want nothing dirty", got)
	}
	c := contrib(3, 7208, 11, 9)
	m.Apply(&c)
	if got, want := flushAll(m), []drained{{11, 9}}; !reflect.DeepEqual(got, want) {
		t.Errorf("re-dirtied row drained %+v, want %+v", got, want)
	}
}

// Stale state survives a drain: the row is clean, but the stamp is remembered.
func TestMirrorStaleAfterDrain(t *testing.T) {
	st := new(mirror.Stats)
	m := mirror.New(st)
	a, b := contrib(5, 7208, 10, 5), contrib(4, 7208, 10, 4)
	m.Apply(&a)
	flushAll(m)
	m.Apply(&b)
	if got := flushAll(m); len(got) != 0 || st.Stale.Load() != 1 {
		t.Errorf("drained %+v, Stale = %d; want nothing and 1", got, st.Stale.Load())
	}
}

// Applying to a row that already exists is the hot path and must not
// allocate (ADR-006).
func TestMirrorApplyAllocatesNothing(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	c := contrib(1, 7208, 10, 1)
	m.Apply(&c)
	allocs := testing.AllocsPerRun(100, func() {
		c.Seq++
		m.Apply(&c)
	})
	if allocs != 0 {
		t.Errorf("Apply allocates %.0f times, want 0", allocs)
	}
}

func TestQuoteIsPointerFree(t *testing.T) {
	assertPointerFree(t, "Quote", reflect.TypeOf(mirror.Quote{}))
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
		{"new row", contrib(5, 6511, 1, 1), true},
		{"newer stamp", contrib(6, 6511, 1, 2), true},
		{"stale stamp", contrib(4, 6511, 1, 3), false},
	}
	for _, s := range steps {
		if got := m.Apply(&s.c); got != s.want {
			t.Errorf("%s: Apply = %v, want %v", s.name, got, s.want)
		}
	}
}
