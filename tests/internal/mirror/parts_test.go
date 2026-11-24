package mirror_test

import (
	"errors"
	"reflect"
	"testing"

	"feedvalve/internal/mirror"
)

// master builds a 7305 contribution at arrival seq for token, whose master
// part carries base as its base price.
func master(seq uint64, token int32, base int64) mirror.Contribution {
	return mirror.Contribution{Seq: seq, Code: 7305, Token: token, Part: mirror.PartMaster, Master: mirror.Master{BasePrice: base}}
}

// part is one batch row reduced to what these tests compare.
type part struct {
	token              int32
	hasQuote, hasMastr bool
	ltp, base          int64
}

// epoch runs one whole epoch in a single batch, completed with err, and
// returns its rows.
func epoch(m *mirror.Mirror, err error) []part {
	var got []part
	if !m.OpenEpoch() {
		return got
	}
	b := mirror.NewBatch(0, 64)
	m.Fill(b, 64)
	for _, r := range b.Rows {
		got = append(got, part{r.Token, r.HasQuote, r.HasMaster, r.Quote.LastTradedPrice, r.Master.BasePrice})
	}
	b.Err = err
	m.Complete(b)
	return got
}

// The quote and master parts of a row are separate per-transcode sequences:
// a newer 7208 never makes an older 7305 stale, and vice versa.
func TestPartsHaveSeparateStaleChecks(t *testing.T) {
	st := new(mirror.Stats)
	m := mirror.New(st)
	steps := []struct {
		name string
		c    mirror.Contribution
		want bool
	}{
		{"quote at 10", contrib(10, 7208, 1, 100), true},
		{"older master still applies", master(5, 1, 50), true},
		{"older master than the last master is stale", master(4, 1, 40), false},
		{"older quote than the last quote is stale", contrib(9, 7208, 1, 90), false},
	}
	for _, s := range steps {
		if got := m.Apply(&s.c); got != s.want {
			t.Errorf("%s: Apply = %v, want %v", s.name, got, s.want)
		}
	}
	if got, want := epoch(m, nil), []part{{1, true, true, 100, 50}}; !reflect.DeepEqual(got, want) {
		t.Errorf("batch = %+v, want %+v", got, want)
	}
	if n := st.Stale.Load(); n != 2 {
		t.Errorf("Stale = %d, want 2", n)
	}
}

// A batch row says which parts changed since the row was last written, so
// a token seen only in 7305 never writes the quote columns.
func TestFillFlagsOnlyChangedParts(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	apply(m, contrib(1, 7208, 1, 11), master(2, 2, 22), contrib(3, 7208, 3, 33), master(4, 3, 34))
	want := []part{{1, true, false, 11, 0}, {2, false, true, 0, 22}, {3, true, true, 33, 34}}
	if got := epoch(m, nil); !reflect.DeepEqual(got, want) {
		t.Errorf("first epoch = %+v, want %+v", got, want)
	}
	apply(m, master(5, 1, 15))
	// The quote is carried along unchanged, but only the master is flagged.
	if got, want := epoch(m, nil), []part{{1, false, true, 11, 15}}; !reflect.DeepEqual(got, want) {
		t.Errorf("second epoch = %+v, want %+v", got, want)
	}
}

// A part merged while its row is in flight is written by the next epoch, and
// only that part.
func TestPartMergedInFlight(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	apply(m, contrib(1, 7208, 1, 11))
	m.OpenEpoch()
	b := mirror.NewBatch(0, 4)
	m.Fill(b, 4)
	apply(m, master(2, 1, 22))
	m.Complete(b)
	if got, want := epoch(m, nil), []part{{1, false, true, 11, 22}}; !reflect.DeepEqual(got, want) {
		t.Errorf("next epoch = %+v, want %+v", got, want)
	}
}

// A failed batch puts back the parts it carried, merged with any that changed
// while it was in flight.
func TestFailedBatchRestoresParts(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	apply(m, contrib(1, 7208, 1, 11))
	m.OpenEpoch()
	b := mirror.NewBatch(0, 4)
	m.Fill(b, 4)
	apply(m, master(2, 1, 22))
	b.Err = errors.New("commit failed")
	m.Complete(b)
	if got, want := epoch(m, nil), []part{{1, true, true, 11, 22}}; !reflect.DeepEqual(got, want) {
		t.Errorf("retry = %+v, want %+v", got, want)
	}
}

func TestMasterIsPointerFree(t *testing.T) {
	assertPointerFree(t, "Master", reflect.TypeOf(mirror.Master{}))
}
