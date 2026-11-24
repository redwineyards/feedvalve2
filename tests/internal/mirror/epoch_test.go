package mirror_test

import (
	"errors"
	"reflect"
	"testing"

	"feedvalve/internal/mirror"
)

// apply applies each contribution to m in order.
func apply(m *mirror.Mirror, cs ...mirror.Contribution) {
	for i := range cs {
		m.Apply(&cs[i])
	}
}

// tokens lists the token of every row in b, in batch order.
func tokens(b *mirror.Batch) []int32 {
	var got []int32
	for _, r := range b.Rows {
		got = append(got, r.Token)
	}
	return got
}

func TestOpenEpochNothingDirty(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	if m.OpenEpoch() {
		t.Error("OpenEpoch on an empty mirror = true, want false")
	}
	apply(m, contrib(1, 7208, 1, 1))
	flushAll(m)
	if m.OpenEpoch() {
		t.Error("OpenEpoch after everything was written = true, want false")
	}
}

func TestFillBudget(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	apply(m, contrib(1, 7208, 21, 1), contrib(2, 7208, 11, 1), contrib(3, 7208, 12, 1),
		contrib(4, 7208, 22, 1), contrib(5, 7208, 13, 1))
	if !m.OpenEpoch() {
		t.Fatal("OpenEpoch = false with 5 dirty rows")
	}
	if n := m.Cohort(); n != 5 {
		t.Fatalf("Cohort() = %d, want 5", n)
	}
	b := mirror.NewBatch(3, 4)
	m.Fill(b, 4)
	if want := []int32{21, 11, 12, 22}; !reflect.DeepEqual(tokens(b), want) {
		t.Errorf("first batch = %v, want %v", tokens(b), want)
	}
	if b.Shard != 3 {
		t.Errorf("batch Shard = %d, want 3", b.Shard)
	}
	if n := m.Cohort(); n != 1 {
		t.Errorf("Cohort() after first batch = %d, want 1", n)
	}
	b2 := mirror.NewBatch(3, 4)
	m.Fill(b2, 4)
	if want := []int32{13}; !reflect.DeepEqual(tokens(b2), want) {
		t.Errorf("second batch = %v, want %v", tokens(b2), want)
	}
	if n := m.Cohort(); n != 0 {
		t.Errorf("Cohort() after second batch = %d, want 0", n)
	}
}

func TestEpochMergesDuringFlush(t *testing.T) {
	t.Run("row dirtied mid-epoch waits for the next epoch", func(t *testing.T) {
		m := mirror.New(new(mirror.Stats))
		apply(m, contrib(1, 7208, 1, 1))
		m.OpenEpoch()
		apply(m, contrib(2, 7208, 2, 2))
		if c, d := m.Cohort(), m.Dirty(); c != 1 || d != 1 {
			t.Fatalf("(Cohort, Dirty) = (%d, %d), want (1, 1)", c, d)
		}
		b := mirror.NewBatch(0, 10)
		m.Fill(b, 10)
		if want := []int32{1}; !reflect.DeepEqual(tokens(b), want) {
			t.Errorf("epoch carried %v, want only %v", tokens(b), want)
		}
		m.Complete(b)
		if got := flushAll(m); len(got) != 1 || got[0].token != 2 {
			t.Errorf("next epoch carried %+v, want row 2", got)
		}
	})

	t.Run("cohort row merged before batching goes out once, newest", func(t *testing.T) {
		m := mirror.New(new(mirror.Stats))
		apply(m, contrib(1, 7208, 1, 1))
		m.OpenEpoch()
		apply(m, contrib(2, 7208, 1, 9))
		b := mirror.NewBatch(0, 10)
		m.Fill(b, 10)
		if len(b.Rows) != 1 || b.Rows[0].Quote.LastTradedPrice != 9 || m.Dirty() != 0 {
			t.Errorf("batch %v, Dirty %d; want one row with the newest value and nothing left dirty", tokens(b), m.Dirty())
		}
	})

	t.Run("batch is a copy and an in-flight row is not batched twice", func(t *testing.T) {
		m := mirror.New(new(mirror.Stats))
		apply(m, contrib(1, 7208, 1, 1))
		m.OpenEpoch()
		b := mirror.NewBatch(0, 10)
		m.Fill(b, 10)

		apply(m, contrib(2, 7208, 1, 2))
		if v := b.Rows[0].Quote.LastTradedPrice; v != 1 {
			t.Errorf("batch row changed to %d after a merge, want the batched 1", v)
		}
		if d := m.Dirty(); d != 0 {
			t.Errorf("Dirty() = %d while the row is in flight, want 0", d)
		}
		if m.OpenEpoch() {
			t.Error("OpenEpoch = true with the only row in flight, want false")
		}

		m.Complete(b)
		if d := m.Dirty(); d != 1 {
			t.Fatalf("Dirty() after completion = %d, want the re-merged row back", d)
		}
		if got := flushAll(m); len(got) != 1 || got[0].ltp != 2 {
			t.Errorf("next epoch carried %+v, want row 1 with value 2", got)
		}
	})
}

func TestCompleteFailureRequeuesAtHead(t *testing.T) {
	st := new(mirror.Stats)
	m := mirror.New(st)
	apply(m, contrib(1, 7208, 1, 1), contrib(2, 7208, 2, 2))
	m.OpenEpoch()
	b := mirror.NewBatch(0, 10)
	m.Fill(b, 10)

	apply(m, contrib(3, 7208, 3, 3)) // dirtied during the epoch
	apply(m, contrib(4, 7208, 2, 4)) // re-merged while in flight
	b.Err = errors.New("commit failed")
	m.Complete(b)

	if n := st.Requeued.Load(); n != 2 {
		t.Errorf("Requeued = %d, want 2", n)
	}
	want := []drained{{1, 1}, {2, 4}, {3, 3}}
	if got := flushAll(m); !reflect.DeepEqual(got, want) {
		t.Errorf("next epoch = %+v, want %+v (failed rows first, in batch order, newest values)", got, want)
	}
	if m.Dirty() != 0 {
		t.Errorf("Dirty() = %d after the retry committed, want 0", m.Dirty())
	}
}

func TestPending(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	apply(m, contrib(1, 7208, 1, 1), contrib(2, 7208, 2, 1), contrib(3, 7208, 3, 1))
	if got := m.Pending(); got != 3 {
		t.Errorf("Pending() before the epoch = %d, want 3", got)
	}
	m.OpenEpoch()
	b := mirror.NewBatch(0, 1)
	m.Fill(b, 1)                     // row 1 in flight
	apply(m, contrib(4, 7208, 5, 1)) // dirtied mid-epoch
	if got := m.Pending(); got != 3 {
		t.Errorf("Pending() mid-epoch = %d, want 3 (in-flight rows excluded)", got)
	}
}

// A steady-state epoch over existing rows must not allocate (ADR-006).
func TestEpochAllocatesNothing(t *testing.T) {
	m := mirror.New(new(mirror.Stats))
	cs := []mirror.Contribution{contrib(1, 7208, 1, 1), contrib(1, 7208, 2, 1), contrib(1, 7208, 3, 1)}
	b := mirror.NewBatch(0, 8)
	run := func() {
		for i := range cs {
			cs[i].Seq++
			m.Apply(&cs[i])
		}
		m.OpenEpoch()
		m.Fill(b, 8)
		m.Complete(b)
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Errorf("an epoch allocates %.0f times, want 0", allocs)
	}
}

func TestBatchRowIsPointerFree(t *testing.T) {
	assertPointerFree(t, "BatchRow", reflect.TypeOf(mirror.BatchRow{}))
}
