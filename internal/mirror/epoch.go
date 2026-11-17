package mirror

import "slices"

// BatchRow is one row in a batch: its destination, key and a copy of its
// image taken when it was batched. Pointer-free (ADR-006).
type BatchRow struct {
	Dest uint8 // destination table
	Key  int64 // row key within Dest
	Row  Row   // the image to write
}

// Batch is one transaction's worth of rows across all three destinations,
// destination 0's rows first, then 1, then 2 (HLD §7.3). Batches are made
// once per shard at startup and reused; the shard fills one, a writer
// commits it, and the shard takes it back.
type Batch struct {
	Shard int        // the owning shard, to which the writer returns it
	Rows  []BatchRow // the rows; capacity fixed by NewBatch
	Err   error      // set by the writer: nil when the batch committed
}

// NewBatch returns an empty batch owned by shard, with room for maxRows rows.
func NewBatch(shard, maxRows int) *Batch {
	return &Batch{Shard: shard, Rows: make([]BatchRow, 0, maxRows)}
}

// OpenEpoch starts an epoch: every row now on an accumulating ring becomes the
// epoch's cohort, and rows dirtied from here on wait for the next one. It
// returns false, changing nothing, when no row is dirty. Call it only when the
// previous epoch's cohort has been fully batched.
func (m *Mirror) OpenEpoch() bool {
	var (
		t *table // destination being snapshotted
	)

	if m.Dirty() == 0 {
		return false
	}
	for i := range m.tables {
		t = &m.tables[i]
		t.dirty, t.cohort = t.cohort[:0], t.dirty
		t.head = 0
	}
	return true
}

// Cohort returns how many rows of the open epoch have not been batched yet.
func (m *Mirror) Cohort() int {
	var (
		n int // running total
	)

	for i := range m.tables {
		n += len(m.tables[i].cohort) - m.tables[i].head
	}
	return n
}

// Dirty returns how many rows are waiting for the next epoch.
func (m *Mirror) Dirty() int {
	var (
		n int // running total
	)

	for i := range m.tables {
		n += len(m.tables[i].dirty)
	}
	return n
}

// Pending returns, per destination, the rows not yet written and not in a
// batch: those waiting for the next epoch plus the open epoch's unbatched
// rows.
func (m *Mirror) Pending() [Dests]int {
	var (
		n [Dests]int // the result
	)

	for i := range m.tables {
		n[i] = len(m.tables[i].dirty) + len(m.tables[i].cohort) - m.tables[i].head
	}
	return n
}

// Fill empties b and refills it with up to maxRows of the cohort's rows,
// destination 0 first, oldest first within each. Each row's image is copied
// into b and the row is marked in flight until Complete.
func (m *Mirror) Fill(b *Batch, maxRows int) {
	var (
		t *table // destination being drawn from
		e *entry // row being batched
	)

	b.Rows = b.Rows[:0]
	b.Err = nil
	for d := range m.tables {
		t = &m.tables[d]
		for len(b.Rows) < maxRows && t.head < len(t.cohort) {
			e = &t.rows[t.cohort[t.head]]
			t.head++
			e.dirty = false
			e.inFlight = true
			b.Rows = append(b.Rows, BatchRow{Dest: uint8(d), Key: e.key, Row: e.row})
		}
		if t.head == len(t.cohort) {
			t.cohort = t.cohort[:0]
			t.head = 0
		}
	}
}

// Complete takes back a batch the writer has finished with. On success, rows
// merged while in flight rejoin the back of the accumulating ring. On failure
// (b.Err set), every row goes back to the head of its accumulating ring, in
// batch order, so the next epoch retries it before anything dirtied later.
func (m *Mirror) Complete(b *Batch) {
	var (
		t   *table     // the row's destination
		e   *entry     // the row
		pos int32      // the row's position in t.rows
		ins [Dests]int // next head-of-ring insertion point per destination
	)

	for i := range b.Rows {
		t = &m.tables[b.Rows[i].Dest]
		pos = t.index[b.Rows[i].Key]
		e = &t.rows[pos]
		e.inFlight = false
		switch {
		case b.Err != nil:
			e.reDirty = false
			e.dirty = true
			t.dirty = slices.Insert(t.dirty, ins[b.Rows[i].Dest], pos)
			ins[b.Rows[i].Dest]++
			m.st.Requeued.Add(1)
		case e.reDirty:
			e.reDirty = false
			e.dirty = true
			t.dirty = append(t.dirty, pos)
		}
	}
}
