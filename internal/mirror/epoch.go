package mirror

import "slices"

// BatchRow is one row in a batch: its token, copies of both parts taken when
// it was batched, and which parts changed since it was last batched; only
// those are written. Pointer-free (ADR-006).
type BatchRow struct {
	Token     int32  // ABC_TOKEN_NO, GHI_TOKEN_NO
	Quote     Quote  // the quote values
	Master    Master // the master values
	HasQuote  bool   // write Quote
	HasMaster bool   // write Master
}

// Batch is one transaction's worth of rows. Batches are made once per shard
// at startup and reused; the shard fills one, a writer commits it, and the
// shard takes it back.
type Batch struct {
	Shard int        // the owning shard, to which the writer returns it
	Rows  []BatchRow // the rows; capacity fixed by NewBatch
	Err   error      // set by the writer: nil when the batch committed
}

// NewBatch returns an empty batch owned by shard, with room for maxRows rows.
func NewBatch(shard, maxRows int) *Batch {
	return &Batch{Shard: shard, Rows: make([]BatchRow, 0, maxRows)}
}

// OpenEpoch starts an epoch: every row now on the accumulating ring becomes
// the epoch's cohort, and rows dirtied from here on wait for the next one. It
// returns false, changing nothing, when no row is dirty. Call it only when the
// previous epoch's cohort has been fully batched.
func (m *Mirror) OpenEpoch() bool {
	if len(m.dirty) == 0 {
		return false
	}
	m.dirty, m.cohort = m.cohort[:0], m.dirty
	m.head = 0
	return true
}

// Cohort returns how many rows of the open epoch have not been batched yet.
func (m *Mirror) Cohort() int {
	return len(m.cohort) - m.head
}

// Dirty returns how many rows are waiting for the next epoch.
func (m *Mirror) Dirty() int {
	return len(m.dirty)
}

// Pending returns the rows not yet written and not in a batch: those waiting
// for the next epoch plus the open epoch's unbatched rows.
func (m *Mirror) Pending() int {
	return m.Dirty() + m.Cohort()
}

// Fill empties b and refills it with up to maxRows of the cohort's rows,
// oldest first. Each row's parts are copied into b with their changed flags,
// which are then cleared, and the row is marked in flight until Complete.
func (m *Mirror) Fill(b *Batch, maxRows int) {
	var (
		e *entry // row being batched
	)

	b.Rows = b.Rows[:0]
	b.Err = nil
	for len(b.Rows) < maxRows && m.head < len(m.cohort) {
		e = &m.rows[m.cohort[m.head]]
		m.head++
		e.dirty = false
		e.inFlight = true
		b.Rows = append(b.Rows, BatchRow{Token: e.token, Quote: e.quote, Master: e.master,
			HasQuote: e.quotePending, HasMaster: e.masterPending})
		e.quotePending = false
		e.masterPending = false
	}
	if m.head == len(m.cohort) {
		m.cohort = m.cohort[:0]
		m.head = 0
	}
}

// Complete takes back a batch the writer has finished with. On success, rows
// merged while in flight rejoin the back of the accumulating ring. On failure
// (b.Err set), every row goes back to the head of the accumulating ring, in
// batch order, with the parts it carried marked changed again, so the next
// epoch retries it before anything dirtied later.
func (m *Mirror) Complete(b *Batch) {
	var (
		e   *entry // the row
		pos int32  // the row's position in m.rows
		ins int    // next head-of-ring insertion point
	)

	for i := range b.Rows {
		pos = m.index[b.Rows[i].Token]
		e = &m.rows[pos]
		e.inFlight = false
		switch {
		case b.Err != nil:
			e.quotePending = e.quotePending || b.Rows[i].HasQuote
			e.masterPending = e.masterPending || b.Rows[i].HasMaster
			e.reDirty = false
			e.dirty = true
			m.dirty = slices.Insert(m.dirty, ins, pos)
			ins++
			m.st.Requeued.Add(1)
		case e.reDirty:
			e.reDirty = false
			e.dirty = true
			m.dirty = append(m.dirty, pos)
		}
	}
}
