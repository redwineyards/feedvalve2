package mirror

import "sync/atomic"

// Stats counts what the mirrors have done. One Stats is shared by every
// shard's Mirror, so it is safe to read concurrently.
type Stats struct {
	Applied  atomic.Uint64 // contributions merged into a row
	Stale    atomic.Uint64 // contributions older than the last applied to the same part of their token; dropped
	NewRows  atomic.Uint64 // rows created
	Requeued atomic.Uint64 // rows whose batch failed, put back at the head of the next epoch
}

// entry is a row together with its bookkeeping. Pointer-free, so the arena
// holding it is never scanned by the GC (ADR-006). A row is clean, dirty (on
// the accumulating ring or the cohort) or in flight (in a batch a writer
// holds), never two at once.
type entry struct {
	token         int32  // the row key
	quote         Quote  // the latest quote
	master        Master // the latest master
	quoteStamp    uint64 // Stamp of the newest applied quote
	masterStamp   uint64 // Stamp of the newest applied master
	quotePending  bool   // quote changed since it was last batched
	masterPending bool   // master changed since it was last batched
	dirty         bool   // waiting to be batched
	inFlight      bool   // in a batch not yet completed
	reDirty       bool   // merged again while in flight; dirty once the batch completes
}

// Mirror is one shard's coalesced copy of its rows: an arena of entries
// indexed by token, and the positions of the dirty ones in the order they
// became dirty. The accumulating ring collects rows for the next epoch; the
// cohort is the open epoch's snapshot, drained from head and never appended
// to (ADR-018). Exactly one goroutine owns it; it is not safe for concurrent
// use.
type Mirror struct {
	index  map[int32]int32 // token → position in rows
	rows   []entry         // the arena
	dirty  []int32         // accumulating ring: dirty rows, first-dirtied first
	cohort []int32         // the open epoch's rows, oldest first
	head   int             // next cohort row to batch
	st     *Stats          // shared counters
}

// New returns an empty mirror that counts into st.
func New(st *Stats) *Mirror {
	return &Mirror{index: make(map[int32]int32), st: st}
}

// Apply replaces the part of c's token that c carries, and the row joins the
// accumulating ring unless it is already dirty. A row in flight is only
// marked to rejoin once its batch completes, so no two batches ever hold the
// same row. A contribution older than the last one applied to the same part
// of its token is dropped as stale; the parts come from different transcodes,
// so their stamps are not compared with each other. Apply reports whether c
// was merged.
func (m *Mirror) Apply(c *Contribution) bool {
	var (
		pos   int32  // the row's position in m.rows
		e     *entry // the row
		stamp uint64 // c's arrival position
		last  uint64 // stamp of the newest applied contribution to c's part
		ok    bool   // whether the row already existed
	)

	stamp = c.Stamp()
	pos, ok = m.index[c.Token]
	if !ok {
		pos = int32(len(m.rows))
		m.rows = append(m.rows, entry{token: c.Token})
		m.index[c.Token] = pos
		m.st.NewRows.Add(1)
	}
	e = &m.rows[pos]
	last = e.quoteStamp
	if c.Part == PartMaster {
		last = e.masterStamp
	}
	if ok && stamp < last {
		m.st.Stale.Add(1)
		return false
	}

	if c.Part == PartMaster {
		e.masterStamp = stamp
		e.master = c.Master
		e.masterPending = true
	} else {
		e.quoteStamp = stamp
		e.quote = c.Quote
		e.quotePending = true
	}
	switch {
	case e.inFlight:
		e.reDirty = true
	case !e.dirty:
		e.dirty = true
		m.dirty = append(m.dirty, pos)
	}
	m.st.Applied.Add(1)
	return true
}
