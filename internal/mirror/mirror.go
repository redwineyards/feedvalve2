package mirror

import (
	"log/slog"
	"math/bits"
	"sync/atomic"
)

// Dests is the number of destination tables; Contribution.Dest is 0..Dests-1.
const Dests = 3

// maxContributors is how many transcodes can write one row before the stale
// check stops tracking new ones (HLD §6.5 needs at least five).
const maxContributors = 8

// Stats counts what the mirrors have done. One Stats is shared by every
// shard's Mirror, so it is safe to read concurrently.
type Stats struct {
	Applied   atomic.Uint64 // contributions merged into a row
	Stale     atomic.Uint64 // contributions older than the last applied for their (row, transcode); dropped
	NewRows   atomic.Uint64 // rows created
	BadDest   atomic.Uint64 // contributions naming no destination; dropped
	SlotsFull atomic.Uint64 // contributions applied without a stale check: their row had no free transcode slot
	Requeued  atomic.Uint64 // rows whose batch failed, put back at the head of the next epoch
}

// Row is one destination row's coalesced image: which columns have ever been
// set, and their latest values. Only columns whose bit is set in Mask are
// meaningful.
type Row struct {
	Mask uint64 // bit i set: Cols[i] holds a value
	Cols [MaxCols]int64
}

// slot remembers the newest contribution one transcode has applied to a row.
type slot struct {
	used  bool   // whether the slot belongs to a transcode yet
	code  uint16 // the transcode
	stamp uint64 // Stamp of its newest applied contribution
}

// entry is a row together with its bookkeeping. Pointer-free, so the arena
// holding it is never scanned by the GC (ADR-006). A row is clean, dirty (on
// the accumulating ring or the cohort) or in flight (in a batch a writer
// holds), never two at once.
type entry struct {
	key      int64                 // the row key
	row      Row                   // the coalesced image
	dirty    bool                  // waiting to be batched
	inFlight bool                  // in a batch not yet completed
	reDirty  bool                  // merged again while in flight; dirty once the batch completes
	slots    [maxContributors]slot // stale-check state per contributing transcode
}

// table is one destination's rows: an arena of entries indexed by row key,
// and the positions of the dirty ones in the order they became dirty. The
// accumulating ring collects rows for the next epoch; the cohort is the open
// epoch's snapshot, drained from head and never appended to (ADR-018).
type table struct {
	index  map[int64]int32 // row key → position in rows
	rows   []entry         // the arena
	dirty  []int32         // accumulating ring: dirty rows, first-dirtied first
	cohort []int32         // the open epoch's rows, oldest first
	head   int             // next cohort row to batch
}

// Mirror is one shard's coalesced copy of its rows in every destination.
// Exactly one goroutine owns it; it is not safe for concurrent use.
type Mirror struct {
	tables [Dests]table // one per destination
	st     *Stats       // shared counters
}

// New returns an empty mirror that counts into st.
func New(st *Stats) *Mirror {
	var (
		m *Mirror // the new mirror
	)

	m = &Mirror{st: st}
	for i := range m.tables {
		m.tables[i].index = make(map[int64]int32)
	}
	return m
}

// Apply merges c into its row: the columns c sets overwrite the row's, the
// rest are kept, and the row joins the accumulating ring unless it is already
// dirty. A row in flight is only marked to rejoin once its batch completes,
// so no two batches ever hold the same row.
// A contribution older than the last one applied for the same (row,
// transcode) is dropped as stale; across transcodes, the latest to arrive
// wins. Apply reports whether c was merged.
func (m *Mirror) Apply(c *Contribution) bool {
	var (
		t     *table // c's destination
		pos   int32  // the row's position in t.rows
		e     *entry // the row
		s     *slot  // c's transcode slot on the row, nil if none was free
		stamp uint64 // c's arrival position
		mask  uint64 // columns of c still to copy
		i     int    // column being copied
		ok    bool   // whether the row already existed
	)

	if c.Dest >= Dests {
		m.st.BadDest.Add(1)
		slog.Warn("contribution names no destination, dropped", "dest", c.Dest, "code", c.Code, "token", c.Token)
		return false
	}
	t = &m.tables[c.Dest]
	pos, ok = t.index[c.RowKey]
	if !ok {
		pos = int32(len(t.rows))
		t.rows = append(t.rows, entry{key: c.RowKey})
		t.index[c.RowKey] = pos
		m.st.NewRows.Add(1)
	}
	e = &t.rows[pos]

	stamp = c.Stamp()
	s = findSlot(e, c.Code)
	if s == nil {
		m.st.SlotsFull.Add(1)
		slog.Warn("row has no free transcode slot, applied without stale check", "dest", c.Dest, "row", c.RowKey, "code", c.Code)
	} else {
		if s.used && stamp < s.stamp {
			m.st.Stale.Add(1)
			return false
		}
		*s = slot{used: true, code: c.Code, stamp: stamp}
	}

	for mask = c.Mask; mask != 0; mask &= mask - 1 {
		i = bits.TrailingZeros64(mask)
		e.row.Cols[i] = c.Cols[i]
	}
	e.row.Mask |= c.Mask
	switch {
	case e.inFlight:
		e.reDirty = true
	case !e.dirty:
		e.dirty = true
		t.dirty = append(t.dirty, pos)
	}
	m.st.Applied.Add(1)
	return true
}

// findSlot returns the slot e keeps for transcode code, or a free one, or nil
// if every slot belongs to another transcode.
func findSlot(e *entry, code uint16) *slot {
	var (
		free *slot // first unused slot seen
	)

	for i := range e.slots {
		if !e.slots[i].used {
			if free == nil {
				free = &e.slots[i]
			}
			continue
		}
		if e.slots[i].code == code {
			return &e.slots[i]
		}
	}
	return free
}
