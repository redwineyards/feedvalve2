// Package shard runs the shard goroutines. Each one owns one mirror.Mirror
// exclusively and applies its queue to it in arrival order, so every row has
// a single writer (HLD ADR-002, ASM-15).
//
// Each shard also flushes its mirror in epochs (HLD §7.3, ADR-018). On the
// global tick it snapshots every dirty row and sends them to the writer pool
// in batches of at most MaxBatch rows, with at most MaxInFlight batches out
// at once. It never waits for a commit: completions come back on Done and
// drive the next batch, so the queue keeps draining while writers work.
// Rows dirtied during an epoch go to the next one. A tick that lands while an
// epoch is open is remembered, and the next epoch opens as soon as this one
// closes.
//
// The second trigger is the flush request (HLD §6.8, §7.4). When a shard
// merges a contribution from a non-rate-limited transcode, it raises a
// request on every shard, itself included, so every shard flushes now rather
// than at the next tick. A request is an edge, not a message: raising one
// that is already pending changes nothing. It is cleared only when an epoch
// actually opens. One that lands mid-epoch opens the next epoch the moment
// the current one closes; one that finds nothing dirty waits for the shard's
// next dirty row and flushes it at once.
package shard

import (
	"log/slog"
	"sync/atomic"
	"time"

	"feedvalve/internal/mirror"
)

// Stats counts what the shards have done. One Stats is shared by every
// shard, so it is safe to read concurrently.
type Stats struct {
	Epochs      atomic.Uint64 // epochs opened
	EpochEmpty  atomic.Uint64 // ticks that found nothing dirty
	TickSkipped atomic.Uint64 // ticks that landed while an epoch was open
	Batches     atomic.Uint64 // batches sent to the writers
	Abandoned   atomic.Uint64 // rows left unwritten when the shutdown drain timed out

	FlushTriggered atomic.Uint64 // non-rate-limited merges that raised a flush request
	FlushCoalesced atomic.Uint64 // raises that found a shard's request already pending
}

// Params is everything one shard needs.
type Params struct {
	ID           int                        // this shard's number; batches carry it
	In           <-chan mirror.Contribution // the shard's queue; closing it starts shutdown
	Tick         <-chan struct{}            // the global flush tick
	Mirror       *mirror.Mirror             // owned by this shard alone
	BatchQ       chan<- *mirror.Batch       // shared with every shard; capacity shards × MaxInFlight
	Done         chan *mirror.Batch         // this shard's completions; capacity MaxInFlight
	MaxBatch     int                        // rows per batch
	MaxInFlight  int                        // batches out at once
	DrainTimeout time.Duration              // how long the shutdown flush may take
	Stats        *Stats                     // shared counters

	Flush          <-chan struct{} // this shard's flush requests; capacity 1
	FlushAll       []chan struct{} // every shard's request channel, this one's included
	NonRateLimited map[uint16]bool // transcodes whose merges raise a request; read-only
}

// flusher is a shard's epoch state. Only the shard goroutine touches it.
type flusher struct {
	p          Params          // the shard's settings
	free       []*mirror.Batch // batches not out with a writer
	open       bool            // an epoch is open
	out        int             // batches out with writers
	outRows    int             // rows in those batches
	tickMissed bool            // a tick landed while the epoch was open
	flushReq   bool            // a flush request is waiting for an epoch to open
}

// Run applies contributions from p.In to p.Mirror and flushes it on every
// tick and flush request, until p.In is closed. It then flushes until the mirror is clean or
// p.DrainTimeout passes, and returns.
func Run(p Params) {
	var (
		f *flusher // the epoch state
	)

	// Allocations: the shard's batches, made once and reused (ADR-006).
	f = &flusher{p: p, free: make([]*mirror.Batch, 0, p.MaxInFlight)}
	for range p.MaxInFlight {
		f.free = append(f.free, mirror.NewBatch(p.ID, p.MaxBatch))
	}

	for {
		select {
		case c, ok := <-p.In:
			if !ok {
				f.shutdown()
				return
			}
			if p.Mirror.Apply(&c) {
				f.merged(c.Code)
			}
		case <-p.Tick:
			f.tick()
		case <-p.Flush:
			f.flushReq = true
			if !f.open {
				f.openEpoch()
			}
		case b := <-p.Done:
			f.complete(b)
		}
	}
}

// tick opens an epoch, or notes the tick if one is already open.
func (f *flusher) tick() {
	if f.open {
		f.tickMissed = true
		f.p.Stats.TickSkipped.Add(1)
		return
	}
	f.openEpoch()
}

// merged follows a merge of a contribution from transcode code: a
// non-rate-limited one raises a flush request on every shard, and a request
// that was waiting for a dirty row is served now.
func (f *flusher) merged(code uint16) {
	if f.p.NonRateLimited[code] {
		f.raise()
	}
	if f.flushReq && !f.open {
		f.openEpoch()
	}
}

// raise puts a flush request on every shard. A shard that already has one
// pending keeps just that one.
func (f *flusher) raise() {
	f.p.Stats.FlushTriggered.Add(1)
	for _, ch := range f.p.FlushAll {
		select {
		case ch <- struct{}{}:
		default:
			f.p.Stats.FlushCoalesced.Add(1)
		}
	}
}

// openEpoch snapshots the dirty rows and starts sending them, unless nothing
// is dirty. Only an epoch that opens serves a pending tick or flush request;
// otherwise both stay as they were.
func (f *flusher) openEpoch() {
	if !f.p.Mirror.OpenEpoch() {
		f.p.Stats.EpochEmpty.Add(1)
		return
	}
	f.open = true
	f.tickMissed = false
	f.flushReq = false
	f.p.Stats.Epochs.Add(1)
	f.pump()
}

// pump sends batches of the epoch's rows while it has free batches and rows
// left to send.
func (f *flusher) pump() {
	var (
		b *mirror.Batch // batch being sent
	)

	for len(f.free) > 0 && f.p.Mirror.Cohort() > 0 {
		b = f.free[len(f.free)-1]
		f.free = f.free[:len(f.free)-1]
		f.p.Mirror.Fill(b, f.p.MaxBatch)
		select {
		case f.p.BatchQ <- b:
		default:
			// BatchQ holds every batch that exists, so this is a sizing bug.
			// Waiting for a completion instead could wait forever (HLD §7.3).
			panic("shard: batch queue full; it must hold shards × maxInFlightPerShard batches")
		}
		f.out++
		f.outRows += len(b.Rows)
		f.p.Stats.Batches.Add(1)
	}
}

// complete takes back a batch from a writer, then sends more of the epoch or,
// if the epoch is done, closes it and opens the next at once if a flush
// request is pending, or a tick was missed and rows are dirty.
func (f *flusher) complete(b *mirror.Batch) {
	f.outRows -= len(b.Rows)
	f.p.Mirror.Complete(b)
	f.free = append(f.free, b)
	f.out--
	if f.p.Mirror.Cohort() > 0 || f.out > 0 {
		f.pump()
		return
	}
	f.open = false
	if f.flushReq || (f.tickMissed && f.p.Mirror.Dirty() > 0) {
		f.openEpoch()
	}
}

// shutdown flushes epoch after epoch, retrying failed rows, until nothing is
// dirty or in flight, or p.DrainTimeout passes; then it logs and counts
// whatever is left (HLD §11.4).
func (f *flusher) shutdown() {
	var (
		timer *time.Timer // the drain deadline
		left  int         // rows not written
	)

	timer = time.NewTimer(f.p.DrainTimeout)
	defer timer.Stop()
	for {
		if !f.open {
			if f.p.Mirror.Dirty() == 0 {
				slog.Info("shard drained", "shard", f.p.ID)
				return
			}
			f.openEpoch()
		}
		select {
		case b := <-f.p.Done:
			f.complete(b)
		case <-timer.C:
			left = f.outRows + f.p.Mirror.Pending()
			f.p.Stats.Abandoned.Add(uint64(left))
			slog.Error("shutdown drain timed out, rows not written",
				"shard", f.p.ID, "inFlight", f.outRows, "pending", f.p.Mirror.Pending())
			return
		}
	}
}

// Ticker is the global flush tick: every interval it ticks every shard. Each
// out channel should have capacity 1; a shard that has not taken its last
// tick simply misses this one, and never delays the others. It returns when
// stop is closed.
func Ticker(every time.Duration, outs []chan struct{}, stop <-chan struct{}) {
	var (
		t *time.Ticker // the clock
	)

	t = time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			for _, out := range outs {
				select {
				case out <- struct{}{}:
				default:
				}
			}
		}
	}
}
