package shard_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"feedvalve/internal/mirror"
	"feedvalve/internal/shard"
)

const wait = 2 * time.Second // generous bound for anything that should happen

// harness runs one shard. In, Tick and done are unbuffered, so a send returns
// only once the shard has taken it, and everything it does next happens
// before it takes the following one. The test plays the writer through batchQ and done.
type harness struct {
	in     chan mirror.Contribution
	tick   chan struct{}
	batchQ chan *mirror.Batch
	done   chan *mirror.Batch
	st     *shard.Stats
	mst    *mirror.Stats
	flush  chan struct{} // this shard's flush-request channel
	exited chan struct{}
}

// opts are the flush-trigger settings of a test shard.
type opts struct {
	nonRL    map[uint16]bool // non-rate-limited transcodes
	flush    chan struct{}   // the shard's request channel; made, capacity 1, if nil
	flushAll []chan struct{} // every shard's request channel; just flush if nil
}

// start runs a shard with the given batch size, in-flight limit and drain
// timeout, and no non-rate-limited transcodes.
func start(t *testing.T, maxBatch, maxInFlight int, drain time.Duration) *harness {
	t.Helper()
	return startWith(t, maxBatch, maxInFlight, drain, opts{})
}

// startWith is start with flush-trigger settings.
func startWith(t *testing.T, maxBatch, maxInFlight int, drain time.Duration, o opts) *harness {
	t.Helper()
	if o.flush == nil {
		o.flush = make(chan struct{}, 1)
	}
	if o.flushAll == nil {
		o.flushAll = []chan struct{}{o.flush}
	}
	h := &harness{
		in:     make(chan mirror.Contribution),
		tick:   make(chan struct{}),
		batchQ: make(chan *mirror.Batch, maxInFlight),
		done:   make(chan *mirror.Batch),
		st:     new(shard.Stats),
		mst:    new(mirror.Stats),
		flush:  o.flush,
		exited: make(chan struct{}),
	}
	go func() {
		defer close(h.exited)
		shard.Run(shard.Params{
			ID: 2, In: h.in, Tick: h.tick, Mirror: mirror.New(h.mst),
			BatchQ: h.batchQ, Done: h.done,
			MaxBatch: maxBatch, MaxInFlight: maxInFlight, DrainTimeout: drain, Stats: h.st,
			Flush: o.flush, FlushAll: o.flushAll, NonRateLimited: o.nonRL,
		})
	}()
	return h
}

// put dirties row key of destination 0 from the rate-limited 7208, with
// column 0 = v.
func (h *harness) put(seq uint64, key, v int64) {
	h.putCode(seq, 7208, key, v)
}

// putCode is put from transcode code.
func (h *harness) putCode(seq uint64, code uint16, key, v int64) {
	c := mirror.Contribution{Seq: seq, Code: code, Dest: 0, RowKey: key}
	c.Set(0, v)
	h.in <- c
}

// take returns the next batch the shard sends.
func (h *harness) take(t *testing.T) *mirror.Batch {
	t.Helper()
	select {
	case b := <-h.batchQ:
		return b
	case <-time.After(wait):
		t.Fatal("no batch arrived")
		return nil
	}
}

// none asserts that no batch arrives for a short while.
func (h *harness) none(t *testing.T) {
	t.Helper()
	select {
	case b := <-h.batchQ:
		t.Fatalf("unexpected batch %v", rowKeys(b))
	case <-time.After(50 * time.Millisecond):
	}
}

// complete hands b back to the shard as a writer would, with err as the result.
func (h *harness) complete(b *mirror.Batch, err error) {
	b.Err = err
	h.done <- b
}

// waitExit waits for Run to return.
func (h *harness) waitExit(t *testing.T) {
	t.Helper()
	select {
	case <-h.exited:
	case <-time.After(wait):
		t.Fatal("Run did not return")
	}
}

// rowKeys lists b's row keys in order.
func rowKeys(b *mirror.Batch) []int64 {
	var keys []int64
	for _, r := range b.Rows {
		keys = append(keys, r.Key)
	}
	return keys
}

// waitFor polls cond until it holds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTickWithNothingDirty(t *testing.T) {
	h := start(t, 10, 2, time.Second)
	h.tick <- struct{}{}
	waitFor(t, "EpochEmpty = 1", func() bool { return h.st.EpochEmpty.Load() == 1 })
	h.none(t)
	if n := h.st.Epochs.Load(); n != 0 {
		t.Errorf("Epochs = %d, want 0", n)
	}
	close(h.in)
	h.waitExit(t)
}

func TestEpochPumpsWithinInFlightLimit(t *testing.T) {
	h := start(t, 2, 2, time.Second)
	for k := int64(1); k <= 5; k++ {
		h.put(uint64(k), k, k*10)
	}
	h.tick <- struct{}{}

	// Batches are reused once completed, so read each before handing it back.
	var got []int64
	b1, b2 := h.take(t), h.take(t)
	got = append(got, rowKeys(b1)...)
	got = append(got, rowKeys(b2)...)
	h.none(t) // both free batches are out: the shard must wait
	h.complete(b1, nil)
	b3 := h.take(t)
	got = append(got, rowKeys(b3)...)
	for _, b := range []*mirror.Batch{b2, b3} {
		if b.Shard != 2 {
			t.Errorf("batch Shard = %d, want 2", b.Shard)
		}
	}
	if want := []int64{1, 2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("rows batched %v, want %v", got, want)
	}
	h.complete(b2, nil)
	h.complete(b3, nil)

	// The epoch has closed: a tick now finds nothing dirty instead of
	// landing mid-epoch.
	h.tick <- struct{}{}
	waitFor(t, "EpochEmpty = 1", func() bool { return h.st.EpochEmpty.Load() == 1 })
	if e, s, b := h.st.Epochs.Load(), h.st.TickSkipped.Load(), h.st.Batches.Load(); e != 1 || s != 0 || b != 3 {
		t.Errorf("(Epochs, TickSkipped, Batches) = (%d, %d, %d), want (1, 0, 3)", e, s, b)
	}
	close(h.in)
	h.waitExit(t)
}

func TestTickDuringEpochReopensOnClose(t *testing.T) {
	h := start(t, 10, 1, time.Second)
	h.put(1, 1, 1)
	h.tick <- struct{}{}
	b := h.take(t)

	h.put(2, 2, 2) // dirtied mid-epoch
	h.tick <- struct{}{}
	waitFor(t, "TickSkipped = 1", func() bool { return h.st.TickSkipped.Load() == 1 })
	h.none(t)

	h.complete(b, nil)
	b2 := h.take(t) // no further tick needed: a tick was missed and a row is dirty
	if want := []int64{2}; !reflect.DeepEqual(rowKeys(b2), want) {
		t.Errorf("reopened epoch carried %v, want %v", rowKeys(b2), want)
	}
	if n := h.st.Epochs.Load(); n != 2 {
		t.Errorf("Epochs = %d, want 2", n)
	}
	h.complete(b2, nil)
	close(h.in)
	h.waitExit(t)
}

func TestFailedBatchRetriedNextEpoch(t *testing.T) {
	h := start(t, 10, 1, time.Second)
	h.put(1, 1, 1)
	h.put(2, 2, 2)
	h.tick <- struct{}{}
	b := h.take(t)
	h.complete(b, errors.New("commit failed"))
	h.none(t) // the retry waits for the next tick

	h.tick <- struct{}{}
	b2 := h.take(t)
	if want := []int64{1, 2}; !reflect.DeepEqual(rowKeys(b2), want) {
		t.Errorf("retry carried %v, want %v", rowKeys(b2), want)
	}
	if n := h.mst.Requeued.Load(); n != 2 {
		t.Errorf("Requeued = %d, want 2", n)
	}
	h.complete(b2, nil)
	close(h.in)
	h.waitExit(t)
}

func TestShutdownDrainsEverything(t *testing.T) {
	h := start(t, 2, 1, 5*time.Second)
	h.put(1, 1, 1)
	h.put(2, 2, 2)
	h.put(3, 3, 3)
	close(h.in) // no tick: shutdown flushes on its own

	var written []int64
	b := h.take(t)
	h.complete(b, errors.New("commit failed")) // retried within the drain
	for range 2 {
		b = h.take(t)
		written = append(written, rowKeys(b)...)
		h.complete(b, nil)
	}
	h.waitExit(t)
	// Rows 1 and 2 failed and retry in the next epoch, after row 3, which
	// was still in this one.
	if want := []int64{3, 1, 2}; !reflect.DeepEqual(written, want) {
		t.Errorf("written %v, want %v", written, want)
	}
	if n := h.st.Abandoned.Load(); n != 0 {
		t.Errorf("Abandoned = %d, want 0", n)
	}
}

func TestShutdownGivesUpAfterTimeout(t *testing.T) {
	h := start(t, 2, 1, 50*time.Millisecond)
	h.put(1, 1, 1)
	h.put(2, 2, 2)
	h.put(3, 3, 3)
	close(h.in)
	h.take(t) // two rows in flight, never completed; one row still dirty
	h.waitExit(t)
	if n := h.st.Abandoned.Load(); n != 3 {
		t.Errorf("Abandoned = %d, want 3", n)
	}
}

func TestShutdownWithNothingDirty(t *testing.T) {
	h := start(t, 2, 1, 5*time.Second)
	close(h.in)
	h.waitExit(t)
	h.none(t)
}

func TestTicker(t *testing.T) {
	outs := []chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}, 1)}
	stop := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		shard.Ticker(time.Millisecond, outs, stop)
		close(exited)
	}()

	for i, out := range outs {
		select {
		case <-out:
		case <-time.After(wait):
			t.Fatalf("shard %d got no tick", i)
		}
	}
	// Leave outs[0] full: the others must keep ticking regardless.
	<-outs[0]
	waitFor(t, "outs[0] full", func() bool { return len(outs[0]) == 1 })
	for range 3 {
		select {
		case <-outs[1]:
		case <-time.After(wait):
			t.Fatal("a full shard tick channel blocked the others")
		}
	}

	close(stop)
	select {
	case <-exited:
	case <-time.After(wait):
		t.Fatal("Ticker did not return after stop")
	}
}
