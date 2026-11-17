package shard_test

import (
	"reflect"
	"testing"
	"time"
)

// marketStatus is a non-rate-limited transcode (bible §4.5).
const marketStatus = 6511

// nrl is the non-rate-limited set used by these tests.
var nrl = map[uint16]bool{marketStatus: true}

func TestNonRateLimitedFlushesWithoutTick(t *testing.T) {
	h := startWith(t, 10, 1, time.Second, opts{nonRL: nrl})
	h.put(1, 1, 1) // rate-limited: waits for the tick
	h.none(t)

	h.putCode(2, marketStatus, 2, 2)
	b := h.take(t)
	if want := []int64{1, 2}; !reflect.DeepEqual(rowKeys(b), want) {
		t.Errorf("flushed %v, want %v (everything dirty, not just the trigger)", rowKeys(b), want)
	}
	if n := h.st.FlushTriggered.Load(); n != 1 {
		t.Errorf("FlushTriggered = %d, want 1", n)
	}
	h.complete(b, nil)
	close(h.in)
	h.waitExit(t)
}

func TestFlushRequestReachesEveryShard(t *testing.T) {
	fa, fb := make(chan struct{}, 1), make(chan struct{}, 1)
	all := []chan struct{}{fa, fb}
	a := startWith(t, 10, 1, time.Second, opts{nonRL: nrl, flush: fa, flushAll: all})
	b := startWith(t, 10, 1, time.Second, opts{nonRL: nrl, flush: fb, flushAll: all})

	a.put(1, 10, 1) // shard A holds only rate-limited state
	a.none(t)
	b.putCode(2, marketStatus, 20, 2)

	ba, bb := a.take(t), b.take(t)
	if !reflect.DeepEqual(rowKeys(ba), []int64{10}) || !reflect.DeepEqual(rowKeys(bb), []int64{20}) {
		t.Errorf("shard A flushed %v, shard B %v; want [10] and [20]", rowKeys(ba), rowKeys(bb))
	}
	a.complete(ba, nil)
	b.complete(bb, nil)
	close(a.in)
	close(b.in)
	a.waitExit(t)
	b.waitExit(t)
}

func TestFlushRequestDuringEpochWaitsForClose(t *testing.T) {
	h := startWith(t, 10, 1, time.Second, opts{nonRL: nrl})
	h.put(1, 1, 1)
	h.tick <- struct{}{}
	b1 := h.take(t)

	h.putCode(2, marketStatus, 2, 2)
	h.none(t) // the open epoch's cohort is a snapshot: row 2 is not admitted

	h.complete(b1, nil)
	b2 := h.take(t) // the next epoch opens at once, no tick
	if want := []int64{2}; !reflect.DeepEqual(rowKeys(b2), want) {
		t.Errorf("next epoch carried %v, want %v", rowKeys(b2), want)
	}
	h.complete(b2, nil)
	close(h.in)
	h.waitExit(t)
}

func TestFlushRequestWithNothingDirtyStaysPending(t *testing.T) {
	h := startWith(t, 10, 1, time.Second, opts{nonRL: nrl})
	h.flush <- struct{}{} // as another shard would
	waitFor(t, "EpochEmpty = 1", func() bool { return h.st.EpochEmpty.Load() == 1 })
	h.none(t)

	h.put(1, 1, 1) // rate-limited, but the request is still pending
	b := h.take(t)
	if want := []int64{1}; !reflect.DeepEqual(rowKeys(b), want) {
		t.Errorf("flushed %v, want %v", rowKeys(b), want)
	}
	h.complete(b, nil)

	h.put(2, 2, 2) // the request was served: back to waiting for the tick
	h.none(t)
	close(h.in)
	h.complete(h.take(t), nil) // the shutdown flush
	h.waitExit(t)
}

func TestStaleNonRateLimitedDoesNotTrigger(t *testing.T) {
	h := startWith(t, 10, 1, time.Second, opts{nonRL: nrl})
	h.putCode(5, marketStatus, 1, 5)
	h.complete(h.take(t), nil)

	h.putCode(4, marketStatus, 1, 4) // older than what was applied: dropped
	h.none(t)
	if n := h.st.FlushTriggered.Load(); n != 1 {
		t.Errorf("FlushTriggered = %d, want 1", n)
	}
	close(h.in)
	h.waitExit(t)
}

func TestFlushRequestsCoalesce(t *testing.T) {
	peer := make(chan struct{}, 1) // a shard that never takes its request
	h := startWith(t, 10, 1, time.Second, opts{nonRL: nrl, flushAll: []chan struct{}{peer}})
	h.putCode(1, marketStatus, 1, 1)
	h.putCode(2, marketStatus, 2, 2)
	waitFor(t, "FlushTriggered = 2", func() bool { return h.st.FlushTriggered.Load() == 2 })
	if co := h.st.FlushCoalesced.Load(); co != 1 {
		t.Errorf("FlushCoalesced = %d, want 1", co)
	}
	close(h.in)
	h.complete(h.take(t), nil) // the shutdown flush
	h.waitExit(t)
}
