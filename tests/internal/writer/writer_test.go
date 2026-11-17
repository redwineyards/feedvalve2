package writer_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"feedvalve/internal/mirror"
	"feedvalve/internal/writer"
)

// batch returns a batch for shard holding n rows.
func batch(shard, n int) *mirror.Batch {
	b := mirror.NewBatch(shard, n)
	for i := range n {
		b.Rows = append(b.Rows, mirror.BatchRow{Key: int64(i)})
	}
	return b
}

func TestRunReturnsBatchesToTheirShards(t *testing.T) {
	in := make(chan *mirror.Batch, 4)
	done := []chan *mirror.Batch{make(chan *mirror.Batch, 4), make(chan *mirror.Batch, 4)}
	st := new(writer.Stats)
	bad := errors.New("ORA-00060")
	commit := func(b *mirror.Batch) error {
		if len(b.Rows) == 3 {
			return bad
		}
		return nil
	}

	in <- batch(0, 2)
	in <- batch(1, 3)
	in <- batch(1, 1)
	close(in)
	writer.Run(in, done, commit, st) // returns once in is drained and closed

	if len(done[0]) != 1 || len(done[1]) != 2 {
		t.Fatalf("done depths = (%d, %d), want (1, 2)", len(done[0]), len(done[1]))
	}
	if b := <-done[0]; b.Err != nil {
		t.Errorf("shard 0 batch Err = %v, want nil", b.Err)
	}
	if b := <-done[1]; b.Err != bad {
		t.Errorf("failed batch Err = %v, want %v", b.Err, bad)
	}
	if b := <-done[1]; b.Err != nil {
		t.Errorf("shard 1 second batch Err = %v, want nil", b.Err)
	}
	if s := [3]uint64{st.Batches.Load(), st.Rows.Load(), st.Failed.Load()}; s != [3]uint64{3, 6, 1} {
		t.Errorf("(Batches, Rows, Failed) = %v, want [3 6 1]", s)
	}
}

// A batch reused after a failure must not keep its old error.
func TestRunClearsStaleError(t *testing.T) {
	in := make(chan *mirror.Batch, 1)
	done := []chan *mirror.Batch{make(chan *mirror.Batch, 1)}
	b := batch(0, 1)
	b.Err = errors.New("from last time")
	in <- b
	close(in)
	writer.Run(in, done, func(*mirror.Batch) error { return nil }, new(writer.Stats))
	if got := <-done[0]; got.Err != nil {
		t.Errorf("Err = %v, want nil", got.Err)
	}
}

// Several writers on one queue commit at the same time.
func TestPoolCommitsInParallel(t *testing.T) {
	const writers = 3
	in := make(chan *mirror.Batch, writers)
	done := []chan *mirror.Batch{make(chan *mirror.Batch, writers)}
	var inside sync.WaitGroup
	inside.Add(writers)
	allIn := make(chan struct{})
	go func() { inside.Wait(); close(allIn) }()
	commit := func(*mirror.Batch) error {
		inside.Done()
		select {
		case <-allIn: // every writer is inside commit at once
			return nil
		case <-time.After(2 * time.Second):
			return errors.New("commits did not overlap")
		}
	}

	var pool sync.WaitGroup
	st := new(writer.Stats)
	for range writers {
		pool.Add(1)
		go func() {
			defer pool.Done()
			writer.Run(in, done, commit, st)
		}()
	}
	for range writers {
		in <- batch(0, 1)
	}
	close(in)
	pool.Wait()

	for range writers {
		if b := <-done[0]; b.Err != nil {
			t.Errorf("batch Err = %v: the pool did not commit in parallel", b.Err)
		}
	}
}
