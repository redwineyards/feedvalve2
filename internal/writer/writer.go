// Package writer runs the writer pool: stateless goroutines that take
// batches from the shared batch queue, commit each one, and hand it back to
// the shard that owns it (HLD ADR-005). Any writer takes any shard's batch,
// so the pool writes up to its size in parallel and several writers can drain
// one busy shard at once.
//
// Parallel commits are safe because no two batches in flight ever share a
// row: a row belongs to one shard, and a shard never batches a row that is
// already in flight (HLD §6.7, §8.4).
package writer

import (
	"log/slog"
	"sync/atomic"

	"feedvalve/internal/mirror"
)

// Commit writes one batch as one transaction, destination 0 first, and
// reports whether it committed. The SQL is not written yet: it needs the
// destination table DDLs.
type Commit func(b *mirror.Batch) error

// Stats counts what the writers have done. Safe to read concurrently.
type Stats struct {
	Batches atomic.Uint64 // batches committed or failed
	Rows    atomic.Uint64 // rows in those batches
	Failed  atomic.Uint64 // batches whose commit failed; their rows are retried next epoch
}

// Run commits batches from in until in is closed. Each batch's result goes
// in its Err, and the batch goes back to done[b.Shard]. That send never
// blocks: each shard's done channel has room for every batch it can have out.
func Run(in <-chan *mirror.Batch, done []chan *mirror.Batch, commit Commit, st *Stats) {
	for b := range in {
		b.Err = commit(b)
		st.Batches.Add(1)
		st.Rows.Add(uint64(len(b.Rows)))
		if b.Err != nil {
			st.Failed.Add(1)
			slog.Warn("batch commit failed, rows retried next epoch", "shard", b.Shard, "rows", len(b.Rows), "err", b.Err)
		}
		done[b.Shard] <- b
	}
}
