// Package router turns each decompressed message into contributions and puts
// each one on the queue of the shard that owns it.
//
// Splitting a message into contributions is per-transcode work: multi-token messages
// carry their own record count, and each transcode needs its own
// preprocessing. That work sits behind a Splitter, one per transcode. Codes
// with no Splitter are counted and dropped (HLD F2).
//
// The shard is chosen by token only (HLD ADR-002, ASM-15), so every transcode
// for a token lands on one shard and the rows a token touches have a single
// writer. The stale-drop check per token happens later, inside the shard.
package router

import (
	"context"
	"encoding/binary"
	"log/slog"
	"sync/atomic"

	"feedvalve/internal/mirror"
	"feedvalve/internal/unpack"
)

// errorCodeOff is ErrorCode's offset, the same in both header layouts
// (bible §2.8).
const errorCodeOff = 12

// Splitter does one transcode's preprocessing: it turns p into contributions
// and calls r.Send once per contribution, in order. It must not keep p.
//
// It takes the Router rather than a send function on purpose: a pointer
// passed through a function value escapes to the heap, which would allocate
// every ~390-byte Contribution; a direct r.Send call lets it stay on the stack
// (ADR-006).
type Splitter func(p *unpack.Packet, r *Router)

// Stats counts what the router has seen. Safe to read concurrently.
type Stats struct {
	Units         atomic.Uint64 // contributions put on a shard queue
	NoSplitter    atomic.Uint64 // packets with no Splitter for their code; dropped
	ErrorResponse atomic.Uint64 // packets with a non-zero ErrorCode; dropped
}

// Router sends contributions to shard queues. Safe for concurrent use: everything in
// it is read-only after New, apart from the counters.
type Router struct {
	splitters map[uint16]Splitter          // preprocessing per transcode
	shards    []chan<- mirror.Contribution // one queue per shard
	st        *Stats                       // the router's counters
}

// New returns a router that splits packets with splitters and sends contributions
// to shards. Neither the map nor the slice may change afterwards.
func New(splitters map[uint16]Splitter, shards []chan<- mirror.Contribution, st *Stats) *Router {
	return &Router{splitters: splitters, shards: shards, st: st}
}

// Route drops p if it is an error response or has no Splitter; otherwise it
// lets p's Splitter turn it into contributions and sends each one to its shard.
func (r *Router) Route(p *unpack.Packet) {
	var (
		split Splitter // p's preprocessing
		ok    bool     // whether p's code has a Splitter
	)

	// A non-zero ErrorCode means the body is MS_ERROR_RESPONSE, whatever the
	// code says (bible §2.10), so no Splitter may see it.
	if binary.BigEndian.Uint16(p.Data[errorCodeOff:]) != 0 {
		r.st.ErrorResponse.Add(1)
		return
	}
	split, ok = r.splitters[p.Code]
	if !ok {
		r.st.NoSplitter.Add(1)
		return
	}
	split(p, r)
}

// Send puts c on the queue of the shard that owns its token, blocking while
// that queue is full, and counts it. At debug level it also logs c's token,
// transcode and shard. That write is synchronous on the receive path and can
// break the NFR4 stall budget under burst, so debug is for troubleshooting
// only; the Enabled check keeps Send allocation-free otherwise (ADR-006).
func (r *Router) Send(c *mirror.Contribution) {
	var (
		shard int // index of the owning shard
	)

	shard = int(mix(c.Token) % uint32(len(r.shards)))
	r.shards[shard] <- *c
	r.st.Units.Add(1)
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("contribution queued", "token", c.Token, "code", c.Code, "shard", shard)
	}
}

// mix scrambles a token so that shard = mix(token) % n spreads tokens evenly.
// Exchange tokens come in contiguous blocks, so a plain modulo would tie a
// shard to a contract family (HLD §6.5).
func mix(token int32) uint32 {
	var (
		h uint32 // the scrambled token
	)

	h = uint32(token) * 0x9E3779B1
	h ^= h >> 16
	return h
}
