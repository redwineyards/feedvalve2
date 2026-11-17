// Package stats holds packet counters shared by more than one pipeline stage:
// packets received per transcode, and per (token, transcode). Both are safe
// for concurrent use.
package stats

import (
	"sync"
	"sync/atomic"
)

// TranscodeCounts counts packets per transaction code. It is a fixed array
// indexed by the code, so Add is lock-free and never allocates. The zero
// value is ready to use; it is 512 KB, so embed it rather than copy it.
type TranscodeCounts struct {
	n [1 << 16]atomic.Uint64 // count per transcode
}

// Add counts one packet with transcode tc.
func (c *TranscodeCounts) Add(tc uint16) {
	c.n[tc].Add(1)
}

// Get returns how many packets with transcode tc have been counted.
func (c *TranscodeCounts) Get(tc uint16) uint64 {
	return c.n[tc].Load()
}

// Each calls fn for every transcode with a non-zero count, in ascending order.
func (c *TranscodeCounts) Each(fn func(tc uint16, n uint64)) {
	var (
		n uint64 // count for the transcode being visited
	)

	for i := range c.n {
		n = c.n[i].Load()
		if n != 0 {
			fn(uint16(i), n)
		}
	}
}

// tokenKey identifies one (token, transcode) pair.
type tokenKey struct {
	token int32
	tc    uint16
}

// TokenCounts counts packets per (token, transcode). Only a key never seen
// before allocates; the token universe is bounded, so the steady state does
// not.
type TokenCounts struct {
	mu sync.Mutex          // guards m
	m  map[tokenKey]uint64 // count per (token, transcode)
}

// NewTokenCounts returns an empty TokenCounts with room for capacity keys
// before the map has to grow.
func NewTokenCounts(capacity int) *TokenCounts {
	var (
		c *TokenCounts // the result
	)

	// Allocations.
	c = &TokenCounts{m: make(map[tokenKey]uint64, capacity)}

	return c
}

// Add counts one packet for token with transcode tc.
func (c *TokenCounts) Add(token int32, tc uint16) {
	c.mu.Lock()
	c.m[tokenKey{token, tc}]++
	c.mu.Unlock()
}

// Get returns how many packets have been counted for token with transcode tc.
func (c *TokenCounts) Get(token int32, tc uint16) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[tokenKey{token, tc}]
}

// Len returns how many distinct (token, transcode) pairs have been counted.
func (c *TokenCounts) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

// Each calls fn for every counted (token, transcode) pair, in no particular
// order. fn runs with the counter locked, so it must not call back into c.
func (c *TokenCounts) Each(fn func(token int32, tc uint16, n uint64)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, n := range c.m {
		fn(k.token, k.tc, n)
	}
}
