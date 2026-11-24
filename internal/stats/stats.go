// Package stats holds the per-transcode packet counter (FR10). It is safe for
// concurrent use.
package stats

import "sync/atomic"

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
