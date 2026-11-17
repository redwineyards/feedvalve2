// Package mirror holds each shard's coalesced copy of the destination rows.
//
// Its input is the Contribution: one update to one destination row, already
// normalised by a transcode's mapper. The mirror knows nothing of table
// schemas; a row is just numbered int64 columns, and a contribution says
// which of them it sets. Where two transcodes set the same column, whichever
// contribution reaches the shard last wins; there are no per-column stamps.
package mirror

// MaxCols is the number of columns a destination row can have, one bit each
// in Contribution.Mask.
const MaxCols = 64

// Contribution is one update to one destination row, passed by value on a
// shard queue. It holds only scalars and fixed-size arrays (ADR-006), so it
// never aliases a buffer a worker reuses. Values are canonical: scaled
// integer prices, Unix-epoch seconds, flags as 0/1. Only the columns whose
// bit is set in Mask are meaningful.
type Contribution struct {
	Seq     uint64 // arrival stamp of the datagram it came from
	Index   uint16 // sub-packet position within that datagram
	Record  uint16 // record position within the message
	Code    uint16 // transcode; with Token, the key of the stale check
	Dest    uint8  // destination table, 0..2 (HLD: exactly three)
	General bool   // no token: goes to shard 0
	Token   int32  // ignored when General
	RowKey  int64  // the row within Dest
	Mask    uint64 // bit i set: Cols[i] carries a value
	Cols    [MaxCols]int64
}

// Set stores v in column col and marks the column as set.
func (c *Contribution) Set(col int, v int64) {
	c.Cols[col] = v
	c.Mask |= 1 << col
}

// Stamp packs the contribution's arrival position into one number that
// orders contributions by arrival: datagram, then sub-packet, then record.
// Index and Record each fit in 8 bits: a 516-byte datagram holds at most 10
// sub-packets, and record counts are clamped far below 256.
func (c *Contribution) Stamp() uint64 {
	return c.Seq<<16 | uint64(c.Index)<<8 | uint64(c.Record)
}
