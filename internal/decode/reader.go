package decode

import (
	"encoding/binary"
	"math"
)

// reader reads big-endian fields one after another from b (bible §2.1).
// Decoders check the message length before reading, so a read never runs
// past b; padding is skipped explicitly with pad, never assumed.
type reader struct {
	b   []byte // the bytes being read
	off int    // offset of the next field
}

// u8 reads one byte.
func (r *reader) u8() byte {
	var (
		v byte // the field
	)

	v = r.b[r.off]
	r.off++
	return v
}

// i16 reads a SHORT.
func (r *reader) i16() int16 {
	var (
		v int16 // the field
	)

	v = int16(binary.BigEndian.Uint16(r.b[r.off:]))
	r.off += 2
	return v
}

// i32 reads a LONG, which is 4 bytes on this protocol (bible §2.7).
func (r *reader) i32() int32 {
	var (
		v int32 // the field
	)

	v = int32(binary.BigEndian.Uint32(r.b[r.off:]))
	r.off += 4
	return v
}

// f64 reads a DOUBLE.
func (r *reader) f64() float64 {
	var (
		v float64 // the field
	)

	v = math.Float64frombits(binary.BigEndian.Uint64(r.b[r.off:]))
	r.off += 8
	return v
}

// pad skips n padding bytes. name says which pad it is, so every skipped
// byte is visible at the call site (bible §2.3).
func (r *reader) pad(name string, n int) {
	_ = name
	r.off += n
}
