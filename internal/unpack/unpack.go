// Package unpack runs the worker pool between the receiver and the shards. A
// worker takes a datagram, splits it into sub-packets in FIFO order (bible
// §3.2), LZO1Z-decompresses the compressed ones (bible §3.5), strips the
// 8-byte prefix (bible §3.4), works out which header the message has and
// hands each message to the next stage.
//
// Deliberate deviation from ADR-001/ADR-007 (phase1_hld_design1.md): decode
// does not run inline on the receive goroutine, and several workers process
// datagrams in parallel, so datagrams can overtake each other. Order within
// one datagram is kept, because one worker handles it start to finish. Order
// across datagrams is restored downstream from the receiver's arrival stamp:
// every Packet carries (Seq, Index), and the shard drops a packet older than
// the latest it has applied for the same (token, transcode). That is safe
// because every transcode is a complete snapshot.
//
// Header resolution follows HLD §6.4. Nearly every message has BCAST_HEADER,
// with TransactionCode at offset 10, but 6541 and 6503 have MESSAGE_HEADER,
// with the code at offset 0 (bible §2.8). A message is MESSAGE_HEADER only
// when the SHORT at offset 0 is 6541 and the message is 40 bytes long, or
// 6503 and 52 bytes; everything else is BCAST_HEADER.
//
// A malformed sub-packet is counted, logged and dropped; it is never fatal.
package unpack

import (
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"feedvalve/internal/lzo"
	"feedvalve/internal/receiver"
	"feedvalve/internal/stats"
)

const (
	envelopeLen = 4  // cNetId[2] + iNoPackets (bible §3.2)
	compLenLen  = 2  // iCompLen before every sub-packet (bible §3.3)
	prefixLen   = 8  // broadcast-data prefix before the header (bible §3.4)
	headerLen   = 40 // BCAST_HEADER / MESSAGE_HEADER (bible §2.8)
	msgLenOff   = 38 // MessageLength, from the start of the header
	tcodeOff    = 10 // TransactionCode in BCAST_HEADER; MESSAGE_HEADER has it at 0

	heartbeatCode = 6541 // MESSAGE_HEADER only, 40 bytes (bible §4.12)
	heartbeatLen  = 40
	assetRateCode = 6503 // MESSAGE_HEADER, 52 bytes (bible §4.15)
	assetRateLen  = 52
)

// Packet is one sub-packet's message, from the header onward, with the
// prefix already stripped. It holds no pointers or slices (ADR-006), so it
// never aliases a buffer a worker reuses. Only Data[:Len] is meaningful.
type Packet struct {
	Seq       uint64 // arrival stamp of the datagram it came from
	Index     uint16 // position within that datagram, FIFO from 0
	Code      uint16 // TransactionCode, read from the right header layout
	MsgHeader bool   // true for MESSAGE_HEADER (6541, 6503), else BCAST_HEADER
	Len       uint16
	Data      [lzo.MaxDecompressedLen]byte
}

// Stats counts what the pool has seen. Safe to read concurrently.
type Stats struct {
	Datagrams     atomic.Uint64 // datagrams taken from the queue
	Packets       atomic.Uint64 // packets sent on the output queue
	Malformed     atomic.Uint64 // framing lost; the rest of the datagram was dropped
	ShortMessage  atomic.Uint64 // message shorter than its header; dropped
	NetIDMismatch atomic.Uint64 // prefix byte 0 differs from the envelope netId; still sent
	TrailingBytes atomic.Uint64 // bytes left over after the last sub-packet

	// One counter per lzo sentinel, because each means a different fix (FR10).
	LZOInputOverrun      atomic.Uint64
	LZOOutputOverrun     atomic.Uint64
	LZOLookbehindOverrun atomic.Uint64
	LZOInputNotConsumed  atomic.Uint64

	Transcodes stats.TranscodeCounts // packets emitted, per resolved transcode
}

// Run starts workers goroutines that unpack every datagram from in and call
// emit once per message, in FIFO order within a datagram. It returns once in
// is closed and fully drained. emit runs on the worker and may block; the
// receiver's non-blocking enqueue is what protects the read loop (NFR4).
// emit is called concurrently from every worker and must not keep p after it
// returns, because the worker reuses it.
func Run(in <-chan receiver.Datagram, workers int, st *Stats, emit func(p *Packet)) {
	var (
		wg sync.WaitGroup // tracks the running workers
	)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			work(in, st, emit)
		}()
	}
	wg.Wait()
}

// work is one pool member: it unpacks datagrams until in is closed.
func work(in <-chan receiver.Datagram, st *Stats, emit func(p *Packet)) {
	var (
		scratch [lzo.MaxDecompressedLen]byte // decompression output, reused per sub-packet
		p       Packet                       // the packet being emitted, reused
	)

	for d := range in {
		unpackOne(&d, &scratch, &p, st, emit)
	}
}

// unpackOne splits one datagram into sub-packets, decompresses the
// compressed ones and emits each message, in FIFO order. scratch and p are
// the worker's reusable buffers.
func unpackOne(d *receiver.Datagram, scratch *[lzo.MaxDecompressedLen]byte, p *Packet, st *Stats, emit func(p *Packet)) {
	var (
		b       []byte // the datagram's meaningful bytes
		netID   byte   // envelope netId, which every prefix must repeat
		count   int    // iNoPackets
		cur     int    // read position in b
		compLen int    // iCompLen of the current sub-packet
		msgLen  int    // MessageLength of an uncompressed sub-packet
		payload []byte // current sub-packet's data, prefix included
		n       int    // decompressed length
		err     error  // decompression result
	)

	st.Datagrams.Add(1)
	b = d.Data[:d.Len]
	if len(b) < envelopeLen {
		st.Malformed.Add(1)
		slog.Warn("datagram shorter than its envelope", "seq", d.Seq, "bytes", len(b))
		return
	}
	netID = b[0]
	count = int(int16(binary.BigEndian.Uint16(b[2:])))
	if count < 0 {
		st.Malformed.Add(1)
		slog.Warn("negative iNoPackets", "seq", d.Seq, "iNoPackets", count)
		return
	}

	cur = envelopeLen
	for i := 0; i < count; i++ {
		if len(b)-cur < compLenLen {
			st.Malformed.Add(1)
			slog.Warn("datagram ended before iNoPackets sub-packets", "seq", d.Seq, "index", i, "iNoPackets", count)
			return
		}
		compLen = int(int16(binary.BigEndian.Uint16(b[cur:])))
		cur += compLenLen

		switch {
		case compLen < 0:
			st.Malformed.Add(1)
			slog.Warn("negative iCompLen", "seq", d.Seq, "index", i, "iCompLen", compLen)
			return

		case compLen > 0:
			if compLen > len(b)-cur {
				st.Malformed.Add(1)
				slog.Warn("iCompLen runs past the datagram", "seq", d.Seq, "index", i, "iCompLen", compLen, "left", len(b)-cur)
				return
			}
			n, err = lzo.DecompressSafe(scratch[:], b[cur:cur+compLen])
			cur += compLen
			if err != nil {
				countLZOError(st, err)
				slog.Warn("decompression failed, sub-packet skipped", "seq", d.Seq, "index", i, "err", err)
				continue
			}
			payload = scratch[:n]

		default:
			if len(b)-cur < prefixLen+headerLen {
				st.Malformed.Add(1)
				slog.Warn("uncompressed sub-packet shorter than a header", "seq", d.Seq, "index", i, "left", len(b)-cur)
				return
			}
			msgLen = int(binary.BigEndian.Uint16(b[cur+prefixLen+msgLenOff:]))
			if msgLen < headerLen {
				// Advancing by a too-small length would misframe every later
				// sub-packet, so the rest of the datagram goes (bible §3.3).
				st.ShortMessage.Add(1)
				slog.Warn("MessageLength under 40, rest of datagram dropped", "seq", d.Seq, "index", i, "messageLength", msgLen)
				return
			}
			if prefixLen+msgLen > len(b)-cur {
				st.Malformed.Add(1)
				slog.Warn("MessageLength runs past the datagram", "seq", d.Seq, "index", i, "messageLength", msgLen, "left", len(b)-cur)
				return
			}
			payload = b[cur : cur+prefixLen+msgLen]
			cur += prefixLen + msgLen
		}

		// The one place the prefix is handled (bible §3.4).
		if len(payload) < prefixLen+headerLen {
			st.ShortMessage.Add(1)
			slog.Warn("message shorter than its header, skipped", "seq", d.Seq, "index", i, "bytes", len(payload))
			continue
		}
		if payload[0] != netID {
			st.NetIDMismatch.Add(1)
			slog.Warn("prefix netId differs from envelope", "seq", d.Seq, "index", i, "prefix", payload[0], "envelope", netID)
		}
		p.Seq = d.Seq
		p.Index = uint16(i)
		p.Len = uint16(copy(p.Data[:], payload[prefixLen:]))
		p.Code, p.MsgHeader = resolveHeader(p.Data[:p.Len])
		st.Transcodes.Add(p.Code)
		st.Packets.Add(1)
		emit(p)
	}

	if cur != len(b) {
		st.TrailingBytes.Add(1)
		slog.Warn("bytes left after the last sub-packet", "seq", d.Seq, "left", len(b)-cur, "iNoPackets", count)
	}
}

// resolveHeader works out which header m (a message from the header onward,
// at least 40 bytes) has, and returns its TransactionCode (HLD §6.4).
func resolveHeader(m []byte) (code uint16, msgHeader bool) {
	var (
		atZero uint16 // MESSAGE_HEADER.TransactionCode position
	)

	atZero = binary.BigEndian.Uint16(m[0:])
	if (atZero == heartbeatCode && len(m) == heartbeatLen) || (atZero == assetRateCode && len(m) == assetRateLen) {
		return atZero, true
	}
	return binary.BigEndian.Uint16(m[tcodeOff:]), false
}

// countLZOError adds one to the counter for err's lzo sentinel.
func countLZOError(st *Stats, err error) {
	switch {
	case errors.Is(err, lzo.ErrInputOverrun):
		st.LZOInputOverrun.Add(1)
	case errors.Is(err, lzo.ErrOutputOverrun):
		st.LZOOutputOverrun.Add(1)
	case errors.Is(err, lzo.ErrLookbehindOverrun):
		st.LZOLookbehindOverrun.Add(1)
	case errors.Is(err, lzo.ErrInputNotConsumed):
		st.LZOInputNotConsumed.Add(1)
	}
}
