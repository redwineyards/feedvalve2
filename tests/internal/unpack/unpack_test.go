package unpack_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"feedvalve/internal/lzo"
	"feedvalve/internal/receiver"
	"feedvalve/internal/unpack"
)

const netID = 8

// msg builds a message from the header onward: a 40-byte header carrying
// transcode tc and MessageLength, then bodyLen body bytes that depend on tc
// so packets are distinguishable.
func msg(tc uint16, bodyLen int) []byte {
	m := make([]byte, 40+bodyLen)
	binary.BigEndian.PutUint16(m[10:], tc)
	binary.BigEndian.PutUint16(m[38:], uint16(40+bodyLen))
	for i := 40; i < len(m); i++ {
		m[i] = byte(int(tc) + i)
	}
	return m
}

// msgHeader builds a MESSAGE_HEADER message: transcode at offset 0,
// MessageLength at 38, then bodyLen body bytes.
func msgHeader(tc uint16, bodyLen int) []byte {
	m := make([]byte, 40+bodyLen)
	binary.BigEndian.PutUint16(m[0:], tc)
	binary.BigEndian.PutUint16(m[38:], uint16(40+bodyLen))
	return m
}

// withPrefix puts the 8-byte broadcast-data prefix in front of m.
func withPrefix(net byte, m []byte) []byte {
	return append([]byte{net, 1, 2, 3, 4, 5, 6, 7}, m...)
}

// lzoLiteral encodes b as a literal-only LZO1Z stream, checked against the
// real decompressor so the helper cannot silently be wrong.
func lzoLiteral(t *testing.T, b []byte) []byte {
	t.Helper()
	if len(b) < 4 || len(b) > 238 {
		t.Fatalf("lzoLiteral: %d bytes, need 4..238", len(b))
	}
	s := append([]byte{byte(17 + len(b))}, b...)
	s = append(s, 0x11, 0x00, 0x00) // end-of-stream marker
	dst := make([]byte, lzo.MaxDecompressedLen)
	n, err := lzo.DecompressSafe(dst, s)
	if err != nil || !bytes.Equal(dst[:n], b) {
		t.Fatalf("lzoLiteral self-check failed: n=%d err=%v", n, err)
	}
	return s
}

// plain is an uncompressed sub-packet: iCompLen 0, then the payload.
func plain(payload []byte) []byte {
	return append([]byte{0, 0}, payload...)
}

// comp is a compressed sub-packet: iCompLen, then the compressed stream.
func comp(stream []byte) []byte {
	s := binary.BigEndian.AppendUint16(nil, uint16(len(stream)))
	return append(s, stream...)
}

// dgram builds a datagram with envelope netId, iNoPackets n and the given
// sub-packets back to back.
func dgram(t *testing.T, seq uint64, n int16, subs ...[]byte) receiver.Datagram {
	t.Helper()
	b := []byte{netID, 0}
	b = binary.BigEndian.AppendUint16(b, uint16(n))
	for _, s := range subs {
		b = append(b, s...)
	}
	return raw(t, seq, b)
}

// raw wraps arbitrary bytes as a datagram.
func raw(t *testing.T, seq uint64, b []byte) receiver.Datagram {
	t.Helper()
	if len(b) > receiver.MaxDatagram {
		t.Fatalf("test datagram is %d bytes, max %d", len(b), receiver.MaxDatagram)
	}
	d := receiver.Datagram{Seq: seq, Len: uint16(len(b))}
	copy(d.Data[:], b)
	return d
}

// runPool feeds dgs through unpack.Run and returns every packet it emitted,
// in emit order. It fails if Run does not return after in is closed.
func runPool(t *testing.T, workers int, dgs []receiver.Datagram) ([]unpack.Packet, *unpack.Stats) {
	t.Helper()
	in := make(chan receiver.Datagram, len(dgs))
	st := new(unpack.Stats)
	for _, d := range dgs {
		in <- d
	}
	close(in)

	var mu sync.Mutex
	var got []unpack.Packet
	emit := func(p *unpack.Packet) {
		mu.Lock()
		got = append(got, *p) // copy: p is reused after emit returns
		mu.Unlock()
	}

	returned := make(chan struct{})
	go func() {
		unpack.Run(in, workers, st, emit)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after in was closed")
	}
	return got, st
}

// counts is a plain copy of unpack.Stats, for comparing in one go.
type counts struct {
	Datagrams, Packets, Malformed, ShortMessage, NetIDMismatch, TrailingBytes    uint64
	LZOInputOverrun, LZOOutputOverrun, LZOLookbehindOverrun, LZOInputNotConsumed uint64
}

func snap(st *unpack.Stats) counts {
	return counts{
		Datagrams: st.Datagrams.Load(), Packets: st.Packets.Load(),
		Malformed: st.Malformed.Load(), ShortMessage: st.ShortMessage.Load(),
		NetIDMismatch: st.NetIDMismatch.Load(), TrailingBytes: st.TrailingBytes.Load(),
		LZOInputOverrun: st.LZOInputOverrun.Load(), LZOOutputOverrun: st.LZOOutputOverrun.Load(),
		LZOLookbehindOverrun: st.LZOLookbehindOverrun.Load(), LZOInputNotConsumed: st.LZOInputNotConsumed.Load(),
	}
}

// want describes one expected packet.
type want struct {
	seq       uint64
	index     uint16
	data      []byte
	code      uint16 // 0: the code at header offset 10
	msgHeader bool
}

func TestRunFraming(t *testing.T) {
	m7305 := msg(7305, 10)
	m7208 := msg(7208, 20)
	m6541 := msg(6541, 8)
	m7201 := msg(7201, 30)

	shortML := msg(7305, 0)
	binary.BigEndian.PutUint16(shortML[38:], 39)
	longML := msg(7305, 10)
	binary.BigEndian.PutUint16(longML[38:], 200)

	ok := plain(withPrefix(netID, m7305))

	heartbeat := msgHeader(6541, 0)
	assetRate := msgHeader(6503, 12)
	// A BCAST_HEADER message whose first SHORT happens to read 6541: only the
	// exact 6541 length makes it a MESSAGE_HEADER.
	lookalike := msg(7305, 10)
	binary.BigEndian.PutUint16(lookalike[0:], 6541)

	tests := []struct {
		name   string
		dgs    func(t *testing.T) []receiver.Datagram
		want   []want
		counts counts
		tcs    map[uint16]uint64 // per-transcode counts expected
	}{
		{
			name:   "single uncompressed, prefix stripped",
			dgs:    func(t *testing.T) []receiver.Datagram { return []receiver.Datagram{dgram(t, 7, 1, ok)} },
			want:   []want{{seq: 7, index: 0, data: m7305}},
			counts: counts{Datagrams: 1, Packets: 1},
			tcs:    map[uint16]uint64{7305: 1},
		},
		{
			name: "mixed compressed and uncompressed, FIFO",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 3,
					comp(lzoLiteral(t, withPrefix(netID, m7208))),
					plain(withPrefix(netID, m6541)),
					comp(lzoLiteral(t, withPrefix(netID, m7201))))}
			},
			want:   []want{{seq: 1, index: 0, data: m7208}, {seq: 1, index: 1, data: m6541}, {seq: 1, index: 2, data: m7201}},
			counts: counts{Datagrams: 1, Packets: 3},
			tcs:    map[uint16]uint64{7208: 1, 6541: 1, 7201: 1},
		},
		{
			name: "MessageLength under 40 drops the rest of the datagram",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 3, ok, plain(withPrefix(netID, shortML)), ok)}
			},
			want:   []want{{seq: 1, index: 0, data: m7305}},
			counts: counts{Datagrams: 1, Packets: 1, ShortMessage: 1},
			tcs:    map[uint16]uint64{7305: 1},
		},
		{
			name: "uncompressed MessageLength past the end",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 1, plain(withPrefix(netID, longML)))}
			},
			counts: counts{Datagrams: 1, Malformed: 1},
		},
		{
			name: "uncompressed with fewer than 48 bytes left",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 1, plain(make([]byte, 20)))}
			},
			counts: counts{Datagrams: 1, Malformed: 1},
		},
		{
			name: "iCompLen past the end",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 1, []byte{0, 100, 1, 2, 3})}
			},
			counts: counts{Datagrams: 1, Malformed: 1},
		},
		{
			name: "negative iCompLen",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 2, ok, []byte{0xFF, 0xFF, 1, 2, 3})}
			},
			want:   []want{{seq: 1, index: 0, data: m7305}},
			counts: counts{Datagrams: 1, Packets: 1, Malformed: 1},
			tcs:    map[uint16]uint64{7305: 1},
		},
		{
			name: "decompressed payload under 48 bytes skips only that sub-packet",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 2, comp(lzoLiteral(t, make([]byte, 40))), ok)}
			},
			want:   []want{{seq: 1, index: 1, data: m7305}},
			counts: counts{Datagrams: 1, Packets: 1, ShortMessage: 1},
			tcs:    map[uint16]uint64{7305: 1},
		},
		{
			name: "netId mismatch is counted but delivered",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 1, plain(withPrefix(4, m7305)))}
			},
			want:   []want{{seq: 1, index: 0, data: m7305}},
			counts: counts{Datagrams: 1, Packets: 1, NetIDMismatch: 1},
			tcs:    map[uint16]uint64{7305: 1},
		},
		{
			name:   "iNoPackets larger than what is present",
			dgs:    func(t *testing.T) []receiver.Datagram { return []receiver.Datagram{dgram(t, 1, 2, ok)} },
			want:   []want{{seq: 1, index: 0, data: m7305}},
			counts: counts{Datagrams: 1, Packets: 1, Malformed: 1},
			tcs:    map[uint16]uint64{7305: 1},
		},
		{
			name:   "iNoPackets smaller than what is present",
			dgs:    func(t *testing.T) []receiver.Datagram { return []receiver.Datagram{dgram(t, 1, 1, ok, ok)} },
			want:   []want{{seq: 1, index: 0, data: m7305}},
			counts: counts{Datagrams: 1, Packets: 1, TrailingBytes: 1},
			tcs:    map[uint16]uint64{7305: 1},
		},
		{
			name:   "negative iNoPackets",
			dgs:    func(t *testing.T) []receiver.Datagram { return []receiver.Datagram{dgram(t, 1, -1, ok)} },
			counts: counts{Datagrams: 1, Malformed: 1},
		},
		{
			name: "6541 heartbeat resolves to MESSAGE_HEADER",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 1, plain(withPrefix(netID, heartbeat)))}
			},
			want:   []want{{seq: 1, index: 0, data: heartbeat, code: 6541, msgHeader: true}},
			counts: counts{Datagrams: 1, Packets: 1},
			tcs:    map[uint16]uint64{6541: 1},
		},
		{
			name: "6503 asset rate resolves to MESSAGE_HEADER",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 1, plain(withPrefix(netID, assetRate)))}
			},
			want:   []want{{seq: 1, index: 0, data: assetRate, code: 6503, msgHeader: true}},
			counts: counts{Datagrams: 1, Packets: 1},
			tcs:    map[uint16]uint64{6503: 1},
		},
		{
			name: "6541 at offset 0 with the wrong length stays BCAST_HEADER",
			dgs: func(t *testing.T) []receiver.Datagram {
				return []receiver.Datagram{dgram(t, 1, 1, plain(withPrefix(netID, lookalike)))}
			},
			want:   []want{{seq: 1, index: 0, data: lookalike, code: 7305}},
			counts: counts{Datagrams: 1, Packets: 1},
			tcs:    map[uint16]uint64{7305: 1},
		},
		{
			name:   "datagram under 4 bytes",
			dgs:    func(t *testing.T) []receiver.Datagram { return []receiver.Datagram{raw(t, 1, []byte{netID, 0, 0})} },
			counts: counts{Datagrams: 1, Malformed: 1},
		},
		{
			name:   "zero sub-packets",
			dgs:    func(t *testing.T) []receiver.Datagram { return []receiver.Datagram{dgram(t, 1, 0)} },
			counts: counts{Datagrams: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, st := runPool(t, 1, tt.dgs(t))
			if len(got) != len(tt.want) {
				t.Fatalf("got %d packets, want %d", len(got), len(tt.want))
			}
			for i, w := range tt.want {
				p := got[i]
				if p.Seq != w.seq || p.Index != w.index {
					t.Errorf("packet %d: (Seq, Index) = (%d, %d), want (%d, %d)", i, p.Seq, p.Index, w.seq, w.index)
				}
				if !bytes.Equal(p.Data[:p.Len], w.data) {
					t.Errorf("packet %d: data mismatch: got %d bytes, want %d", i, p.Len, len(w.data))
				}
				code := w.code
				if code == 0 {
					code = binary.BigEndian.Uint16(w.data[10:])
				}
				if p.Code != code || p.MsgHeader != w.msgHeader {
					t.Errorf("packet %d: (Code, MsgHeader) = (%d, %v), want (%d, %v)", i, p.Code, p.MsgHeader, code, w.msgHeader)
				}
			}
			if c := snap(st); c != tt.counts {
				t.Errorf("stats = %+v\n         want %+v", c, tt.counts)
			}
			seen := map[uint16]uint64{}
			st.Transcodes.Each(func(tc uint16, n uint64) { seen[tc] = n })
			if len(seen) != len(tt.tcs) {
				t.Errorf("transcode counts = %v, want %v", seen, tt.tcs)
			}
			for tc, n := range tt.tcs {
				if got := st.Transcodes.Get(tc); got != n {
					t.Errorf("Transcodes.Get(%d) = %d, want %d", tc, got, n)
				}
			}
		})
	}
}

// Each malformed stream below is checked against lzo.DecompressSafe first,
// so the case proves which sentinel it exercises.
func TestRunCountsEachLZOError(t *testing.T) {
	good := msg(7305, 10)
	valid := lzoLiteral(t, withPrefix(netID, good))

	tests := []struct {
		name    string
		stream  []byte
		wantErr error
		counts  counts
	}{
		{"truncated stream", valid[:len(valid)-2], lzo.ErrInputOverrun,
			counts{Datagrams: 1, Packets: 1, LZOInputOverrun: 1}},
		{"output larger than the buffer", []byte{21, 'a', 'a', 'a', 'a', 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0}, lzo.ErrOutputOverrun,
			counts{Datagrams: 1, Packets: 1, LZOOutputOverrun: 1}},
		{"match before the start", []byte{21, 1, 2, 3, 4, 0, 0, 0}, lzo.ErrLookbehindOverrun,
			counts{Datagrams: 1, Packets: 1, LZOLookbehindOverrun: 1}},
		{"trailing compressed bytes", append(append([]byte{}, valid...), 0xFF), lzo.ErrInputNotConsumed,
			counts{Datagrams: 1, Packets: 1, LZOInputNotConsumed: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := make([]byte, lzo.MaxDecompressedLen)
			if _, err := lzo.DecompressSafe(dst, tt.stream); !errors.Is(err, tt.wantErr) {
				t.Fatalf("fixture self-check: DecompressSafe = %v, want %v", err, tt.wantErr)
			}

			// The bad sub-packet is skipped; the one after it still arrives.
			got, st := runPool(t, 1, []receiver.Datagram{dgram(t, 1, 2, comp(tt.stream), plain(withPrefix(netID, good)))})
			if len(got) != 1 || got[0].Index != 1 || !bytes.Equal(got[0].Data[:got[0].Len], good) {
				t.Fatalf("got %d packets, want only sub-packet 1", len(got))
			}
			if c := snap(st); c != tt.counts {
				t.Errorf("stats = %+v\n         want %+v", c, tt.counts)
			}
		})
	}
}

func TestRunPoolDeliversEverythingInOrderPerDatagram(t *testing.T) {
	const datagrams, perDatagram = 200, 3
	var dgs []receiver.Datagram
	for s := uint64(1); s <= datagrams; s++ {
		var subs [][]byte
		for i := range perDatagram {
			m := msg(uint16(7200+i), int(s%50))
			if i == 1 {
				subs = append(subs, comp(lzoLiteral(t, withPrefix(netID, m))))
			} else {
				subs = append(subs, plain(withPrefix(netID, m)))
			}
		}
		dgs = append(dgs, dgram(t, s, perDatagram, subs...))
	}

	got, st := runPool(t, 4, dgs)

	if len(got) != datagrams*perDatagram {
		t.Fatalf("got %d packets, want %d", len(got), datagrams*perDatagram)
	}
	next := map[uint64]uint16{} // next Index expected per Seq
	for _, p := range got {
		if p.Index != next[p.Seq] {
			t.Fatalf("Seq %d: Index %d arrived when %d was expected", p.Seq, p.Index, next[p.Seq])
		}
		next[p.Seq]++
		if w := msg(uint16(7200+int(p.Index)), int(p.Seq%50)); !bytes.Equal(p.Data[:p.Len], w) {
			t.Fatalf("Seq %d Index %d: data mismatch", p.Seq, p.Index)
		}
	}
	if len(next) != datagrams {
		t.Errorf("packets from %d datagrams, want %d", len(next), datagrams)
	}
	if c := snap(st); c != (counts{Datagrams: datagrams, Packets: datagrams * perDatagram}) {
		t.Errorf("stats = %+v", c)
	}
	for i := range perDatagram {
		if n := st.Transcodes.Get(uint16(7200 + i)); n != datagrams {
			t.Errorf("Transcodes.Get(%d) = %d, want %d", 7200+i, n, datagrams)
		}
	}
}
