package split_test

import (
	"encoding/binary"
	"testing"

	"feedvalve/internal/decode"
	"feedvalve/internal/mirror"
	"feedvalve/internal/router"
	"feedvalve/internal/split"
	"feedvalve/internal/stats"
	"feedvalve/internal/unpack"
)

// onlyMBP builds a 7208 packet of length size with NoOfRecords count, whose
// record slots carry tokens 1001 and 1002.
func onlyMBP(size int, count int16) *unpack.Packet {
	p := &unpack.Packet{Seq: 42, Index: 3, Code: 7208, Len: uint16(size)}
	binary.BigEndian.PutUint16(p.Data[10:], 7208)
	binary.BigEndian.PutUint16(p.Data[38:], uint16(size))
	binary.BigEndian.PutUint16(p.Data[40:], uint16(count))
	binary.BigEndian.PutUint32(p.Data[42:], 1001)     // record 0 token
	binary.BigEndian.PutUint32(p.Data[42+214:], 1002) // record 1 token
	return p
}

// call is one invocation of the test mapper.
type call struct {
	rec   uint16
	token int32
}

// harness wires split.OnlyMBP into a router over two shards, with a mapper
// that records each call and sends one unit per record.
func harness() (*router.Router, *[]call, []chan mirror.Contribution, *split.Stats) {
	calls := new([]call)
	st := new(split.Stats)
	mapRec := func(p *unpack.Packet, rec uint16, r decode.OnlyMBP, rt *router.Router) {
		*calls = append(*calls, call{rec, r.Token})
		var u mirror.Contribution
		u = mirror.Contribution{Seq: p.Seq, Index: p.Index, Record: rec, Code: p.Code, Token: r.Token}
		rt.Send(&u)
	}
	qs := []chan mirror.Contribution{make(chan mirror.Contribution, 16), make(chan mirror.Contribution, 16)}
	rtr := router.New(
		map[uint16]router.Splitter{7208: split.OnlyMBP(mapRec, st)},
		[]chan<- mirror.Contribution{qs[0], qs[1]},
		stats.NewTokenCounts(4),
		new(router.Stats))
	return rtr, calls, qs, st
}

func TestOnlyMBP(t *testing.T) {
	tests := []struct {
		name      string
		p         *unpack.Packet
		wantCalls []call
		wantStats [3]uint64 // Records, Clamped, BadLength
	}{
		{"two records, in order", onlyMBP(470, 2), []call{{0, 1001}, {1, 1002}}, [3]uint64{2, 0, 0}},
		{"one record", onlyMBP(470, 1), []call{{0, 1001}}, [3]uint64{1, 0, 0}},
		{"count clamped", onlyMBP(470, 9), []call{{0, 1001}, {1, 1002}}, [3]uint64{2, 1, 0}},
		{"bad length never reaches the mapper", onlyMBP(468, 2), nil, [3]uint64{0, 0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rtr, calls, qs, st := harness()

			rtr.Route(tt.p)

			if len(*calls) != len(tt.wantCalls) {
				t.Fatalf("mapper calls = %v, want %v", *calls, tt.wantCalls)
			}
			for i, c := range tt.wantCalls {
				if (*calls)[i] != c {
					t.Errorf("call %d = %v, want %v", i, (*calls)[i], c)
				}
			}
			units := len(qs[0]) + len(qs[1])
			if units != len(tt.wantCalls) {
				t.Errorf("%d units reached the shards, want %d", units, len(tt.wantCalls))
			}
			for _, q := range qs {
				for len(q) > 0 {
					if u := <-q; u.Seq != 42 || u.Index != 3 || u.Code != 7208 {
						t.Errorf("unit %+v lost its packet identity", u)
					}
				}
			}
			if s := [3]uint64{st.Records.Load(), st.Clamped.Load(), st.BadLength.Load()}; s != tt.wantStats {
				t.Errorf("(Records, Clamped, BadLength) = %v, want %v", s, tt.wantStats)
			}
		})
	}
}

// The whole 7208 path, decode through the real mapper to the shard queue,
// must not allocate (ADR-006).
func TestOnlyMBPAllocatesNothing(t *testing.T) {
	st := new(split.Stats)
	mapRec := split.NewOnlyMBPMapper(loadSchema(t, allFields()))
	q := make(chan mirror.Contribution, 4)
	rtr := router.New(map[uint16]router.Splitter{7208: split.OnlyMBP(mapRec, st)},
		[]chan<- mirror.Contribution{q}, stats.NewTokenCounts(4), new(router.Stats))
	p := onlyMBP(470, 2)

	allocs := testing.AllocsPerRun(100, func() {
		rtr.Route(p)
		for len(q) > 0 {
			<-q
		}
	})
	if allocs != 0 {
		t.Errorf("7208 path allocates %.0f times per packet, want 0", allocs)
	}
}
