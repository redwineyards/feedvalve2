package split_test

import (
	"encoding/binary"
	"testing"

	"feedvalve/internal/mirror"
	"feedvalve/internal/router"
	"feedvalve/internal/split"
	"feedvalve/internal/unpack"
)

// secPacket builds a 7305 packet of length size for token 777, with distinct
// values in every mapped field (offsets from bible §4.3) and multiplier mult.
func secPacket(size int, mult int32) *unpack.Packet {
	p := &unpack.Packet{Seq: 9, Index: 2, Code: 7305, Len: uint16(size)}
	i32 := func(off int, v int32) { binary.BigEndian.PutUint32(p.Data[off:], uint32(v)) }
	binary.BigEndian.PutUint16(p.Data[10:], 7305)
	binary.BigEndian.PutUint16(p.Data[38:], uint16(size))
	i32(40, 777)    // Token
	i32(88, 5000)   // FreezeQuantity
	i32(134, 10)    // MinimumLotQuantity
	i32(138, 100)   // BoardLotQuantity
	i32(142, 5)     // TickSize
	i32(188, 90000) // LowPriceRange
	i32(192, 99000) // HighPriceRange
	i32(226, mult)  // Multiplier
	p.Data[264] = 'N'
	copy(p.Data[265:290], "SPLIT 1:2                ") // Remark, blank-padded
	i32(290, 95000)                                    // BasePrice
	return p
}

// wantMaster is the Master secPacket's record maps to.
func wantMaster(mult int64) mirror.Master {
	m := mirror.Master{
		HighPriceRange: 99000, LowPriceRange: 90000, BasePrice: 95000, Multiplier: mult,
		MinimumLotQuantity: 10, BoardLotQuantity: 100, TickSize: 5, FreezeQuantity: 5000, DeleteFlag: 'N',
	}
	copy(m.Remark[:], "SPLIT 1:2                ")
	return m
}

// secHarness wires split.SecurityUpdate into a router over one shard queue.
func secHarness() (*router.Router, chan mirror.Contribution, *split.Stats) {
	st := new(split.Stats)
	q := make(chan mirror.Contribution, 4)
	rtr := router.New(map[uint16]router.Splitter{7305: split.SecurityUpdate(st)},
		[]chan<- mirror.Contribution{q}, new(router.Stats))
	return rtr, q, st
}

func TestSecurityUpdate(t *testing.T) {
	tests := []struct {
		name      string
		size      int
		mult      int32
		wantSent  bool
		wantStats [3]uint64 // Records, BadLength, OutOfRange
	}{
		{"master part sent", 294, 1, true, [3]uint64{1, 0, 0}},
		{"multiplier at the NUMBER(5) limit", 294, 99999, true, [3]uint64{1, 0, 0}},
		{"multiplier over the NUMBER(5) limit", 294, 100000, false, [3]uint64{0, 0, 1}},
		{"negative multiplier over the limit", 294, -100000, false, [3]uint64{0, 0, 1}},
		{"bad length dropped", 293, 1, false, [3]uint64{0, 1, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rtr, q, st := secHarness()
			rtr.Route(secPacket(tt.size, tt.mult))
			if tt.wantSent {
				if len(q) != 1 {
					t.Fatalf("%d contributions sent, want 1", len(q))
				}
				c := <-q
				if c.Seq != 9 || c.Index != 2 || c.Record != 0 || c.Code != 7305 || c.Token != 777 || c.Part != mirror.PartMaster {
					t.Errorf("identity = %+v, want seq 9, index 2, record 0, code 7305, token 777, master part", c)
				}
				if want := wantMaster(int64(tt.mult)); c.Master != want {
					t.Errorf("master = %+v, want %+v", c.Master, want)
				}
			} else if len(q) != 0 {
				t.Errorf("%d contributions sent, want none", len(q))
			}
			if s := [3]uint64{st.Records.Load(), st.BadLength.Load(), st.OutOfRange.Load()}; s != tt.wantStats {
				t.Errorf("(Records, BadLength, OutOfRange) = %v, want %v", s, tt.wantStats)
			}
		})
	}
}

func TestSecurityUpdateAllocatesNothing(t *testing.T) {
	rtr, q, _ := secHarness()
	p := secPacket(294, 1)
	allocs := testing.AllocsPerRun(100, func() {
		rtr.Route(p)
		for len(q) > 0 {
			<-q
		}
	})
	if allocs != 0 {
		t.Errorf("7305 path allocates %.0f times per packet, want 0", allocs)
	}
}
