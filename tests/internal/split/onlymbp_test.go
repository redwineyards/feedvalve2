package split_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"feedvalve/internal/mirror"
	"feedvalve/internal/router"
	"feedvalve/internal/split"
	"feedvalve/internal/unpack"
)

// putRecord writes the mapped fields of a 7208 record for token into record
// slot rec of p, at the offsets of bible §4.8 (record base 42 + rec*214), with
// distinct values derived from token.
func putRecord(p *unpack.Packet, rec int, token int32) {
	base := 42 + rec*214
	i32 := func(off int, v int32) { binary.BigEndian.PutUint32(p.Data[base+off:], uint32(v)) }
	f64 := func(off int, v float64) { binary.BigEndian.PutUint64(p.Data[base+off:], math.Float64bits(v)) }

	i32(0, token)
	i32(8, token+1)       // VolumeTradedToday
	i32(12, token+2)      // LastTradedPrice
	p.Data[base+16] = '+' // NetChangeIndicator
	i32(18, token+3)      // NetPriceChangeFromClosingPrice
	i32(22, token+4)      // LastTradeQuantity
	i32(26, token+5)      // LastTradeTime
	i32(30, token+6)      // AverageTradePrice
	for i := range 10 {   // 5 buy levels, then 5 sell levels
		off := 56 + i*12
		i32(off, token+100+int32(i))   // Quantity
		i32(off+4, token+200+int32(i)) // Price
	}
	f64(180, 1500.6)   // TotalBuyQuantity
	f64(188, 800.4)    // TotalSellQuantity
	i32(198, token+7)  // ClosingPrice
	i32(202, token+8)  // OpenPrice
	i32(206, token+9)  // HighPrice
	i32(210, token+10) // LowPrice
}

// wantQuote is the quote putRecord's record for token should map to.
func wantQuote(token int32) mirror.Quote {
	t := int64(token)
	q := mirror.Quote{
		VolumeTradedToday: t + 1, LastTradedPrice: t + 2, NetChangeIndicator: '+',
		LastTradeQuantity: t + 4, LastTradeTime: t + 5,
		AverageTradePrice: t + 6, TotalBuyQuantity: 1501, TotalSellQuantity: 800,
		PctChangePrevClose: -50, // (LTP - close) / close = -5 / 1008 or 1009: -0.50%
		ClosingPrice:       t + 7, OpenPrice: t + 8, HighPrice: t + 9, LowPrice: t + 10,
	}
	for i := range 5 {
		q.BuyQuantity[i] = t + 100 + int64(i)
		q.BuyPrice[i] = t + 200 + int64(i)
		q.SellQuantity[i] = t + 105 + int64(i)
		q.SellPrice[i] = t + 205 + int64(i)
	}
	return q
}

// onlyMBP builds a 7208 packet of length size with NoOfRecords count, whose
// record slots carry tokens 1001 and 1002.
func onlyMBP(size int, count int16) *unpack.Packet {
	p := &unpack.Packet{Seq: 42, Index: 3, Code: 7208, Len: uint16(size)}
	binary.BigEndian.PutUint16(p.Data[10:], 7208)
	binary.BigEndian.PutUint16(p.Data[38:], uint16(size))
	binary.BigEndian.PutUint16(p.Data[40:], uint16(count))
	putRecord(p, 0, 1001)
	putRecord(p, 1, 1002)
	return p
}

// harness wires split.OnlyMBP into a router over one shard queue.
func harness() (*router.Router, chan mirror.Contribution, *split.Stats) {
	st := new(split.Stats)
	q := make(chan mirror.Contribution, 16)
	rtr := router.New(
		map[uint16]router.Splitter{7208: split.OnlyMBP(st)},
		[]chan<- mirror.Contribution{q},
		new(router.Stats))
	return rtr, q, st
}

func TestOnlyMBP(t *testing.T) {
	tests := []struct {
		name       string
		p          *unpack.Packet
		wantTokens []int32
		wantStats  [3]uint64 // Records, Clamped, BadLength
	}{
		{"two records, in order", onlyMBP(470, 2), []int32{1001, 1002}, [3]uint64{2, 0, 0}},
		{"one record", onlyMBP(470, 1), []int32{1001}, [3]uint64{1, 0, 0}},
		{"count clamped", onlyMBP(470, 9), []int32{1001, 1002}, [3]uint64{2, 1, 0}},
		{"short one-record message", onlyMBP(256, 1), []int32{1001}, [3]uint64{1, 0, 0}},
		{"count above the records that fit", onlyMBP(256, 2), []int32{1001}, [3]uint64{1, 1, 0}},
		{"too short for the count sends nothing", onlyMBP(41, 0), nil, [3]uint64{0, 0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rtr, q, st := harness()

			rtr.Route(tt.p)

			if len(q) != len(tt.wantTokens) {
				t.Fatalf("%d contributions sent, want %d", len(q), len(tt.wantTokens))
			}
			for i, tok := range tt.wantTokens {
				c := <-q
				if c.Seq != 42 || c.Index != 3 || c.Record != uint16(i) || c.Code != 7208 || c.Token != tok {
					t.Errorf("contribution %d identity = %+v, want seq 42, index 3, record %d, code 7208, token %d", i, c, i, tok)
				}
				if want := wantQuote(tok); c.Quote != want {
					t.Errorf("contribution %d quote = %+v, want %+v", i, c.Quote, want)
				}
			}
			if s := [3]uint64{st.Records.Load(), st.Clamped.Load(), st.BadLength.Load()}; s != tt.wantStats {
				t.Errorf("(Records, Clamped, BadLength) = %v, want %v", s, tt.wantStats)
			}
		})
	}
}

// The whole 7208 path, decode through mapping to the shard queue, must not
// allocate (ADR-006).
func TestOnlyMBPAllocatesNothing(t *testing.T) {
	rtr, q, _ := harness()
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

// ABC_CHNG_PRVS_CLS is the % change of LTP from ClosingPrice, in hundredths
// of a percent, rounded; 0 when there is no close.
func TestOnlyMBPPctChangePrevClose(t *testing.T) {
	tests := []struct {
		name       string
		ltp, close int32
		want       int64
	}{
		{"up 1.23%", 121476, 120000, 123},
		{"down 0.5%", 99500, 100000, -50},
		{"half a hundredth up rounds away from zero", 20001, 20000, 1},
		{"half a hundredth down rounds away from zero", 19999, 20000, -1},
		{"unchanged", 50000, 50000, 0},
		{"no close", 50000, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rtr, q, _ := harness()
			p := onlyMBP(470, 1)
			binary.BigEndian.PutUint32(p.Data[42+12:], uint32(tt.ltp))    // LastTradedPrice
			binary.BigEndian.PutUint32(p.Data[42+198:], uint32(tt.close)) // ClosingPrice
			rtr.Route(p)
			if len(q) != 1 {
				t.Fatalf("%d contributions sent, want 1", len(q))
			}
			if got := (<-q).Quote.PctChangePrevClose; got != tt.want {
				t.Errorf("PctChangePrevClose = %d, want %d", got, tt.want)
			}
		})
	}
}

// A record with a value that would not fit its NUMBER(10) column is dropped
// and counted, so Oracle never rejects a row; the message's other record is
// still sent.
func TestOnlyMBPDropsOutOfRange(t *testing.T) {
	tests := []struct {
		name     string
		set      func(p *unpack.Packet) // changes record 0 only
		wantDrop bool
	}{
		{"total buy over the limit", func(p *unpack.Packet) { putF64(p, 180, 1e10) }, true},
		{"total sell under the limit", func(p *unpack.Packet) { putF64(p, 188, -1e10) }, true},
		{"total buy far over int64", func(p *unpack.Packet) { putF64(p, 180, 1e300) }, true},
		{"total buy just inside the limit", func(p *unpack.Packet) { putF64(p, 180, 9_999_999_999) }, false},
		{"% change over the limit", func(p *unpack.Packet) {
			binary.BigEndian.PutUint32(p.Data[42+12:], 2_000_000_000) // LTP
			binary.BigEndian.PutUint32(p.Data[42+198:], 1)            // ClosingPrice
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rtr, q, st := harness()
			p := onlyMBP(470, 2)
			tt.set(p)
			rtr.Route(p)

			var tokens []int32
			for len(q) > 0 {
				tokens = append(tokens, (<-q).Token)
			}
			want, wantDropped := []int32{1001, 1002}, uint64(0)
			if tt.wantDrop {
				want, wantDropped = []int32{1002}, 1
			}
			if fmt.Sprint(tokens) != fmt.Sprint(want) {
				t.Errorf("sent tokens %v, want %v", tokens, want)
			}
			if n := st.OutOfRange.Load(); n != wantDropped {
				t.Errorf("OutOfRange = %d, want %d", n, wantDropped)
			}
		})
	}
}

// putF64 writes v as a DOUBLE at offset off of record 0.
func putF64(p *unpack.Packet, off int, v float64) {
	binary.BigEndian.PutUint64(p.Data[42+off:], math.Float64bits(v))
}
