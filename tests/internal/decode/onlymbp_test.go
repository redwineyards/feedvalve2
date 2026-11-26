package decode_test

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"feedvalve/internal/decode"
)

// sample returns a record whose every field differs from every other field
// and from the same field in other records, so a misplaced read shows up.
func sample(rec int) decode.OnlyMBP {
	b := int32(rec * 100000)
	r := decode.OnlyMBP{
		LogTime: 1400000000, BCSeqNo: 777,
		Token: 50000 + b, BookType: int16(1 + rec), TradingStatus: int16(2 + rec),
		VolumeTradedToday: 10 + b, LastTradedPrice: -(11 + b), NetChangeIndicator: '+',
		NetPriceChangeFromClosingPrice: 12 + b, LastTradeQuantity: 13 + b, LastTradeTime: 14 + b, AverageTradePrice: 15 + b,
		AuctionNumber: int16(16 + rec), AuctionStatus: int16(17 + rec), InitiatorType: int16(18 + rec),
		InitiatorPrice: 19 + b, InitiatorQuantity: 20 + b, AuctionPrice: 21 + b, AuctionQuantity: 22 + b,
		BbTotalBuyFlag: int16(23 + rec), BbTotalSellFlag: int16(24 + rec),
		TotalBuyQuantity: 1234567.25 + float64(rec), TotalSellQuantity: 7654321.5 + float64(rec),
		IndLastTradeMore: rec == 0, IndLastTradeLess: rec == 1, IndBuy: true, IndSell: rec == 1,
		ClosingPrice: 25 + b, OpenPrice: 26 + b, HighPrice: 27 + b, LowPrice: -(28 + b),
	}
	for i := range 5 {
		r.Buy[i] = decode.MBPLevel{Quantity: int32(30+i) + b, Price: int32(40+i) + b, NumberOfOrders: int16(50 + i), BbBuySellFlag: int16(60 + i)}
		r.Sell[i] = decode.MBPLevel{Quantity: int32(70+i) + b, Price: int32(80+i) + b, NumberOfOrders: int16(90 + i), BbBuySellFlag: int16(100 + i)}
	}
	return r
}

// put writes r at record slot rec of message m, using the absolute offsets of
// bible §4.8 (record base 42 + rec*214, pad byte at 17) rather than any
// cursor, so the test is independent of the decoder's reading order.
func put(m []byte, rec int, r decode.OnlyMBP) {
	base := 42 + rec*214
	i16 := func(off int, v int16) { binary.BigEndian.PutUint16(m[base+off:], uint16(v)) }
	i32 := func(off int, v int32) { binary.BigEndian.PutUint32(m[base+off:], uint32(v)) }
	f64 := func(off int, v float64) { binary.BigEndian.PutUint64(m[base+off:], math.Float64bits(v)) }

	i32(0, r.Token)
	i16(4, r.BookType)
	i16(6, r.TradingStatus)
	i32(8, r.VolumeTradedToday)
	i32(12, r.LastTradedPrice)
	m[base+16] = r.NetChangeIndicator
	m[base+17] = 0xEE // pad: must be ignored
	i32(18, r.NetPriceChangeFromClosingPrice)
	i32(22, r.LastTradeQuantity)
	i32(26, r.LastTradeTime)
	i32(30, r.AverageTradePrice)
	i16(34, r.AuctionNumber)
	i16(36, r.AuctionStatus)
	i16(38, r.InitiatorType)
	i32(40, r.InitiatorPrice)
	i32(44, r.InitiatorQuantity)
	i32(48, r.AuctionPrice)
	i32(52, r.AuctionQuantity)
	for i, lv := range append(r.Buy[:], r.Sell[:]...) {
		off := 56 + i*12
		i32(off, lv.Quantity)
		i32(off+4, lv.Price)
		i16(off+8, lv.NumberOfOrders)
		i16(off+10, lv.BbBuySellFlag)
	}
	i16(176, r.BbTotalBuyFlag)
	i16(178, r.BbTotalSellFlag)
	f64(180, r.TotalBuyQuantity)
	f64(188, r.TotalSellQuantity)
	var ind byte
	for bit, set := range []bool{r.IndLastTradeMore, r.IndLastTradeLess, r.IndBuy, r.IndSell} {
		if set {
			ind |= 0x80 >> bit
		}
	}
	m[base+196] = ind
	m[base+197] = 0xEE // reserved byte of ST_INDICATOR
	i32(198, r.ClosingPrice)
	i32(202, r.OpenPrice)
	i32(206, r.HighPrice)
	i32(210, r.LowPrice)
}

// message builds a 7208 message of length size with NoOfRecords count and
// every record slot that fits whole filled from sample. A size too small for
// the header and count is left all zero.
func message(size int, count int16) []byte {
	m := make([]byte, size)
	if size < 42 {
		return m
	}
	binary.BigEndian.PutUint32(m[4:], 1400000000) // LogTime
	binary.BigEndian.PutUint16(m[10:], 7208)      // TransactionCode
	binary.BigEndian.PutUint32(m[14:], 777)       // BCSeqNo
	binary.BigEndian.PutUint16(m[38:], uint16(size))
	binary.BigEndian.PutUint16(m[40:], uint16(count))
	for i := 0; i < 2 && 42+(i+1)*214 <= size; i++ {
		put(m, i, sample(i))
	}
	return m
}

func TestDecodeOnlyMBP(t *testing.T) {
	tests := []struct {
		name        string
		m           []byte
		wantN       int
		wantClamped bool
		wantErr     error
	}{
		{"two records", message(470, 2), 2, false, nil},
		{"one record", message(470, 1), 1, false, nil},
		{"zero records", message(470, 0), 0, false, nil},
		{"count above the array bound", message(470, 3), 2, true, nil},
		{"negative count", message(470, -1), 0, true, nil},
		// The exchange sends a one-record 7208 short: 40 + 2 + 214 bytes.
		{"one record, short message", message(256, 1), 1, false, nil},
		{"header and count only", message(42, 0), 0, false, nil},
		{"count above the records that fit", message(256, 2), 1, true, nil},
		{"partial second record is not decoded", message(469, 2), 1, true, nil},
		{"trailing bytes ignored", message(471, 2), 2, false, nil},
		{"too short for the count", message(41, 0), 0, false, decode.ErrBadLength},
		{"empty", nil, 0, false, decode.ErrBadLength},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out [2]decode.OnlyMBP
			n, clamped, err := decode.DecodeOnlyMBP(tt.m, &out)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if n != tt.wantN || clamped != tt.wantClamped {
				t.Errorf("(n, clamped) = (%d, %v), want (%d, %v)", n, clamped, tt.wantN, tt.wantClamped)
			}
			for i := range n {
				if want := sample(i); out[i] != want {
					t.Errorf("record %d:\n got  %+v\n want %+v", i, out[i], want)
				}
			}
		})
	}
}

func TestDecodeOnlyMBPAllocatesNothing(t *testing.T) {
	m := message(470, 2)
	var out [2]decode.OnlyMBP
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, err := decode.DecodeOnlyMBP(m, &out); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("DecodeOnlyMBP allocates %.0f times per call, want 0", allocs)
	}
}

func BenchmarkDecodeOnlyMBP(b *testing.B) {
	m := message(470, 2)
	var out [2]decode.OnlyMBP
	b.ReportAllocs()
	for range b.N {
		if _, _, err := decode.DecodeOnlyMBP(m, &out); err != nil {
			b.Fatal(err)
		}
	}
}
