// Package decode parses broadcast messages into typed records exactly as the
// exchange sends them: prices stay scaled integers and times stay seconds
// since 1980. Turning a record into table values is a later stage's job.
//
// Every decoder reads field by field through a big-endian cursor and checks
// the message length first, so no read can run past the message: 7305 must be
// exactly its declared length, and 7208 only has the records that fit read.
package decode

import (
	"encoding/binary"
	"errors"
)

// OnlyMBPLen is the declared length of a 7208 message, header included
// (bible §4.8): 40 + 2 + 2×214. It is the full two-record layout; a 7208 may
// arrive shorter (see DecodeOnlyMBP).
const OnlyMBPLen = 470

const (
	headerLen       = 40  // BCAST_HEADER
	logTimeOff      = 4   // BCAST_HEADER.LogTime
	bcSeqNoOff      = 14  // BCAST_HEADER.BCSeqNo
	onlyMBPCountOff = 40  // NoOfRecords, right after the header
	onlyMBPRecOff   = 42  // first INTERACTIVE_ONLY_MBP_DATA
	onlyMBPRecLen   = 214 // declared 213; a pad byte at 17 makes it 214 (bible §4.8, Part V)
	onlyMBPMaxRecs  = 2
	mbpLevels       = 5 // MBP_INFORMATION[10] is 5 buys then 5 sells
)

// The record stride must reconcile with the declared length; if it ever
// stops doing so, one of these arrays gets a negative size and the build fails.
var (
	_ [OnlyMBPLen - (onlyMBPRecOff + onlyMBPMaxRecs*onlyMBPRecLen)]struct{}
	_ [(onlyMBPRecOff + onlyMBPMaxRecs*onlyMBPRecLen) - OnlyMBPLen]struct{}
)

// ErrBadLength is returned when a message's length cannot be decoded: a 7305
// that is not its declared length, or a 7208 too short for its header and
// NoOfRecords.
var ErrBadLength = errors.New("decode: message length differs from declared")

// MBPLevel is one MBP_INFORMATION entry: one price level of the book.
type MBPLevel struct {
	Quantity       int32
	Price          int32 // scaled (bible §2.4)
	NumberOfOrders int16
	BbBuySellFlag  int16 // currently always zero
}

// OnlyMBP is one 7208 record (INTERACTIVE_ONLY_MBP_DATA), plus the two
// header fields it needs, exactly as sent. Prices are scaled integers
// (bible §2.4) and times are seconds since 1980 (bible §2.5). It holds no
// pointers or slices (ADR-006).
type OnlyMBP struct {
	LogTime int32 // header: message generation time
	BCSeqNo int32 // header: broadcast sequence number

	Token                          int32
	BookType                       int16
	TradingStatus                  int16
	VolumeTradedToday              int32
	LastTradedPrice                int32
	NetChangeIndicator             byte  // '+' or '-'
	NetPriceChangeFromClosingPrice int32 // presently carries the closing price (bible §4.7)
	LastTradeQuantity              int32
	LastTradeTime                  int32
	AverageTradePrice              int32
	AuctionNumber                  int16
	AuctionStatus                  int16
	InitiatorType                  int16
	InitiatorPrice                 int32
	InitiatorQuantity              int32
	AuctionPrice                   int32
	AuctionQuantity                int32
	Buy                            [mbpLevels]MBPLevel // best buys, best first
	Sell                           [mbpLevels]MBPLevel // best sells, best first
	BbTotalBuyFlag                 int16               // currently always zero
	BbTotalSellFlag                int16               // currently always zero
	TotalBuyQuantity               float64
	TotalSellQuantity              float64
	IndLastTradeMore               bool // ST_INDICATOR bits, big-endian order (bible §4.14)
	IndLastTradeLess               bool
	IndBuy                         bool
	IndSell                        bool
	ClosingPrice                   int32
	OpenPrice                      int32
	HighPrice                      int32
	LowPrice                       int32
}

// DecodeOnlyMBP parses the 7208 message m, from the header onward, into out
// and returns how many records it holds. NoOfRecords is clamped to [0, 2]
// (bible §4.8) and to the whole records m holds; clamped reports whether
// either was needed, and a partial record is never decoded. Despite bible
// §4.8's "always 470 bytes", the exchange sends a one-record 7208 short, at
// 40 + 2 + 214 = 256 bytes, so any length is accepted. Only a message too
// short for the header and NoOfRecords returns ErrBadLength.
func DecodeOnlyMBP(m []byte, out *[onlyMBPMaxRecs]OnlyMBP) (n int, clamped bool, err error) {
	var (
		logTime int32  // header LogTime, copied into every record
		bcSeqNo int32  // header BCSeqNo, copied into every record
		fit     int    // whole records m has room for
		r       reader // cursor over the current record
	)

	if len(m) < onlyMBPRecOff {
		return 0, false, ErrBadLength
	}
	fit = (len(m) - onlyMBPRecOff) / onlyMBPRecLen
	logTime = int32(binary.BigEndian.Uint32(m[logTimeOff:]))
	bcSeqNo = int32(binary.BigEndian.Uint32(m[bcSeqNoOff:]))
	n = int(int16(binary.BigEndian.Uint16(m[onlyMBPCountOff:])))
	if n < 0 {
		n, clamped = 0, true
	}
	if n > onlyMBPMaxRecs {
		n, clamped = onlyMBPMaxRecs, true
	}
	if n > fit {
		n, clamped = fit, true
	}

	for i := range n {
		r = reader{b: m, off: onlyMBPRecOff + i*onlyMBPRecLen}
		readOnlyMBP(&r, &out[i])
		out[i].LogTime = logTime
		out[i].BCSeqNo = bcSeqNo
	}
	return n, clamped, nil
}

// readOnlyMBP reads one INTERACTIVE_ONLY_MBP_DATA record, 214 bytes, into rec.
func readOnlyMBP(r *reader, rec *OnlyMBP) {
	var (
		ind byte // ST_INDICATOR flag byte
	)

	rec.Token = r.i32()
	rec.BookType = r.i16()
	rec.TradingStatus = r.i16()
	rec.VolumeTradedToday = r.i32()
	rec.LastTradedPrice = r.i32()
	rec.NetChangeIndicator = r.u8()
	r.pad("after NetChangeIndicator", 1)
	rec.NetPriceChangeFromClosingPrice = r.i32()
	rec.LastTradeQuantity = r.i32()
	rec.LastTradeTime = r.i32()
	rec.AverageTradePrice = r.i32()
	rec.AuctionNumber = r.i16()
	rec.AuctionStatus = r.i16()
	rec.InitiatorType = r.i16()
	rec.InitiatorPrice = r.i32()
	rec.InitiatorQuantity = r.i32()
	rec.AuctionPrice = r.i32()
	rec.AuctionQuantity = r.i32()
	for i := range rec.Buy {
		readMBPLevel(r, &rec.Buy[i])
	}
	for i := range rec.Sell {
		readMBPLevel(r, &rec.Sell[i])
	}
	rec.BbTotalBuyFlag = r.i16()
	rec.BbTotalSellFlag = r.i16()
	rec.TotalBuyQuantity = r.f64()
	rec.TotalSellQuantity = r.f64()
	ind = r.u8()
	r.pad("ST_INDICATOR reserved byte", 1)
	rec.IndLastTradeMore = ind&0x80 != 0
	rec.IndLastTradeLess = ind&0x40 != 0
	rec.IndBuy = ind&0x20 != 0
	rec.IndSell = ind&0x10 != 0
	rec.ClosingPrice = r.i32()
	rec.OpenPrice = r.i32()
	rec.HighPrice = r.i32()
	rec.LowPrice = r.i32()
}

// readMBPLevel reads one 12-byte MBP_INFORMATION entry into lv.
func readMBPLevel(r *reader, lv *MBPLevel) {
	lv.Quantity = r.i32()
	lv.Price = r.i32()
	lv.NumberOfOrders = r.i16()
	lv.BbBuySellFlag = r.i16()
}
