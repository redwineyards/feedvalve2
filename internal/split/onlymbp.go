// Package split holds the per-transcode Splitters: each decodes one message
// type and turns its records, one by one, into contributions for the shards.
package split

import (
	"log/slog"
	"math"
	"sync/atomic"

	"feedvalve/internal/decode"
	"feedvalve/internal/mirror"
	"feedvalve/internal/router"
	"feedvalve/internal/unpack"
)

// Stats counts what one splitter has seen; each splitter has its own. Safe
// to read concurrently.
type Stats struct {
	Records    atomic.Uint64 // records sent as contributions
	Clamped    atomic.Uint64 // messages whose record count had to be clamped
	BadLength  atomic.Uint64 // messages not their declared length; dropped
	OutOfRange atomic.Uint64 // records with a value too large for its column; dropped
}

// maxNumber10 is the largest magnitude a NUMBER(10) column holds. Every
// int32 field fits; only computed or DOUBLE-derived values need checking.
const maxNumber10 = 9_999_999_999

// OnlyMBP returns the 7208 Splitter: it decodes the message and sends one
// contribution per record, in order, keyed by the record's token. A message
// of the wrong length is counted and dropped (HLD §6.4); a clamped record
// count is counted and logged. A record with a value its column cannot hold
// is counted and dropped, so Oracle never rejects a row and a failed batch
// only ever means the database itself failed.
func OnlyMBP(st *Stats) router.Splitter {
	return func(p *unpack.Packet, rt *router.Router) {
		var (
			recs    [2]decode.OnlyMBP   // the message's records, on the stack
			n       int                 // records in the message
			clamped bool                // whether NoOfRecords was out of range
			c       mirror.Contribution // the contribution being sent
			bad     string              // the field out of range, if any
			badVal  float64             // its value
			err     error               // decode result
		)

		n, clamped, err = decode.DecodeOnlyMBP(p.Data[:p.Len], &recs)
		if err != nil {
			st.BadLength.Add(1)
			slog.Warn("7208 dropped", "seq", p.Seq, "index", p.Index, "len", p.Len, "want", decode.OnlyMBPLen, "err", err)
			return
		}
		if clamped {
			st.Clamped.Add(1)
			slog.Warn("7208 NoOfRecords clamped", "seq", p.Seq, "index", p.Index, "records", n)
		}
		for i := range n {
			c = mirror.Contribution{Seq: p.Seq, Index: p.Index, Record: uint16(i), Code: p.Code, Token: recs[i].Token, Part: mirror.PartQuote}
			bad, badVal = quoteOf(&recs[i], &c.Quote)
			if bad != "" {
				st.OutOfRange.Add(1)
				slog.Warn("7208 record dropped: value too large for its column",
					"seq", p.Seq, "index", p.Index, "token", recs[i].Token, "field", bad, "value", badVal)
				continue
			}
			st.Records.Add(1)
			rt.Send(&c)
		}
	}
}

// quoteOf fills q with the ABC_TRADE_QUOTE values of 7208 record r, as sent,
// except that the DOUBLE buy/sell totals are rounded and the percentage change
// from the closing price is computed. NetPriceChangeFromClosingPrice is not
// used: despite its name it carries the closing price (bible §4.7). If a
// computed value is beyond maxNumber10 (or NaN), it returns that field's name
// and value, and q must not be written.
func quoteOf(r *decode.OnlyMBP, q *mirror.Quote) (string, float64) {
	var (
		buy  float64 // TotalBuyQuantity, rounded
		sell float64 // TotalSellQuantity, rounded
		pct  float64 // % change from close, in hundredths of a percent, rounded
	)

	buy = math.Round(r.TotalBuyQuantity)
	sell = math.Round(r.TotalSellQuantity)
	// (LTP − close) / close × 100, in hundredths of a percent, rounded half
	// away from zero. Subtracting in float64 cannot overflow, and a 7208 only
	// arrives after the first trade, so LTP is never 0 here.
	if r.ClosingPrice != 0 {
		pct = math.Round((float64(r.LastTradedPrice) - float64(r.ClosingPrice)) * 10000 / float64(r.ClosingPrice))
	}
	// Checked as float64, before converting: a float beyond int64 has no
	// defined conversion. !(x <= max) also catches NaN.
	switch {
	case !(math.Abs(buy) <= maxNumber10):
		return "TotalBuyQuantity", r.TotalBuyQuantity
	case !(math.Abs(sell) <= maxNumber10):
		return "TotalSellQuantity", r.TotalSellQuantity
	case !(math.Abs(pct) <= maxNumber10):
		return "PctChangePrevClose", pct
	}

	*q = mirror.Quote{
		LastTradedPrice:    int64(r.LastTradedPrice),
		LastTradeQuantity:  int64(r.LastTradeQuantity),
		LastTradeTime:      int64(r.LastTradeTime),
		AverageTradePrice:  int64(r.AverageTradePrice),
		VolumeTradedToday:  int64(r.VolumeTradedToday),
		NetChangeIndicator: int64(r.NetChangeIndicator),
		PctChangePrevClose: int64(pct),
		ClosingPrice:       int64(r.ClosingPrice),
		OpenPrice:          int64(r.OpenPrice),
		HighPrice:          int64(r.HighPrice),
		LowPrice:           int64(r.LowPrice),
		TotalBuyQuantity:   int64(buy),
		TotalSellQuantity:  int64(sell),
	}
	for i := range r.Buy {
		q.BuyPrice[i] = int64(r.Buy[i].Price)
		q.BuyQuantity[i] = int64(r.Buy[i].Quantity)
		q.SellPrice[i] = int64(r.Sell[i].Price)
		q.SellQuantity[i] = int64(r.Sell[i].Quantity)
	}
	return "", 0
}
