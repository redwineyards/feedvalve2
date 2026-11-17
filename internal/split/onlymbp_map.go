package split

import (
	"math"

	"feedvalve/internal/decode"
	"feedvalve/internal/mirror"
	"feedvalve/internal/router"
	"feedvalve/internal/schema"
	"feedvalve/internal/unpack"
)

// OnlyMBPFields names the mirror fields a 7208 record provides, for use in
// the column-mapping file. Values are as the exchange sends them: prices and
// quantities as integers, times in seconds since 1980, the net change
// indicator as its byte code, and the DOUBLE buy/sell totals rounded.
var OnlyMBPFields = []string{
	"lastTradedPrice", "lastTradeQuantity", "lastTradeTime", "averageTradePrice",
	"volumeTradedToday", "netChangeIndicator", "netPriceChangeFromClosingPrice",
	"closingPrice", "openPrice", "highPrice", "lowPrice",
	"totalBuyQuantity", "totalSellQuantity",
	"buyPrice1", "buyPrice2", "buyPrice3", "buyPrice4", "buyPrice5",
	"buyQuantity1", "buyQuantity2", "buyQuantity3", "buyQuantity4", "buyQuantity5",
	"sellPrice1", "sellPrice2", "sellPrice3", "sellPrice4", "sellPrice5",
	"sellQuantity1", "sellQuantity2", "sellQuantity3", "sellQuantity4", "sellQuantity5",
}

// Positions in OnlyMBPFields, which onlyMBPValues fills in the same order.
const (
	fLastTradedPrice = iota
	fLastTradeQuantity
	fLastTradeTime
	fAverageTradePrice
	fVolumeTradedToday
	fNetChangeIndicator
	fNetPriceChange
	fClosingPrice
	fOpenPrice
	fHighPrice
	fLowPrice
	fTotalBuyQuantity
	fTotalSellQuantity
	fBuyPrice         // 5 levels
	fBuyQuantity      = fBuyPrice + 5
	fSellPrice        = fBuyQuantity + 5
	fSellQuantity     = fSellPrice + 5
	onlyMBPFieldCount = fSellQuantity + 5
)

// The list and the constants must agree; if they ever stop doing so, one of
// these arrays gets a negative size and the build fails.
var (
	_ [onlyMBPFieldCount - 33]struct{}
	_ [33 - onlyMBPFieldCount]struct{}
)

// NewOnlyMBPMapper returns the 7208 mapper for schema s. Field names are
// looked up once, here. Per record it sends one contribution to each
// destination that holds any 7208 field, keyed by the record's token. A last
// trade time of 0 means no trade yet and is not written.
func NewOnlyMBPMapper(s *schema.Schema) OnlyMBPMapper {
	var (
		refs  [onlyMBPFieldCount][]schema.Ref // where each field is written
		used  [mirror.Dests]bool              // destinations holding any field
		dests []uint8                         // the same, in order
	)

	for f, name := range OnlyMBPFields {
		refs[f] = s.Refs(name)
		for _, r := range refs[f] {
			used[r.Dest] = true
		}
	}
	for d, u := range used {
		if u {
			dests = append(dests, uint8(d))
		}
	}

	return func(p *unpack.Packet, rec uint16, r decode.OnlyMBP, rt *router.Router) {
		var (
			vals [onlyMBPFieldCount]int64 // the record's field values
			c    mirror.Contribution      // the contribution being built
		)

		onlyMBPValues(&r, &vals)
		for _, d := range dests {
			c = mirror.Contribution{Seq: p.Seq, Index: p.Index, Record: rec, Code: p.Code,
				Dest: d, Token: r.Token, RowKey: int64(r.Token)}
			for f := range vals {
				if f == fLastTradeTime && vals[f] == 0 {
					continue
				}
				for _, ref := range refs[f] {
					if ref.Dest == d {
						c.Set(ref.Slot, vals[f])
					}
				}
			}
			if c.Mask != 0 {
				rt.Send(&c)
			}
		}
	}
}

// onlyMBPValues puts r's field values into v, in OnlyMBPFields order.
func onlyMBPValues(r *decode.OnlyMBP, v *[onlyMBPFieldCount]int64) {
	v[fLastTradedPrice] = int64(r.LastTradedPrice)
	v[fLastTradeQuantity] = int64(r.LastTradeQuantity)
	v[fLastTradeTime] = int64(r.LastTradeTime)
	v[fAverageTradePrice] = int64(r.AverageTradePrice)
	v[fVolumeTradedToday] = int64(r.VolumeTradedToday)
	v[fNetChangeIndicator] = int64(r.NetChangeIndicator)
	v[fNetPriceChange] = int64(r.NetPriceChangeFromClosingPrice)
	v[fClosingPrice] = int64(r.ClosingPrice)
	v[fOpenPrice] = int64(r.OpenPrice)
	v[fHighPrice] = int64(r.HighPrice)
	v[fLowPrice] = int64(r.LowPrice)
	v[fTotalBuyQuantity] = int64(math.Round(r.TotalBuyQuantity))
	v[fTotalSellQuantity] = int64(math.Round(r.TotalSellQuantity))
	for i := range r.Buy {
		v[fBuyPrice+i] = int64(r.Buy[i].Price)
		v[fBuyQuantity+i] = int64(r.Buy[i].Quantity)
		v[fSellPrice+i] = int64(r.Sell[i].Price)
		v[fSellQuantity+i] = int64(r.Sell[i].Quantity)
	}
}
