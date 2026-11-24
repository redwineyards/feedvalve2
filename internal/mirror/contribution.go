// Package mirror holds each shard's coalesced copy of its rows, one entry per
// token. An entry has two parts, each from its own transcode: the Quote
// (7208, ABC_TRADE_QUOTE's market columns) and the Master (7305,
// ABC_TRADE_QUOTE's range columns and GHI_CNT_MSTR).
//
// Its input is the Contribution: one record's part for one token. The mirror
// keeps the latest of each part per token; a contribution older than the last
// one applied to the same part of its token is dropped as stale.
package mirror

// Quote is the ABC_TRADE_QUOTE columns a 7208 record sets, as the exchange
// sends them: prices in paise, times in seconds since 1980 (0 = no trade yet),
// the net change indicator as its byte code, and the buy/sell totals rounded.
// The one computed value is the percentage change from the closing price.
type Quote struct {
	LastTradedPrice    int64    // ABC_LST_TRD_PRC
	LastTradeQuantity  int64    // ABC_LST_TRD_QTY
	LastTradeTime      int64    // ABC_LST_TRD_TM
	AverageTradePrice  int64    // ABC_AVRG_TRD_PRC
	VolumeTradedToday  int64    // ABC_TOT_QTY_TRDD
	NetChangeIndicator int64    // ABC_INDCTR
	PctChangePrevClose int64    // ABC_CHNG_PRVS_CLS: % change from ClosingPrice, in hundredths of a percent; 0 if no close
	ClosingPrice       int64    // ABC_CLS_PRC
	OpenPrice          int64    // ABC_OPN_PRC
	HighPrice          int64    // ABC_HGH_PRC
	LowPrice           int64    // ABC_LOW_PRC
	TotalBuyQuantity   int64    // ABC_TOT_BID_QTY
	TotalSellQuantity  int64    // ABC_TOT_OFFR_QTY
	BuyPrice           [5]int64 // ABC_BSTn_BID_PRC, best first
	BuyQuantity        [5]int64 // ABC_BSTn_BID_QTY
	SellPrice          [5]int64 // ABC_BSTn_OFFR_PRC
	SellQuantity       [5]int64 // ABC_BSTn_OFFR_QTY
}

// Master is the columns a 7305 record sets, as the exchange sends them:
// prices in paise, the delete flag as its byte code, the remark as raw
// blank-padded bytes.
type Master struct {
	HighPriceRange     int64    // ABC_HGH_PRC_RNG
	LowPriceRange      int64    // ABC_LOW_PRC_RNG
	BasePrice          int64    // ABC_BASE_PRC and GHI_BASE_PRICE
	Multiplier         int64    // ABC_MULTPLR, NUMBER(5)
	MinimumLotQuantity int64    // GHI_MIN_LOT_QTY
	BoardLotQuantity   int64    // GHI_BOARD_LOT_QTY
	TickSize           int64    // GHI_TCK_SZ
	FreezeQuantity     int64    // GHI_FREEZE_QTY
	DeleteFlag         int64    // GHI_RECORD_DELETE_FLG: 'N' active, 'Y' deleted
	Remark             [25]byte // GHI_REMARKS, CHAR(25)
}

// Part says which part of a token's entry a contribution sets.
type Part uint8

const (
	PartQuote  Part = iota // Quote, from 7208
	PartMaster             // Master, from 7305
)

// Contribution is one update to one token's row, passed by value on a shard
// queue. It holds only scalars and fixed-size arrays (ADR-006), so it never
// aliases a buffer a worker reuses.
type Contribution struct {
	Seq    uint64 // arrival stamp of the datagram it came from
	Index  uint16 // sub-packet position within that datagram
	Record uint16 // record position within the message
	Code   uint16 // transcode
	Token  int32  // the row: ABC_TOKEN_NO, GHI_TOKEN_NO
	Part   Part   // which of Quote and Master carries the values
	Quote  Quote  // the values, when Part is PartQuote
	Master Master // the values, when Part is PartMaster
}

// Stamp packs the contribution's arrival position into one number that
// orders contributions by arrival: datagram, then sub-packet, then record.
// Index and Record each fit in 8 bits: a 516-byte datagram holds at most 10
// sub-packets, and record counts are clamped far below 256.
func (c *Contribution) Stamp() uint64 {
	return c.Seq<<16 | uint64(c.Index)<<8 | uint64(c.Record)
}
