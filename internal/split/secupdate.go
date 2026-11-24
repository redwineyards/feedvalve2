package split

import (
	"log/slog"

	"feedvalve/internal/decode"
	"feedvalve/internal/mirror"
	"feedvalve/internal/router"
	"feedvalve/internal/unpack"
)

// maxNumber5 is the largest magnitude a NUMBER(5) column (ABC_MULTPLR) holds.
const maxNumber5 = 99_999

// SecurityUpdate returns the 7305 Splitter (security master change, bible
// §4.3). It decodes the message and sends its record as the master part of
// the token's row, written to ABC_TRADE_QUOTE and GHI_CNT_MSTR. A message of
// the wrong length is counted and dropped. A record with a value its column
// cannot hold is counted and dropped, so Oracle never rejects a row.
func SecurityUpdate(st *Stats) router.Splitter {
	return func(p *unpack.Packet, rt *router.Router) {
		var (
			rec decode.SecurityUpdate // the message's one record, on the stack
			c   mirror.Contribution   // the contribution being sent
			err error                 // decode result
		)

		err = decode.DecodeSecurityUpdate(p.Data[:p.Len], &rec)
		if err != nil {
			st.BadLength.Add(1)
			slog.Warn("7305 dropped", "seq", p.Seq, "index", p.Index, "len", p.Len, "want", decode.SecurityUpdateLen, "err", err)
			return
		}
		// Every other mapped field is an int32 going into NUMBER(10), so it fits.
		if rec.Multiplier > maxNumber5 || rec.Multiplier < -maxNumber5 {
			st.OutOfRange.Add(1)
			slog.Warn("7305 record dropped: value too large for its column",
				"seq", p.Seq, "index", p.Index, "token", rec.Token, "field", "Multiplier", "value", rec.Multiplier)
			return
		}
		st.Records.Add(1)
		c = mirror.Contribution{Seq: p.Seq, Index: p.Index, Code: p.Code, Token: rec.Token,
			Part: mirror.PartMaster, Master: masterOf(&rec)}
		rt.Send(&c)
	}
}

// masterOf returns the ABC_TRADE_QUOTE and GHI_CNT_MSTR values of 7305
// record r, as sent.
func masterOf(r *decode.SecurityUpdate) mirror.Master {
	return mirror.Master{
		HighPriceRange:     int64(r.HighPriceRange),
		LowPriceRange:      int64(r.LowPriceRange),
		BasePrice:          int64(r.BasePrice),
		Multiplier:         int64(r.Multiplier),
		MinimumLotQuantity: int64(r.MinimumLotQuantity),
		BoardLotQuantity:   int64(r.BoardLotQuantity),
		TickSize:           int64(r.TickSize),
		FreezeQuantity:     int64(r.FreezeQuantity),
		DeleteFlag:         int64(r.DeleteFlag),
		Remark:             r.Remark,
	}
}
