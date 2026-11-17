// Package split holds the per-transcode Splitters: each decodes one message
// type and hands its records, one by one, to a mapper that turns them into
// normalised table rows for the shards.
//
// The mappers are not written yet, because the destination tables are not
// defined. Each Splitter takes its mapper as an argument, so it can be
// supplied without touching the decode.
package split

import (
	"log/slog"
	"sync/atomic"

	"feedvalve/internal/decode"
	"feedvalve/internal/router"
	"feedvalve/internal/unpack"
)

// Stats counts what the splitters have seen. Safe to read concurrently.
type Stats struct {
	Records   atomic.Uint64 // records handed to a mapper
	Clamped   atomic.Uint64 // messages whose record count had to be clamped
	BadLength atomic.Uint64 // messages not their declared length; dropped
}

// OnlyMBPMapper turns one decoded 7208 record into normalised table rows and
// sends each as a mirror.Contribution with rt.Send. rec is the record's position in the message.
// It must not keep p.
type OnlyMBPMapper func(p *unpack.Packet, rec uint16, r decode.OnlyMBP, rt *router.Router)

// OnlyMBP returns the 7208 Splitter: it decodes the message and calls mapRec
// once per record, in order. A message of the wrong length is counted and
// dropped (HLD §6.4); a clamped record count is counted and logged.
func OnlyMBP(mapRec OnlyMBPMapper, st *Stats) router.Splitter {
	return func(p *unpack.Packet, rt *router.Router) {
		var (
			recs    [2]decode.OnlyMBP // the message's records, on the stack
			n       int               // records in the message
			clamped bool              // whether NoOfRecords was out of range
			err     error             // decode result
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
			st.Records.Add(1)
			mapRec(p, uint16(i), recs[i], rt)
		}
	}
}
