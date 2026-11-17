package split_test

import (
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"feedvalve/internal/decode"
	"feedvalve/internal/mirror"
	"feedvalve/internal/router"
	"feedvalve/internal/schema"
	"feedvalve/internal/split"
	"feedvalve/internal/stats"
	"feedvalve/internal/unpack"
)

// loadSchema writes body to a file and loads it.
func loadSchema(t testing.TB, body string) *schema.Schema {
	t.Helper()
	p := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := schema.Load(p)
	if err != nil {
		t.Fatalf("schema.Load: %v", err)
	}
	return s
}

// oneQueue returns a router over a single shard queue.
func oneQueue() (*router.Router, chan mirror.Contribution) {
	q := make(chan mirror.Contribution, 8)
	return router.New(nil, []chan<- mirror.Contribution{q}, stats.NewTokenCounts(4), new(router.Stats)), q
}

// drainQ returns everything on q.
func drainQ(q chan mirror.Contribution) []mirror.Contribution {
	var got []mirror.Contribution
	for len(q) > 0 {
		got = append(got, <-q)
	}
	return got
}

// record is a 7208 record with distinct values in every mapped field.
func record() decode.OnlyMBP {
	r := decode.OnlyMBP{
		Token: 4242, LastTradedPrice: 123450, LastTradeQuantity: 7, LastTradeTime: 1451610123,
		AverageTradePrice: 123300, VolumeTradedToday: 99000, NetChangeIndicator: '+',
		NetPriceChangeFromClosingPrice: 120000, ClosingPrice: 120001, OpenPrice: 121000,
		HighPrice: 124000, LowPrice: 119000, TotalBuyQuantity: 1500.6, TotalSellQuantity: 800.4,
	}
	for i := range r.Buy {
		r.Buy[i] = decode.MBPLevel{Price: int32(123400 - i*10), Quantity: int32(10 + i)}
		r.Sell[i] = decode.MBPLevel{Price: int32(123500 + i*10), Quantity: int32(20 + i)}
	}
	return r
}

const quoteMap = `{"destinations": [{"table": "ABC", "key": "TOKEN", "columns": [
  {"column": "LTP", "field": "lastTradedPrice", "type": "number"},
  {"column": "LTT", "field": "lastTradeTime", "type": "date"},
  {"column": "IND", "field": "netChangeIndicator", "type": "char"},
  {"column": "UPD", "type": "sysdate"},
  {"column": "TBQ", "field": "totalBuyQuantity", "type": "number"},
  {"column": "B1P", "field": "buyPrice1", "type": "number"},
  {"column": "S5Q", "field": "sellQuantity5", "type": "number"}]}]}`

func TestOnlyMBPMapperFillsMappedSlots(t *testing.T) {
	mapRec := split.NewOnlyMBPMapper(loadSchema(t, quoteMap))
	rt, q := oneQueue()
	mapRec(&unpack.Packet{Seq: 42, Index: 3, Code: 7208}, 1, record(), rt)

	got := drainQ(q)
	if len(got) != 1 {
		t.Fatalf("sent %d contributions, want 1", len(got))
	}
	c := got[0]
	if c.Seq != 42 || c.Index != 3 || c.Record != 1 || c.Code != 7208 || c.Dest != 0 || c.Token != 4242 || c.RowKey != 4242 || c.General {
		t.Errorf("identity = %+v, want seq 42, index 3, record 1, code 7208, dest 0, token and row 4242", c)
	}
	want := map[int]int64{0: 123450, 1: 1451610123, 2: '+', 4: 1501, 5: 123400, 6: 24}
	var wantMask uint64
	for slot, v := range want {
		wantMask |= 1 << slot
		if c.Cols[slot] != v {
			t.Errorf("slot %d = %d, want %d", slot, c.Cols[slot], v)
		}
	}
	if c.Mask != wantMask {
		t.Errorf("Mask = %#b, want %#b (the sysdate slot is never set)", c.Mask, wantMask)
	}
}

func TestOnlyMBPMapperSkipsZeroTradeTime(t *testing.T) {
	mapRec := split.NewOnlyMBPMapper(loadSchema(t, quoteMap))
	rt, q := oneQueue()
	r := record()
	r.LastTradeTime = 0 // no trade yet today
	mapRec(&unpack.Packet{Code: 7208}, 0, r, rt)
	got := drainQ(q)
	if len(got) != 1 || got[0].Mask&(1<<1) != 0 || got[0].Mask&1 == 0 {
		t.Errorf("got %+v, want a contribution without the trade-time slot", got)
	}
}

func TestOnlyMBPMapperPerDestination(t *testing.T) {
	two := `{"destinations": [
	  {"table": "A", "key": "K", "columns": [{"column": "LTP", "field": "lastTradedPrice", "type": "number"}]},
	  {"table": "B", "key": "K", "columns": [{"column": "LTP", "field": "lastTradedPrice", "type": "number"},
	                                          {"column": "OPN", "field": "openPrice", "type": "number"}]}]}`
	mapRec := split.NewOnlyMBPMapper(loadSchema(t, two))
	rt, q := oneQueue()
	mapRec(&unpack.Packet{Code: 7208}, 0, record(), rt)

	got := drainQ(q)
	if len(got) != 2 || got[0].Dest != 0 || got[1].Dest != 1 {
		t.Fatalf("got %+v, want one contribution for each destination, in order", got)
	}
	if got[0].Mask != 1 || got[0].Cols[0] != 123450 {
		t.Errorf("dest 0 = mask %#b col0 %d, want LTP only", got[0].Mask, got[0].Cols[0])
	}
	if got[1].Mask != 3 || got[1].Cols[0] != 123450 || got[1].Cols[1] != 121000 {
		t.Errorf("dest 1 = mask %#b cols %v, want LTP and open price", got[1].Mask, got[1].Cols[:2])
	}
}

func TestOnlyMBPMapperIgnoresOtherFields(t *testing.T) {
	other := `{"destinations": [{"table": "S", "key": "K", "columns": [{"column": "ST", "field": "marketStatus", "type": "number"}]}]}`
	mapRec := split.NewOnlyMBPMapper(loadSchema(t, other))
	rt, q := oneQueue()
	mapRec(&unpack.Packet{Code: 7208}, 0, record(), rt)
	if got := drainQ(q); len(got) != 0 {
		t.Errorf("sent %+v for a schema with no 7208 field, want nothing", got)
	}
}

// allFields maps every 7208 field to a column of one table.
func allFields() string {
	var cols []string
	for i, f := range split.OnlyMBPFields {
		cols = append(cols, fmt.Sprintf(`{"column": "C%d", "field": %q, "type": "number"}`, i, f))
	}
	return `{"destinations": [{"table": "T", "key": "K", "columns": [` + strings.Join(cols, ",") + `]}]}`
}

func TestOnlyMBPFieldsAllProduced(t *testing.T) {
	seen := map[string]bool{}
	for _, f := range split.OnlyMBPFields {
		if seen[f] {
			t.Errorf("OnlyMBPFields lists %q twice", f)
		}
		seen[f] = true
	}
	mapRec := split.NewOnlyMBPMapper(loadSchema(t, allFields()))
	rt, q := oneQueue()
	mapRec(&unpack.Packet{Code: 7208}, 0, record(), rt)
	got := drainQ(q)
	if len(got) != 1 || bits.OnesCount64(got[0].Mask) != len(split.OnlyMBPFields) {
		t.Fatalf("got %+v, want every one of the %d fields set", got, len(split.OnlyMBPFields))
	}
	// Slot order follows OnlyMBPFields: spot-check the book's two ends.
	idx := func(name string) int {
		for i, f := range split.OnlyMBPFields {
			if f == name {
				return i
			}
		}
		t.Fatalf("OnlyMBPFields has no %q", name)
		return -1
	}
	for name, want := range map[string]int64{"buyPrice1": 123400, "buyQuantity5": 14, "sellPrice5": 123540, "totalSellQuantity": 800} {
		if v := got[0].Cols[idx(name)]; v != want {
			t.Errorf("%s = %d, want %d", name, v, want)
		}
	}
}
