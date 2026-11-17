package router_test

import (
	"encoding/binary"
	"sync"
	"testing"

	"feedvalve/internal/mirror"
	"feedvalve/internal/router"
	"feedvalve/internal/stats"
	"feedvalve/internal/unpack"
)

// newRouter builds a router over n buffered shard queues.
func newRouter(n int, splitters map[uint16]router.Splitter) (*router.Router, []chan mirror.Contribution, *stats.TokenCounts, *router.Stats) {
	qs := make([]chan mirror.Contribution, n)
	send := make([]chan<- mirror.Contribution, n)
	for i := range qs {
		qs[i] = make(chan mirror.Contribution, 4096)
		send[i] = qs[i]
	}
	tokens := stats.NewTokenCounts(16)
	st := new(router.Stats)
	return router.New(splitters, send, tokens, st), qs, tokens, st
}

// drain returns everything queued on q so far, without blocking.
func drain(q chan mirror.Contribution) []mirror.Contribution {
	var got []mirror.Contribution
	for {
		select {
		case u := <-q:
			got = append(got, u)
		default:
			return got
		}
	}
}

// shardOf sends u through r and reports which queue it landed on.
func shardOf(t *testing.T, r *router.Router, qs []chan mirror.Contribution, u mirror.Contribution) int {
	t.Helper()
	r.Send(&u)
	for i, q := range qs {
		if got := drain(q); len(got) == 1 {
			return i
		}
	}
	t.Fatalf("unit for token %d reached no shard", u.Token)
	return -1
}

func TestSendDeliversContributionIntact(t *testing.T) {
	r, qs, _, st := newRouter(4, nil)
	u := mirror.Contribution{Seq: 9, Index: 2, Record: 1, Code: 7208, Dest: 2, Token: 4242, RowKey: -5}
	u.Set(0, 11)
	u.Set(mirror.MaxCols-1, -22)

	r.Send(&u)

	var got []mirror.Contribution
	for _, q := range qs {
		got = append(got, drain(q)...)
	}
	if len(got) != 1 || got[0] != u {
		t.Fatalf("got %+v, want exactly the unit sent", got)
	}
	if n := st.Units.Load(); n != 1 {
		t.Errorf("Units = %d, want 1", n)
	}
}

func TestSendShardSelection(t *testing.T) {
	tests := []struct {
		name   string
		shards int
		units  []mirror.Contribution
		check  func(t *testing.T, got []int)
	}{
		{
			name:   "same token, any transcode, same shard",
			shards: 8,
			units: []mirror.Contribution{
				{Token: 31337, Code: 7208}, {Token: 31337, Code: 7201},
				{Token: 31337, Code: 7305}, {Token: 31337, Code: 7202},
			},
			check: func(t *testing.T, got []int) {
				for _, s := range got[1:] {
					if s != got[0] {
						t.Errorf("token 31337 reached shards %v, want one shard", got)
						return
					}
				}
			},
		},
		{
			name:   "general units go to shard 0",
			shards: 8,
			units:  []mirror.Contribution{{General: true, Code: 6541}, {General: true, Code: 7306, Token: 999}},
			check: func(t *testing.T, got []int) {
				for _, s := range got {
					if s != 0 {
						t.Errorf("general units reached shards %v, want all 0", got)
						return
					}
				}
			},
		},
		{
			name:   "one shard takes everything",
			shards: 1,
			units:  []mirror.Contribution{{Token: 1}, {Token: -7}, {Token: 1 << 30}, {General: true}},
			check: func(t *testing.T, got []int) {
				for _, s := range got {
					if s != 0 {
						t.Errorf("got shards %v, want all 0", got)
						return
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, qs, _, _ := newRouter(tt.shards, nil)
			var got []int
			for _, u := range tt.units {
				got = append(got, shardOf(t, r, qs, u))
			}
			tt.check(t, got)
		})
	}
}

// Exchange tokens come in contiguous blocks; the mix must still spread them.
func TestSendSpreadsSequentialTokens(t *testing.T) {
	const shards, tokens = 4, 1000
	r, qs, _, _ := newRouter(shards, nil)
	for tok := int32(40000); tok < 40000+tokens; tok++ {
		r.Send(&mirror.Contribution{Token: tok, Code: 7208})
	}
	for i, q := range qs {
		if n := len(drain(q)); n < tokens*15/100 {
			t.Errorf("shard %d got %d of %d units, want at least 15%%", i, n, tokens)
		}
	}
}

func TestSendCountsTokens(t *testing.T) {
	r, _, tokens, st := newRouter(4, nil)
	for _, u := range []mirror.Contribution{
		{Token: 101, Code: 7208}, {Token: 101, Code: 7208}, {Token: 101, Code: 7201},
		{Token: 102, Code: 7208}, {General: true, Code: 6541},
	} {
		r.Send(&u)
	}
	for _, c := range []struct {
		token int32
		code  uint16
		want  uint64
	}{{101, 7208, 2}, {101, 7201, 1}, {102, 7208, 1}, {0, 6541, 0}} {
		if got := tokens.Get(c.token, c.code); got != c.want {
			t.Errorf("tokens.Get(%d, %d) = %d, want %d", c.token, c.code, got, c.want)
		}
	}
	if got := tokens.Len(); got != 3 {
		t.Errorf("tokens.Len() = %d, want 3 (general units are not counted)", got)
	}
	if got := st.Units.Load(); got != 5 {
		t.Errorf("Units = %d, want 5", got)
	}
}

// packet builds a BCAST_HEADER packet with transcode code and ErrorCode errCode.
func packet(code, errCode uint16) *unpack.Packet {
	p := &unpack.Packet{Seq: 3, Index: 1, Code: code, Len: 40}
	binary.BigEndian.PutUint16(p.Data[10:], code)
	binary.BigEndian.PutUint16(p.Data[12:], errCode)
	return p
}

func TestRoute(t *testing.T) {
	const code = 7208
	tokens := []int32{11, 22, 33}

	// splitOut emits one unit per token, as a multi-record splitter would.
	var calls int
	splitOut := func(p *unpack.Packet, r *router.Router) {
		calls++
		var u mirror.Contribution
		for i, tok := range tokens {
			u = mirror.Contribution{Seq: p.Seq, Index: p.Index, Record: uint16(i), Code: p.Code, Token: tok}
			r.Send(&u)
		}
	}

	tests := []struct {
		name      string
		p         *unpack.Packet
		wantUnits int
		wantCalls int
		wantStats [3]uint64 // Units, NoSplitter, ErrorResponse
	}{
		{"splitter output reaches the shards", packet(code, 0), 3, 1, [3]uint64{3, 0, 0}},
		{"no splitter for the code", packet(7305, 0), 0, 0, [3]uint64{0, 1, 0}},
		{"error response is dropped before splitting", packet(code, 16388), 0, 0, [3]uint64{0, 0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls = 0
			r, qs, _, st := newRouter(4, map[uint16]router.Splitter{code: splitOut})

			r.Route(tt.p)

			var got []mirror.Contribution
			for _, q := range qs {
				shard := drain(q)
				for i := 1; i < len(shard); i++ {
					if shard[i-1].Record > shard[i].Record {
						t.Errorf("units out of emit order within a shard: %+v", shard)
					}
				}
				got = append(got, shard...)
			}
			if len(got) != tt.wantUnits {
				t.Errorf("got %d units, want %d", len(got), tt.wantUnits)
			}
			for _, u := range got {
				if u.Seq != 3 || u.Index != 1 || u.Code != code {
					t.Errorf("unit %+v lost its packet identity", u)
				}
			}
			if calls != tt.wantCalls {
				t.Errorf("splitter called %d times, want %d", calls, tt.wantCalls)
			}
			if s := [3]uint64{st.Units.Load(), st.NoSplitter.Load(), st.ErrorResponse.Load()}; s != tt.wantStats {
				t.Errorf("(Units, NoSplitter, ErrorResponse) = %v, want %v", s, tt.wantStats)
			}
		})
	}
}

func TestSendConcurrent(t *testing.T) {
	const goroutines, perG = 4, 500
	r, qs, tokens, st := newRouter(4, nil)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perG {
				r.Send(&mirror.Contribution{Token: int32(g*perG + i), Code: 7208})
			}
		}()
	}
	wg.Wait()

	total := 0
	for _, q := range qs {
		total += len(drain(q))
	}
	if total != goroutines*perG || st.Units.Load() != goroutines*perG {
		t.Errorf("delivered %d, Units %d, want %d", total, st.Units.Load(), goroutines*perG)
	}
	if tokens.Len() != goroutines*perG {
		t.Errorf("tokens.Len() = %d, want %d", tokens.Len(), goroutines*perG)
	}
}

// The receive path must not allocate (ADR-006), and a Unit is ~520 bytes:
// Route through a splitter must not move it to the heap.
func TestRouteAllocatesNothing(t *testing.T) {
	split := func(p *unpack.Packet, r *router.Router) {
		var u mirror.Contribution
		u = mirror.Contribution{Seq: p.Seq, Code: p.Code, Token: 77}
		r.Send(&u)
	}
	r, qs, _, _ := newRouter(4, map[uint16]router.Splitter{7208: split})
	p := packet(7208, 0)
	allocs := testing.AllocsPerRun(100, func() {
		r.Route(p)
		for _, q := range qs {
			for len(q) > 0 {
				<-q
			}
		}
	})
	if allocs != 0 {
		t.Errorf("Route allocates %.0f times per packet, want 0", allocs)
	}
}
