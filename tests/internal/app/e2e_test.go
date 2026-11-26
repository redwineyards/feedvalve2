package app_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"feedvalve/internal/app"
	"feedvalve/tests/internal/fakedb"
)

// syncBuffer is a bytes.Buffer safe for the concurrent writes of slog.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// freeUDPPort returns a loopback UDP port with nothing bound to it.
func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := c.LocalAddr().(*net.UDPAddr).Port
	c.Close()
	return port
}

// datagram7208 is one datagram holding one uncompressed 7208 sub-packet
// (bible §3.2–§3.4, §4.8) with a single record for token.
func datagram7208(token, ltp, closing int32) []byte {
	m := make([]byte, 470)
	binary.BigEndian.PutUint16(m[10:], 7208)                // TransactionCode
	binary.BigEndian.PutUint16(m[38:], 470)                 // MessageLength
	binary.BigEndian.PutUint16(m[40:], 1)                   // NoOfRecords
	binary.BigEndian.PutUint32(m[42:], uint32(token))       // Token
	binary.BigEndian.PutUint32(m[42+12:], uint32(ltp))      // LastTradedPrice
	binary.BigEndian.PutUint32(m[42+198:], uint32(closing)) // ClosingPrice
	d := []byte{8, 0, 0, 1}                                 // netId 8, iNoPackets 1
	d = append(d, 0, 0)                                     // iCompLen 0: uncompressed
	d = append(d, 8, 0, 0, 0, 0, 0, 0, 0)                   // 8-byte prefix, byte 0 = netId
	return append(d, m...)
}

// datagram7305 is one datagram holding one uncompressed 7305 sub-packet
// (bible §4.3; 7305 is never compressed) for token, with base price base
// and minimum lot lot.
func datagram7305(token, base, lot int32) []byte {
	m := make([]byte, 294)
	binary.BigEndian.PutUint16(m[10:], 7305)          // TransactionCode
	binary.BigEndian.PutUint16(m[38:], 294)           // MessageLength
	binary.BigEndian.PutUint32(m[40:], uint32(token)) // Token
	binary.BigEndian.PutUint32(m[134:], uint32(lot))  // MinimumLotQuantity
	binary.BigEndian.PutUint32(m[226:], 1)            // Multiplier
	m[264] = 'N'                                      // DeleteFlag
	binary.BigEndian.PutUint32(m[290:], uint32(base)) // BasePrice
	d := []byte{8, 0, 0, 1, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0}
	return append(d, m...)
}

// counterLine returns the last log line for msg whose attributes include
// every one of attrs, or "" if there is none.
func counterLine(logs string, msg string, attrs ...string) string {
	var found string
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, `msg="`+msg+`"`) {
			continue
		}
		ok := true
		for _, a := range attrs {
			ok = ok && strings.Contains(line, " "+a)
		}
		if ok {
			found = line
		}
	}
	return found
}

// waitUntil polls cond every 10 ms until it holds or 5 s pass.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The whole 7208 path: a datagram on the socket becomes an UPDATE of
// ABC_TRADE_QUOTE, counters are logged while running, and the pool is capped
// at the writer count. A 7305 becomes UPDATEs of ABC_TRADE_QUOTE's master
// columns and of GHI_CNT_MSTR.
func TestRunWithDBWrites7208(t *testing.T) {
	logs := new(syncBuffer)
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	port := freeUDPPort(t)
	cfg := filepath.Join(t.TempDir(), "config.json")
	body := fmt.Sprintf(`{
		"gomaxprocs": 2,
		"db": {"host": "unused", "service": "svc", "user": "u", "writers": 2},
		"receiver": {"listenAddr": "127.0.0.1:%d"},
		"flush": {"windowMs": 20},
		"stats": {"intervalMs": 50}
	}`, port)
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	conn, f := fakedb.Open(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- app.RunWithDB(ctx, cfg, conn, new(slog.LevelVar)) }()

	sock, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	d := datagram7208(4242, 121476, 120000)
	// The socket is bound last during startup, so keep sending until one lands.
	waitUntil(t, "an UPDATE", func() bool {
		sock.Write(d)
		time.Sleep(40 * time.Millisecond)
		return len(f.Execs()) > 0
	})

	e := f.Execs()[0]
	if !strings.HasPrefix(e.SQL, "UPDATE ABC_TRADE_QUOTE SET ") {
		t.Errorf("SQL = %q, want an UPDATE of ABC_TRADE_QUOTE", e.SQL)
	}
	if len(e.Args) != 34 {
		t.Fatalf("%d binds, want 34", len(e.Args))
	}
	for _, c := range []struct {
		name string
		bind int
		want []int64
	}{
		{"last traded price", 0, []int64{121476}},
		{"% change from close", 6, []int64{123}},
		{"closing price", 7, []int64{120000}},
		{"token", 33, []int64{4242}},
	} {
		if !reflect.DeepEqual(e.Args[c.bind], c.want) {
			t.Errorf("%s (bind %d) = %v, want %v", c.name, c.bind+1, e.Args[c.bind], c.want)
		}
	}
	waitUntil(t, "a commit", func() bool { return f.Commits() > 0 })

	// 7305: the master part of token 777, written to ABC and GHI.
	sock.Write(datagram7305(777, 95000, 10))
	var abcMaster, ghi *fakedb.Exec
	waitUntil(t, "7305 UPDATEs", func() bool {
		execs := f.Execs()
		abcMaster, ghi = nil, nil
		for i := range execs {
			switch {
			case strings.HasPrefix(execs[i].SQL, "UPDATE ABC_TRADE_QUOTE SET ABC_HGH_PRC_RNG"):
				abcMaster = &execs[i]
			case strings.HasPrefix(execs[i].SQL, "UPDATE GHI_CNT_MSTR SET "):
				ghi = &execs[i]
			}
		}
		return abcMaster != nil && ghi != nil
	})
	if got, want := abcMaster.Args[2], []int64{95000}; !reflect.DeepEqual(got, want) {
		t.Errorf("ABC_BASE_PRC bind = %v, want %v", got, want)
	}
	if got, want := ghi.Args[0], []int64{10}; !reflect.DeepEqual(got, want) {
		t.Errorf("GHI_MIN_LOT_QTY bind = %v, want %v", got, want)
	}
	if got, want := ghi.Args[7], []int64{777}; !reflect.DeepEqual(got, want) {
		t.Errorf("GHI token bind = %v, want %v", got, want)
	}
	waitUntil(t, "7305 counted", func() bool {
		return counterLine(logs.String(), "splitter counters", "transcode=7305", "records=1") != ""
	})
	if n := conn.Stats().MaxOpenConnections; n != 2 {
		t.Errorf("MaxOpenConnections = %d, want the writer count 2", n)
	}
	if counterLine(logs.String(), "splitter counters", "transcode=7208") == "" {
		t.Error("no periodic 7208 splitter counters before shutdown")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunWithDB = %v, want nil after cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunWithDB did not return after cancel")
	}
}

// 7305 is non-rate-limited by default: with a one-minute window, a 7305
// still reaches the database at once instead of waiting for the tick.
func TestRunWithDB7305FlushesImmediately(t *testing.T) {
	port := freeUDPPort(t)
	cfg := filepath.Join(t.TempDir(), "config.json")
	body := fmt.Sprintf(`{
		"gomaxprocs": 2,
		"db": {"host": "unused", "service": "svc", "user": "u"},
		"receiver": {"listenAddr": "127.0.0.1:%d"},
		"flush": {"windowMs": 60000}
	}`, port)
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	conn, f := fakedb.Open(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- app.RunWithDB(ctx, cfg, conn, new(slog.LevelVar)) }()

	sock, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	// The socket is bound last during startup, so keep sending until one lands.
	waitUntil(t, "a GHI_CNT_MSTR UPDATE well before the 60 s tick", func() bool {
		sock.Write(datagram7305(777, 95000, 10))
		time.Sleep(40 * time.Millisecond)
		for _, e := range f.Execs() {
			if strings.HasPrefix(e.SQL, "UPDATE GHI_CNT_MSTR SET ") {
				return true
			}
		}
		return false
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunWithDB = %v, want nil after cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunWithDB did not return after cancel")
	}
}
