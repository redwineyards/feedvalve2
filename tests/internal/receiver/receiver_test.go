package receiver_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"feedvalve/internal/receiver"
)

// start binds a loopback socket, runs receiver.Run on it in a goroutine and
// returns a connected sender plus a channel carrying Run's result.
func start(t *testing.T, ctx context.Context, out chan receiver.Datagram, st *receiver.Stats) (net.Conn, <-chan error) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- receiver.Run(ctx, conn, out, st) }()

	sender, err := net.Dial("udp", conn.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sender.Close() })
	return sender, done
}

func send(t *testing.T, c net.Conn, b []byte) {
	t.Helper()
	if _, err := c.Write(b); err != nil {
		t.Fatal(err)
	}
}

func recv(t *testing.T, out <-chan receiver.Datagram) receiver.Datagram {
	t.Helper()
	select {
	case d := <-out:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a datagram")
		return receiver.Datagram{}
	}
}

func TestRunDeliversDatagramsInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan receiver.Datagram, 10)
	var st receiver.Stats
	sender, _ := start(t, ctx, out, &st)

	payloads := [][]byte{[]byte("first"), bytes.Repeat([]byte{0xAB}, receiver.MaxDatagram), []byte("third")}
	for _, p := range payloads {
		send(t, sender, p)
	}
	for i, want := range payloads {
		d := recv(t, out)
		if got := d.Data[:d.Len]; !bytes.Equal(got, want) {
			t.Errorf("datagram %d: got %d bytes %q..., want %d bytes", i, len(got), got[:min(len(got), 8)], len(want))
		}
		if d.Seq != uint64(i+1) {
			t.Errorf("datagram %d: Seq = %d, want %d", i, d.Seq, i+1)
		}
	}
	if got := st.Received.Load(); got != 3 {
		t.Errorf("Received = %d, want 3", got)
	}
}

func TestRunSkipsOversizeDatagram(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan receiver.Datagram, 10)
	var st receiver.Stats
	sender, _ := start(t, ctx, out, &st)

	send(t, sender, make([]byte, receiver.MaxDatagram+84))
	send(t, sender, []byte("after"))

	d := recv(t, out)
	if string(d.Data[:d.Len]) != "after" {
		t.Errorf("got %q, want the datagram after the oversize one", d.Data[:d.Len])
	}
	if d.Seq != 1 {
		t.Errorf("Seq = %d, want 1 (oversize datagrams take no stamp)", d.Seq)
	}
	if got := st.Oversize.Load(); got != 1 {
		t.Errorf("Oversize = %d, want 1", got)
	}
	if got := st.Received.Load(); got != 1 {
		t.Errorf("Received = %d, want 1 (oversize is not queued)", got)
	}
}

func TestRunStopsLoudlyWhenQueueFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan receiver.Datagram, 1) // nobody drains it
	var st receiver.Stats
	sender, done := start(t, ctx, out, &st)

	send(t, sender, []byte("fills the queue"))
	send(t, sender, []byte("overflows"))

	select {
	case err := <-done:
		if !errors.Is(err, receiver.ErrQueueFull) {
			t.Fatalf("Run returned %v, want ErrQueueFull", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop on a full queue")
	}
}

func TestRunReturnsNilOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan receiver.Datagram, 1)
	var st receiver.Stats
	_, done := start(t, ctx, out, &st)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of cancel")
	}
}
