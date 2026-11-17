// Package receiver owns the UDP socket: it reads datagrams and puts them on a
// queue for downstream processing.
//
// Deliberate deviation from ADR-001 (phase1_hld_design1.md), which decodes
// inline on the receive goroutine with no queue before the decoder. Here the
// receive goroutine only reads, stamps and enqueues; a worker pool
// (internal/unpack) does the rest.
//
// The read loop never blocks on the queue (NFR4): a full queue is fatal, and
// Run returns ErrQueueFull so the process stops loudly (req §10.2).
package receiver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
)

// MaxDatagram is the largest legal broadcast datagram (bible §3.6).
const MaxDatagram = 516

// readBufBytes is deliberately larger than MaxDatagram: a read silently
// truncates what does not fit, so an exact-size buffer would make an
// oversize datagram look legitimately full (bible §3.6).
const readBufBytes = 64 * 1024

// Datagram is one received datagram, copied out of the read buffer. It holds
// no pointers or slices, so it never aliases a buffer the read loop reuses.
// Only Data[:Len] is meaningful; bytes beyond Len are undefined.
//
// Seq stamps arrival order: 1, 2, 3, ... per queued datagram. Datagrams are
// processed by a pool of workers and can overtake each other, so downstream
// stages use Seq, not channel order, to tell older data from newer.
type Datagram struct {
	Seq  uint64
	Len  uint16
	Data [MaxDatagram]byte
}

// ErrQueueFull is returned by Run when the queue has no room for a datagram.
var ErrQueueFull = errors.New("receiver: queue full")

// Stats counts what Run has seen. Safe to read concurrently.
type Stats struct {
	Received atomic.Uint64 // datagrams queued
	Oversize atomic.Uint64 // datagrams over MaxDatagram, skipped
}

// Run reads datagrams from conn and puts each one on out, until ctx is
// cancelled (returns nil) or something fatal happens, such as a full queue.
// Run owns conn and closes it before returning.
func Run(ctx context.Context, conn *net.UDPConn, out chan<- Datagram, st *Stats) error {
	var (
		stop chan struct{}      // closed when Run returns, to end the closer goroutine
		buf  [readBufBytes]byte // socket read buffer, reused for every datagram
		d    Datagram           // the datagram being queued; sent by value, so reused
		n    int                // bytes in the current datagram
		seq  uint64             // arrival stamp of the last queued datagram
		err  error              // result of each socket read
	)

	// Allocations.
	stop = make(chan struct{})

	// Logic.
	defer close(stop)
	// Closing the socket is the only way to unblock a pending read (HLD §6.10).
	go func() {
		select {
		case <-ctx.Done():
		case <-stop:
		}
		conn.Close()
	}()

	for {
		n, err = conn.Read(buf[:])
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receiver: read: %w", err)
		}
		if n > MaxDatagram {
			st.Oversize.Add(1)
			slog.Warn("oversize datagram skipped", "bytes", n, "max", MaxDatagram)
			continue
		}
		seq++
		d.Seq = seq
		d.Len = uint16(n)
		copy(d.Data[:], buf[:n])
		select {
		case out <- d:
			st.Received.Add(1)
		default:
			return fmt.Errorf("%w (capacity %d)", ErrQueueFull, cap(out))
		}
	}
}
