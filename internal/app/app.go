// Package app wires FeedValve together and runs it. Startup order follows
// HLD §11.4: config → GOMAXPROCS → Oracle ping → downstream stages → bind
// the socket last, so no datagram arrives before there is somewhere to put it.
//
// This is the only package, besides main, that reads configuration. It
// passes plain values to everything else.
package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/db"
	"feedvalve/internal/mirror"
	"feedvalve/internal/receiver"
	"feedvalve/internal/router"
	"feedvalve/internal/shard"
	"feedvalve/internal/split"
	"feedvalve/internal/unpack"
	"feedvalve/internal/writer"
)

// PasswordEnv names the environment variable holding the Oracle password,
// the only place it may come from (req §7).
const PasswordEnv = "FEEDVALVE_DB_PASSWORD"

// Run starts FeedValve from the config file at cfgPath and keeps it running
// until ctx is cancelled (returns nil) or something fatal happens. Errors are
// returned rather than exiting, so cleanup always runs (HLD §6.10). lvl is
// the log handler's level; Run sets it from the config's log.level.
func Run(ctx context.Context, cfgPath string, lvl *slog.LevelVar) error {
	var (
		cfg       *config.Config // the loaded, validated config file
		password  string         // Oracle password, from the environment only
		target    db.Target      // which Oracle instance to connect to
		dbTimeout time.Duration  // how long the startup ping may take
		conn      *sql.DB        // Oracle connection pool
		err       error          // result of each startup step
	)

	// Config reads.
	cfg, err = config.Load(cfgPath)
	if err != nil {
		return err
	}
	lvl.Set(cfg.Log.Level)
	password = os.Getenv(PasswordEnv)
	target = db.Target{Host: cfg.DB.Host, Port: cfg.DB.Port, Service: cfg.DB.Service, User: cfg.DB.User}
	dbTimeout = time.Duration(cfg.DB.ConnectTimeoutMs) * time.Millisecond

	// Logic.
	runtime.GOMAXPROCS(cfg.GOMAXPROCS)
	if password == "" {
		return errors.New(PasswordEnv + " is not set")
	}
	conn, err = db.Open(ctx, target, password, dbTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	slog.Info("oracle connected", "db", db.Describe(target))
	return run(ctx, cfg, conn)
}

// RunWithDB is Run for a caller that brings its own database connection, such
// as a test: it loads the config at cfgPath but neither reads the password nor
// opens Oracle. conn stays the caller's to close.
func RunWithDB(ctx context.Context, cfgPath string, conn *sql.DB, lvl *slog.LevelVar) error {
	var (
		cfg *config.Config // the loaded, validated config file
		err error          // result of loading it
	)

	// Config reads.
	cfg, err = config.Load(cfgPath)
	if err != nil {
		return err
	}
	lvl.Set(cfg.Log.Level)

	// Logic.
	runtime.GOMAXPROCS(cfg.GOMAXPROCS)
	return run(ctx, cfg, conn)
}

// run builds the pipeline over conn from cfg, binds the socket last, and runs
// until ctx is cancelled or something fatal happens; on the way out it stops
// every stage, upstream first, and logs the final counters.
func run(ctx context.Context, cfg *config.Config, conn *sql.DB) error {
	var (
		commitTO   time.Duration                // bound on one batch's transaction
		workers    int                          // unpack pool size
		shardCount int                          // number of shards
		shardCap   int                          // capacity of each shard queue
		laddr      *net.UDPAddr                 // address to receive the feed on
		sock       *net.UDPConn                 // the feed's UDP socket
		queue      chan receiver.Datagram       // datagrams waiting for the unpack pool
		ctr        counters                     // every stage's counters, logged while running and at exit
		writers    int                          // writer pool size
		window     time.Duration                // W, the global flush tick
		maxBatch   int                          // rows per batch
		maxInFl    int                          // batches out at once per shard
		drain      time.Duration                // bound on the shutdown flush
		batchQ     chan *mirror.Batch           // batches waiting for a writer, from every shard
		done       []chan *mirror.Batch         // per shard: batches writers have finished
		ticks      []chan struct{}              // per shard: the global tick
		flushReqs  []chan struct{}              // per shard: pending non-rate-limited flush request
		nonRL      map[uint16]bool              // non-rate-limited transcodes
		stopTick   chan struct{}                // closed to stop the ticker
		writersOut chan struct{}                // receives once per writer when it has stopped
		splitters  map[uint16]router.Splitter   // preprocessing per transcode
		shardQs    []chan mirror.Contribution   // one queue per shard
		shardSend  []chan<- mirror.Contribution // the same queues, send side, for the router
		mirrors    []*mirror.Mirror             // one per shard, owned by its goroutine
		rtr        *router.Router               // sends each message's units to their shards
		poolDone   chan struct{}                // closed when the unpack pool has drained and stopped
		shardsDone chan struct{}                // receives once per shard when it has drained its queue
		statsEvery time.Duration                // how often the counters are logged while running
		stopStats  chan struct{}                // closed to stop the periodic counters
		statsDone  chan struct{}                // closed once the periodic counters have stopped
		err        error                        // result of each startup step
	)

	// Config reads.
	workers = cfg.Unpack.Workers
	shardCount = cfg.Shards.Count
	shardCap = cfg.Shards.QueueCapacity
	writers = cfg.DB.Writers
	window = time.Duration(cfg.Flush.WindowMs) * time.Millisecond
	maxBatch = cfg.Flush.MaxBatchSize
	maxInFl = cfg.Flush.MaxInFlightPerShard
	drain = time.Duration(cfg.Flush.DrainTimeoutMs) * time.Millisecond
	commitTO = time.Duration(cfg.DB.CommitTimeoutMs) * time.Millisecond
	statsEvery = time.Duration(cfg.Stats.IntervalMs) * time.Millisecond

	// Allocations.
	queue = make(chan receiver.Datagram, cfg.Receiver.QueueCapacity)
	ctr.unpack = new(unpack.Stats)
	shardQs = make([]chan mirror.Contribution, shardCount)
	shardSend = make([]chan<- mirror.Contribution, shardCount)
	for i := range shardQs {
		shardQs[i] = make(chan mirror.Contribution, shardCap)
		shardSend[i] = shardQs[i]
	}
	mirrors = make([]*mirror.Mirror, shardCount)
	for i := range mirrors {
		mirrors[i] = mirror.New(&ctr.mirror)
	}
	// batchQ holds every batch that exists, so a shard's send never fails
	// (HLD §6.6 Q2); each done channel holds all of its shard's batches, so a
	// writer never blocks returning one.
	batchQ = make(chan *mirror.Batch, shardCount*maxInFl)
	done = make([]chan *mirror.Batch, shardCount)
	ticks = make([]chan struct{}, shardCount)
	flushReqs = make([]chan struct{}, shardCount)
	for i := range done {
		done[i] = make(chan *mirror.Batch, maxInFl)
		ticks[i] = make(chan struct{}, 1)
		flushReqs[i] = make(chan struct{}, 1)
	}
	nonRL = make(map[uint16]bool, len(cfg.Flush.NonRateLimited))
	for _, code := range cfg.Flush.NonRateLimited {
		nonRL[uint16(code)] = true
	}
	stopTick = make(chan struct{})
	writersOut = make(chan struct{}, writers)
	poolDone = make(chan struct{})
	shardsDone = make(chan struct{}, shardCount)
	stopStats = make(chan struct{})
	statsDone = make(chan struct{})
	splitters = map[uint16]router.Splitter{
		7208: split.OnlyMBP(&ctr.split7208),
		7305: split.SecurityUpdate(&ctr.split7305),
	}

	// Logic.
	// Each writer holds one session (ADR-005); the pool never opens more.
	conn.SetMaxOpenConns(writers)
	conn.SetMaxIdleConns(writers)

	// Transcodes without a Splitter are counted as NoSplitter and dropped.
	rtr = router.New(splitters, shardSend, &ctr.router)

	// Writers first, then shards, then the tick (HLD §11.4). Every writer
	// takes from the one batchQ, so they commit in parallel.
	for range writers {
		go func() {
			writer.Run(batchQ, done, db.NewCommit(conn, commitTO, &ctr.db), &ctr.writer)
			writersOut <- struct{}{}
		}()
	}
	// One goroutine per shard coalesces its queue into its own mirror and
	// flushes it on the tick, or at once on a flush request from any shard
	// that merged a non-rate-limited transcode.
	for i := range shardQs {
		go func() {
			shard.Run(shard.Params{
				ID: i, In: shardQs[i], Tick: ticks[i], Mirror: mirrors[i],
				BatchQ: batchQ, Done: done[i],
				MaxBatch: maxBatch, MaxInFlight: maxInFl, DrainTimeout: drain, Stats: &ctr.shard,
				Flush: flushReqs[i], FlushAll: flushReqs, NonRateLimited: nonRL,
			})
			shardsDone <- struct{}{}
		}()
	}
	go shard.Ticker(window, ticks, stopTick)
	go func() {
		defer close(poolDone)
		unpack.Run(queue, workers, ctr.unpack, rtr.Route)
	}()
	// The counters are atomic, so logging them while the stages run is safe (FR10).
	go func() {
		defer close(statsDone)
		every(statsEvery, stopStats, func() {
			logStats(&ctr)
		})
	}()
	// Whatever happens below, stop every stage before returning, upstream
	// first: closing queue lets the pool drain it; once the pool has stopped
	// nothing sends to the shards, so their queues can be closed and each
	// shard makes its final flush; once the shards have stopped nothing sends
	// batches, so the writers can be stopped.
	defer func() {
		close(stopStats)
		<-statsDone
		close(queue)
		<-poolDone
		for _, q := range shardQs {
			close(q)
		}
		for range shardQs {
			<-shardsDone
		}
		close(stopTick)
		close(batchQ)
		for range writers {
			<-writersOut
		}
		slog.Info("pipeline stopped")
		logStats(&ctr)
	}()

	laddr, err = net.ResolveUDPAddr("udp", cfg.Receiver.ListenAddr)
	if err != nil {
		return fmt.Errorf("receiver.listenAddr %q: %w", cfg.Receiver.ListenAddr, err)
	}
	sock, err = net.ListenUDP("udp", laddr)
	if err != nil {
		return fmt.Errorf("bind %s: %w", cfg.Receiver.ListenAddr, err)
	}
	// The kernel caps this at udp_recvspace (req §4); a smaller grant is not an error.
	if err = sock.SetReadBuffer(cfg.Receiver.SocketBufferBytes); err != nil {
		sock.Close()
		return fmt.Errorf("set socket buffer: %w", err)
	}

	slog.Info("startup complete",
		"listen", sock.LocalAddr().String(),
		"gomaxprocs", cfg.GOMAXPROCS,
		"queueCapacity", cfg.Receiver.QueueCapacity,
		"workers", workers,
		"shards", shardCount,
		"writers", writers,
		"windowMs", cfg.Flush.WindowMs,
		"nonRateLimited", cfg.Flush.NonRateLimited)

	return receiver.Run(ctx, sock, queue, &ctr.receiver)
}

// every calls fn every interval until stop is closed.
func every(interval time.Duration, stop <-chan struct{}, fn func()) {
	var (
		t *time.Ticker // the clock
	)

	t = time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			fn()
		}
	}
}

// counters holds every stage's counters. Each field is safe to read while the
// stages run.
type counters struct {
	receiver  receiver.Stats // receive counters
	unpack    *unpack.Stats  // unpack counters; 512 KB, so not on the stack
	router    router.Stats   // router counters
	split7208 split.Stats    // 7208 splitter counters
	split7305 split.Stats    // 7305 splitter counters
	mirror    mirror.Stats   // mirror counters, shared by every shard
	shard     shard.Stats    // epoch counters, shared by every shard
	writer    writer.Stats   // writer counters
	db        db.Stats       // commit counters: tokens that matched no row
}

// logStats logs the counters of every stage, one line per stage.
func logStats(c *counters) {
	var (
		perTC []any // transcode/count pairs for the log line
	)

	slog.Info("receiver counters",
		"received", c.receiver.Received.Load(),
		"oversize", c.receiver.Oversize.Load())
	slog.Info("unpack counters",
		"datagrams", c.unpack.Datagrams.Load(),
		"packets", c.unpack.Packets.Load(),
		"malformed", c.unpack.Malformed.Load(),
		"shortMessage", c.unpack.ShortMessage.Load(),
		"netIdMismatch", c.unpack.NetIDMismatch.Load(),
		"trailingBytes", c.unpack.TrailingBytes.Load(),
		"lzoInputOverrun", c.unpack.LZOInputOverrun.Load(),
		"lzoOutputOverrun", c.unpack.LZOOutputOverrun.Load(),
		"lzoLookbehindOverrun", c.unpack.LZOLookbehindOverrun.Load(),
		"lzoInputNotConsumed", c.unpack.LZOInputNotConsumed.Load())
	c.unpack.Transcodes.Each(func(tc uint16, n uint64) {
		perTC = append(perTC, fmt.Sprint(tc), n)
	})
	slog.Info("packets per transcode", perTC...)
	slog.Info("router counters",
		"units", c.router.Units.Load(),
		"noSplitter", c.router.NoSplitter.Load(),
		"errorResponse", c.router.ErrorResponse.Load())
	slog.Info("splitter counters",
		"transcode", 7208,
		"records", c.split7208.Records.Load(),
		"clamped", c.split7208.Clamped.Load(),
		"badLength", c.split7208.BadLength.Load(),
		"outOfRange", c.split7208.OutOfRange.Load())
	slog.Info("splitter counters",
		"transcode", 7305,
		"records", c.split7305.Records.Load(),
		"badLength", c.split7305.BadLength.Load(),
		"outOfRange", c.split7305.OutOfRange.Load())
	slog.Info("mirror counters",
		"applied", c.mirror.Applied.Load(),
		"stale", c.mirror.Stale.Load(),
		"newRows", c.mirror.NewRows.Load(),
		"requeued", c.mirror.Requeued.Load())
	slog.Info("shard counters",
		"epochs", c.shard.Epochs.Load(),
		"epochEmpty", c.shard.EpochEmpty.Load(),
		"tickSkipped", c.shard.TickSkipped.Load(),
		"batches", c.shard.Batches.Load(),
		"abandoned", c.shard.Abandoned.Load(),
		"flushTriggered", c.shard.FlushTriggered.Load(),
		"flushCoalesced", c.shard.FlushCoalesced.Load())
	slog.Info("writer counters",
		"batches", c.writer.Batches.Load(),
		"rows", c.writer.Rows.Load(),
		"failed", c.writer.Failed.Load(),
		"unmatched", c.db.Unmatched.Load())
}
