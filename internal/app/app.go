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
	"path/filepath"
	"runtime"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/db"
	"feedvalve/internal/mirror"
	"feedvalve/internal/receiver"
	"feedvalve/internal/router"
	"feedvalve/internal/schema"
	"feedvalve/internal/shard"
	"feedvalve/internal/split"
	"feedvalve/internal/stats"
	"feedvalve/internal/unpack"
	"feedvalve/internal/writer"
)

// PasswordEnv names the environment variable holding the Oracle password,
// the only place it may come from (req §7).
const PasswordEnv = "FEEDVALVE_DB_PASSWORD"

// Run starts FeedValve from the config file at cfgPath and keeps it running
// until ctx is cancelled (returns nil) or something fatal happens. Errors are
// returned rather than exiting, so cleanup always runs (HLD §6.10).
func Run(ctx context.Context, cfgPath string) error {
	var (
		cfg        *config.Config               // the loaded, validated config file
		sch        *schema.Schema               // the column-mapping file: tables and mirror fields
		schPath    string                       // where it is; relative paths are from the config's directory
		commitTO   time.Duration                // bound on one batch's transaction
		password   string                       // Oracle password, from the environment only
		target     db.Target                    // which Oracle instance to connect to
		dbTimeout  time.Duration                // how long the startup ping may take
		workers    int                          // unpack pool size
		shardCount int                          // number of shards
		shardCap   int                          // capacity of each shard queue
		conn       *sql.DB                      // Oracle connection pool
		laddr      *net.UDPAddr                 // address to receive the feed on
		sock       *net.UDPConn                 // the feed's UDP socket
		queue      chan receiver.Datagram       // datagrams waiting for the unpack pool
		st         receiver.Stats               // receive counters, logged on exit
		ust        *unpack.Stats                // unpack counters; 512 KB, so not on the stack
		rst        router.Stats                 // router counters
		sst        split.Stats                  // splitter counters
		mst        mirror.Stats                 // mirror counters, shared by every shard
		shst       shard.Stats                  // epoch counters, shared by every shard
		wst        writer.Stats                 // writer counters
		dst        db.Stats                     // commit counters: tokens that matched no row
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
		tokens     *stats.TokenCounts           // units per (token, transcode)
		shardQs    []chan mirror.Contribution   // one queue per shard
		shardSend  []chan<- mirror.Contribution // the same queues, send side, for the router
		mirrors    []*mirror.Mirror             // one per shard, owned by its goroutine
		rtr        *router.Router               // sends each message's units to their shards
		poolDone   chan struct{}                // closed when the unpack pool has drained and stopped
		shardsDone chan struct{}                // receives once per shard when it has drained its queue
		err        error                        // result of each startup step
	)

	// Config reads.
	cfg, err = config.Load(cfgPath)
	if err != nil {
		return err
	}
	schPath = cfg.DB.SchemaFile
	if !filepath.IsAbs(schPath) {
		schPath = filepath.Join(filepath.Dir(cfgPath), schPath)
	}
	sch, err = schema.Load(schPath)
	if err != nil {
		return err
	}
	if err = checkFields(sch, split.OnlyMBPFields); err != nil {
		return fmt.Errorf("schema %s: %w", schPath, err)
	}
	password = os.Getenv(PasswordEnv)
	target = db.Target{Host: cfg.DB.Host, Port: cfg.DB.Port, Service: cfg.DB.Service, User: cfg.DB.User}
	dbTimeout = time.Duration(cfg.DB.ConnectTimeoutMs) * time.Millisecond
	workers = cfg.Unpack.Workers
	shardCount = cfg.Shards.Count
	shardCap = cfg.Shards.QueueCapacity
	writers = cfg.DB.Writers
	window = time.Duration(cfg.Flush.WindowMs) * time.Millisecond
	maxBatch = cfg.Flush.MaxBatchSize
	maxInFl = cfg.Flush.MaxInFlightPerShard
	drain = time.Duration(cfg.Flush.DrainTimeoutMs) * time.Millisecond
	commitTO = time.Duration(cfg.DB.CommitTimeoutMs) * time.Millisecond

	// Allocations.
	queue = make(chan receiver.Datagram, cfg.Receiver.QueueCapacity)
	ust = new(unpack.Stats)
	tokens = stats.NewTokenCounts(cfg.Shards.TokenCountsCapacity)
	shardQs = make([]chan mirror.Contribution, shardCount)
	shardSend = make([]chan<- mirror.Contribution, shardCount)
	for i := range shardQs {
		shardQs[i] = make(chan mirror.Contribution, shardCap)
		shardSend[i] = shardQs[i]
	}
	mirrors = make([]*mirror.Mirror, shardCount)
	for i := range mirrors {
		mirrors[i] = mirror.New(&mst)
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
	splitters = map[uint16]router.Splitter{
		7208: split.OnlyMBP(split.NewOnlyMBPMapper(sch), &sst),
	}

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
	for i, d := range sch.Dests {
		slog.Info("destination", "dest", i, "table", d.Table, "key", d.Key, "columns", len(d.Columns))
	}

	// Transcodes without a Splitter are counted as NoSplitter and dropped.
	rtr = router.New(splitters, shardSend, tokens, &rst)

	// Writers first, then shards, then the tick (HLD §11.4). Every writer
	// takes from the one batchQ, so they commit in parallel; each has its own
	// schema-driven commit.
	for range writers {
		go func() {
			writer.Run(batchQ, done, db.NewCommit(conn, sch, commitTO, &dst), &wst)
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
				MaxBatch: maxBatch, MaxInFlight: maxInFl, DrainTimeout: drain, Stats: &shst,
				Flush: flushReqs[i], FlushAll: flushReqs, NonRateLimited: nonRL,
			})
			shardsDone <- struct{}{}
		}()
	}
	go shard.Ticker(window, ticks, stopTick)
	go func() {
		defer close(poolDone)
		unpack.Run(queue, workers, ust, rtr.Route)
	}()
	// Whatever happens below, stop every stage before returning, upstream
	// first: closing queue lets the pool drain it; once the pool has stopped
	// nothing sends to the shards, so their queues can be closed and each
	// shard makes its final flush; once the shards have stopped nothing sends
	// batches, so the writers can be stopped.
	defer func() {
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
		logStats(&st, ust, &rst, &sst, &mst, &shst, &wst, &dst, tokens)
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

	return receiver.Run(ctx, sock, queue, &st)
}

// logStats logs the counters of every stage once the pipeline has stopped.
func logStats(rst *receiver.Stats, ust *unpack.Stats, rtst *router.Stats, sst *split.Stats, mst *mirror.Stats, shst *shard.Stats, wst *writer.Stats, dst *db.Stats, tokens *stats.TokenCounts) {
	var (
		perTC []any // transcode/count pairs for the log line
	)

	slog.Info("receiver stopped",
		"received", rst.Received.Load(),
		"oversize", rst.Oversize.Load())
	slog.Info("unpack stopped",
		"datagrams", ust.Datagrams.Load(),
		"packets", ust.Packets.Load(),
		"malformed", ust.Malformed.Load(),
		"shortMessage", ust.ShortMessage.Load(),
		"netIdMismatch", ust.NetIDMismatch.Load(),
		"trailingBytes", ust.TrailingBytes.Load(),
		"lzoInputOverrun", ust.LZOInputOverrun.Load(),
		"lzoOutputOverrun", ust.LZOOutputOverrun.Load(),
		"lzoLookbehindOverrun", ust.LZOLookbehindOverrun.Load(),
		"lzoInputNotConsumed", ust.LZOInputNotConsumed.Load())
	ust.Transcodes.Each(func(tc uint16, n uint64) {
		perTC = append(perTC, fmt.Sprint(tc), n)
	})
	slog.Info("packets per transcode", perTC...)
	slog.Info("router stopped",
		"units", rtst.Units.Load(),
		"noSplitter", rtst.NoSplitter.Load(),
		"errorResponse", rtst.ErrorResponse.Load(),
		"tokenTranscodePairs", tokens.Len())
	slog.Info("splitters stopped",
		"records", sst.Records.Load(),
		"clamped", sst.Clamped.Load(),
		"badLength", sst.BadLength.Load())
	slog.Info("mirrors stopped",
		"applied", mst.Applied.Load(),
		"stale", mst.Stale.Load(),
		"newRows", mst.NewRows.Load(),
		"badDest", mst.BadDest.Load(),
		"slotsFull", mst.SlotsFull.Load(),
		"requeued", mst.Requeued.Load())
	slog.Info("shards stopped",
		"epochs", shst.Epochs.Load(),
		"epochEmpty", shst.EpochEmpty.Load(),
		"tickSkipped", shst.TickSkipped.Load(),
		"batches", shst.Batches.Load(),
		"abandoned", shst.Abandoned.Load(),
		"flushTriggered", shst.FlushTriggered.Load(),
		"flushCoalesced", shst.FlushCoalesced.Load())
	slog.Info("writers stopped",
		"batches", wst.Batches.Load(),
		"rows", wst.Rows.Load(),
		"failed", wst.Failed.Load(),
		"unmatched", dst.Unmatched.Load())
}

// checkFields reports every field the schema names that no mapper produces,
// so a misspelt field fails startup instead of silently never being written.
// produced lists each mapper's fields.
func checkFields(s *schema.Schema, produced ...[]string) error {
	var (
		known map[string]bool // every field some mapper produces
		errs  []error         // one per unknown field
	)

	known = make(map[string]bool)
	for _, fields := range produced {
		for _, f := range fields {
			known[f] = true
		}
	}
	for _, d := range s.Dests {
		for _, c := range d.Columns {
			if c.Field != "" && !known[c.Field] {
				errs = append(errs, fmt.Errorf("%s.%s: field %q is produced by no transcode", d.Table, c.Name, c.Field))
			}
		}
	}
	return errors.Join(errs...)
}
