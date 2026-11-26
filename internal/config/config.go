// Package config loads and validates FeedValve's JSON configuration file.
//
// The Oracle password is deliberately absent: it comes only from the
// environment (req §7). Unknown fields are rejected, so a "password" key in
// the file fails loudly rather than being silently ignored.
//
// All times are integer milliseconds, in fields suffixed Ms.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

// Config holds the settings startup needs. It grows as pipeline stages land.
type Config struct {
	// GOMAXPROCS is set from config, never from the detected CPU count
	// (req §4, ADR-009): the capped SMT-8 LPAR reports far more logical
	// CPUs than its entitlement of 4.
	GOMAXPROCS int      `json:"gomaxprocs"`
	DB         DB       `json:"db"`
	Receiver   Receiver `json:"receiver"`
	Unpack     Unpack   `json:"unpack"`
	Shards     Shards   `json:"shards"`
	Flush      Flush    `json:"flush"`
	Stats      Stats    `json:"stats"`
	Log        Log      `json:"log"`
}

// Log configures logging.
type Log struct {
	// Level is the lowest level logged: "debug", "info" (the default),
	// "warn" or "error", in any case. At debug, every contribution queued to
	// a shard is logged; that is for troubleshooting only, as it writes
	// synchronously on the receive path.
	Level slog.Level `json:"level"`
}

// Stats configures the operational counters (FR10).
type Stats struct {
	// IntervalMs is how often every stage's counters are logged while
	// running; they are logged once more at shutdown.
	IntervalMs int `json:"intervalMs"`
}

// Receiver configures the UDP socket and the queue it feeds.
type Receiver struct {
	ListenAddr string `json:"listenAddr"`
	// SocketBufferBytes is requested via SO_RCVBUF; the kernel caps it at
	// udp_recvspace (req §4).
	SocketBufferBytes int `json:"socketBufferBytes"`
	QueueCapacity     int `json:"queueCapacity"`
}

// Unpack configures the worker pool that splits datagrams into sub-packets
// and decompresses them. Startup-only for now.
type Unpack struct {
	Workers int `json:"workers"`
}

// Shards configures the shards and their queues. All startup-only: the
// shard count fixes which shard owns each token (HLD ADR-002).
type Shards struct {
	Count int `json:"count"`
	// QueueCapacity bounds each shard's queue. Each entry is about 390 bytes.
	QueueCapacity int `json:"queueCapacity"`
}

// Flush configures the flush epochs (HLD §7.3, ADR-018). Startup-only for now.
type Flush struct {
	// WindowMs is W, the global tick: every W each shard writes out
	// everything that went dirty since its last epoch.
	WindowMs int `json:"windowMs"`
	// MaxBatchSize caps the rows in one batch, i.e. one transaction (NFR6).
	MaxBatchSize int `json:"maxBatchSize"`
	// MaxInFlightPerShard is how many of one shard's batches may commit at
	// once. It limits concurrency, not how much an epoch writes.
	MaxInFlightPerShard int `json:"maxInFlightPerShard"`
	// DrainTimeoutMs bounds the final flush at shutdown (HLD §11.4).
	DrainTimeoutMs int `json:"drainTimeoutMs"`
	// NonRateLimited lists the transcodes whose merges flush every shard at
	// once instead of waiting for the tick (HLD ADR-016). Everything else is
	// rate-limited: coalesced and flushed every WindowMs. The default is
	// [7305]: a security master change is written at once. A list in the
	// file replaces the default, and [] makes everything rate-limited.
	NonRateLimited []int `json:"nonRateLimited"`
}

// DB identifies the Oracle instance. The password is not here; see the package doc.
type DB struct {
	Host             string `json:"host"`
	Port             int    `json:"port"`
	Service          string `json:"service"`
	User             string `json:"user"`
	ConnectTimeoutMs int    `json:"connectTimeoutMs"`
	// Writers is the size of the writer pool, one Oracle session each; it is
	// independent of the shard count (HLD ADR-005).
	Writers int `json:"writers"`
	// CommitTimeoutMs bounds one batch's transaction, so a hung commit
	// cannot stall a writer or the shutdown drain.
	CommitTimeoutMs int `json:"commitTimeoutMs"`
}

const (
	defaultPort             = 1521
	defaultConnectTimeoutMs = 10000
	maxGOMAXPROCS           = 64
	// 640 KB, the LPAR's udp_recvspace (req §4).
	defaultSocketBufferBytes = 655360
	defaultQueueCapacity     = 4096
	defaultWorkers           = 2
	maxWorkers               = 64
	defaultShards            = 4
	maxShards                = 64
	defaultShardQueueCap     = 4096
	defaultWriters           = 4
	defaultCommitTimeoutMs   = 10000
	maxWriters               = 64
	defaultWindowMs          = 200
	defaultMaxBatchSize      = 1000
	maxMaxBatchSize          = 100000
	defaultMaxInFlight       = 2
	maxMaxInFlight           = 64
	defaultDrainTimeoutMs    = 10000
	defaultStatsIntervalMs   = 60000
	maxTranscode             = 65535
)

// Load reads the config file at path, fills in defaults for anything it
// leaves out, and checks every value. All problems are reported together.
func Load(path string) (*Config, error) {
	var (
		b   []byte        // raw file contents
		cfg Config        // the result, pre-filled with defaults
		dec *json.Decoder // strict decoder: unknown fields are errors
		err error         // result of each step
	)

	// Allocations.
	cfg = Config{
		DB:       DB{Port: defaultPort, ConnectTimeoutMs: defaultConnectTimeoutMs, Writers: defaultWriters, CommitTimeoutMs: defaultCommitTimeoutMs},
		Receiver: Receiver{SocketBufferBytes: defaultSocketBufferBytes, QueueCapacity: defaultQueueCapacity},
		Unpack:   Unpack{Workers: defaultWorkers},
		Shards:   Shards{Count: defaultShards, QueueCapacity: defaultShardQueueCap},
		Flush:    Flush{WindowMs: defaultWindowMs, MaxBatchSize: defaultMaxBatchSize, MaxInFlightPerShard: defaultMaxInFlight, DrainTimeoutMs: defaultDrainTimeoutMs, NonRateLimited: []int{7305}},
		Stats:    Stats{IntervalMs: defaultStatsIntervalMs},
		Log:      Log{Level: slog.LevelInfo},
	}

	// Logic.
	b, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	dec = json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	if err = cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &cfg, nil
}

// validate checks every config value and returns all problems found, joined
// into one error, or nil if the config is usable.
func (c *Config) validate() error {
	var (
		errs []error // every problem found
	)

	if c.GOMAXPROCS < 1 || c.GOMAXPROCS > maxGOMAXPROCS {
		errs = append(errs, fmt.Errorf("gomaxprocs must be 1..%d, got %d", maxGOMAXPROCS, c.GOMAXPROCS))
	}
	if c.DB.Host == "" {
		errs = append(errs, errors.New("db.host is required"))
	}
	if c.DB.Port < 1 || c.DB.Port > 65535 {
		errs = append(errs, fmt.Errorf("db.port must be 1..65535, got %d", c.DB.Port))
	}
	if c.DB.Service == "" {
		errs = append(errs, errors.New("db.service is required"))
	}
	if c.DB.User == "" {
		errs = append(errs, errors.New("db.user is required"))
	}
	if c.DB.ConnectTimeoutMs <= 0 {
		errs = append(errs, fmt.Errorf("db.connectTimeoutMs must be positive, got %d", c.DB.ConnectTimeoutMs))
	}
	if c.DB.Writers < 1 || c.DB.Writers > maxWriters {
		errs = append(errs, fmt.Errorf("db.writers must be 1..%d, got %d", maxWriters, c.DB.Writers))
	}
	if c.DB.CommitTimeoutMs <= 0 {
		errs = append(errs, fmt.Errorf("db.commitTimeoutMs must be positive, got %d", c.DB.CommitTimeoutMs))
	}
	if c.Receiver.ListenAddr == "" {
		errs = append(errs, errors.New("receiver.listenAddr is required"))
	}
	if c.Receiver.SocketBufferBytes <= 0 {
		errs = append(errs, fmt.Errorf("receiver.socketBufferBytes must be positive, got %d", c.Receiver.SocketBufferBytes))
	}
	if c.Receiver.QueueCapacity <= 0 {
		errs = append(errs, fmt.Errorf("receiver.queueCapacity must be positive, got %d", c.Receiver.QueueCapacity))
	}
	if c.Unpack.Workers < 1 || c.Unpack.Workers > maxWorkers {
		errs = append(errs, fmt.Errorf("unpack.workers must be 1..%d, got %d", maxWorkers, c.Unpack.Workers))
	}
	if c.Shards.Count < 1 || c.Shards.Count > maxShards {
		errs = append(errs, fmt.Errorf("shards.count must be 1..%d, got %d", maxShards, c.Shards.Count))
	}
	if c.Shards.QueueCapacity <= 0 {
		errs = append(errs, fmt.Errorf("shards.queueCapacity must be positive, got %d", c.Shards.QueueCapacity))
	}
	if c.Flush.WindowMs <= 0 {
		errs = append(errs, fmt.Errorf("flush.windowMs must be positive, got %d", c.Flush.WindowMs))
	}
	if c.Flush.MaxBatchSize < 1 || c.Flush.MaxBatchSize > maxMaxBatchSize {
		errs = append(errs, fmt.Errorf("flush.maxBatchSize must be 1..%d, got %d", maxMaxBatchSize, c.Flush.MaxBatchSize))
	}
	if c.Flush.MaxInFlightPerShard < 1 || c.Flush.MaxInFlightPerShard > maxMaxInFlight {
		errs = append(errs, fmt.Errorf("flush.maxInFlightPerShard must be 1..%d, got %d", maxMaxInFlight, c.Flush.MaxInFlightPerShard))
	}
	if c.Flush.DrainTimeoutMs <= 0 {
		errs = append(errs, fmt.Errorf("flush.drainTimeoutMs must be positive, got %d", c.Flush.DrainTimeoutMs))
	}
	for _, code := range c.Flush.NonRateLimited {
		if code < 0 || code > maxTranscode {
			errs = append(errs, fmt.Errorf("flush.nonRateLimited codes must be 0..%d, got %d", maxTranscode, code))
		}
	}
	if c.Stats.IntervalMs <= 0 {
		errs = append(errs, fmt.Errorf("stats.intervalMs must be positive, got %d", c.Stats.IntervalMs))
	}
	return errors.Join(errs...)
}
