package config_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"feedvalve/internal/config"
)

func writeFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValidAppliesDefaults(t *testing.T) {
	p := writeFile(t, `{"gomaxprocs": 4, "db": {"host": "h", "service": "svc", "user": "u"}, "receiver": {"listenAddr": ":10830"}}`)
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := config.Config{
		GOMAXPROCS: 4,
		DB:         config.DB{Host: "h", Port: 1521, Service: "svc", User: "u", ConnectTimeoutMs: 10000, Writers: 4, CommitTimeoutMs: 10000},
		Receiver:   config.Receiver{ListenAddr: ":10830", SocketBufferBytes: 655360, QueueCapacity: 4096},
		Unpack:     config.Unpack{Workers: 2},
		Shards:     config.Shards{Count: 4, QueueCapacity: 4096},
		Flush:      config.Flush{WindowMs: 200, MaxBatchSize: 1000, MaxInFlightPerShard: 2, DrainTimeoutMs: 10000, NonRateLimited: []int{7305}},
		Stats:      config.Stats{IntervalMs: 60000},
	}
	if !reflect.DeepEqual(*cfg, want) {
		t.Errorf("got %+v, want %+v", *cfg, want)
	}
}

func TestLoadExplicitValuesOverrideDefaults(t *testing.T) {
	p := writeFile(t, `{"gomaxprocs": 8, "db": {"host": "h", "port": 1522, "service": "s", "user": "u", "connectTimeoutMs": 3000, "writers": 7, "commitTimeoutMs": 4000}, "receiver": {"listenAddr": "127.0.0.1:9000", "socketBufferBytes": 1024, "queueCapacity": 10}, "unpack": {"workers": 6}, "shards": {"count": 8, "queueCapacity": 20}, "flush": {"windowMs": 50, "maxBatchSize": 250, "maxInFlightPerShard": 3, "drainTimeoutMs": 2000, "nonRateLimited": [6511, 6531]}, "stats": {"intervalMs": 5000}}`)
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := config.Config{
		GOMAXPROCS: 8,
		DB:         config.DB{Host: "h", Port: 1522, Service: "s", User: "u", ConnectTimeoutMs: 3000, Writers: 7, CommitTimeoutMs: 4000},
		Receiver:   config.Receiver{ListenAddr: "127.0.0.1:9000", SocketBufferBytes: 1024, QueueCapacity: 10},
		Unpack:     config.Unpack{Workers: 6},
		Shards:     config.Shards{Count: 8, QueueCapacity: 20},
		Flush:      config.Flush{WindowMs: 50, MaxBatchSize: 250, MaxInFlightPerShard: 3, DrainTimeoutMs: 2000, NonRateLimited: []int{6511, 6531}},
		Stats:      config.Stats{IntervalMs: 5000},
	}
	if !reflect.DeepEqual(*cfg, want) {
		t.Errorf("got %+v, want %+v", *cfg, want)
	}
}

func TestTemplateLoads(t *testing.T) {
	if _, err := config.Load(filepath.Join("..", "..", "..", "config.json.template")); err != nil {
		t.Fatalf("config.json.template does not load: %v", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := config.Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("Load succeeded on missing file, want error")
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr []string // substrings that must all appear in the error
	}{
		{"malformed JSON", `{`, []string{"config"}},
		{"unknown field", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "bogus": 1}`, []string{"bogus"}},
		{"password in file", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u", "password": "x"}}`, []string{"password"}},
		{"empty host", `{"gomaxprocs": 4, "db": {"service": "s", "user": "u"}}`, []string{"db.host"}},
		{"empty service", `{"gomaxprocs": 4, "db": {"host": "h", "user": "u"}}`, []string{"db.service"}},
		{"empty user", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s"}}`, []string{"db.user"}},
		{"port zero", `{"gomaxprocs": 4, "db": {"host": "h", "port": 0, "service": "s", "user": "u"}}`, []string{"db.port"}},
		{"port too high", `{"gomaxprocs": 4, "db": {"host": "h", "port": 70000, "service": "s", "user": "u"}}`, []string{"db.port"}},
		{"gomaxprocs missing", `{"db": {"host": "h", "service": "s", "user": "u"}}`, []string{"gomaxprocs"}},
		{"gomaxprocs too high", `{"gomaxprocs": 65, "db": {"host": "h", "service": "s", "user": "u"}}`, []string{"gomaxprocs"}},
		{"timeout zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u", "connectTimeoutMs": 0}}`, []string{"db.connectTimeoutMs"}},
		{"timeout negative", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u", "connectTimeoutMs": -1}}`, []string{"db.connectTimeoutMs"}},
		{"timeout as string", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u", "connectTimeoutMs": "10s"}}`, []string{"connectTimeoutMs"}},
		{"empty listenAddr", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}}`, []string{"receiver.listenAddr"}},
		{"socket buffer zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1", "socketBufferBytes": 0}}`, []string{"receiver.socketBufferBytes"}},
		{"queue capacity negative", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1", "queueCapacity": -1}}`, []string{"receiver.queueCapacity"}},
		{"workers zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "unpack": {"workers": 0}}`, []string{"unpack.workers"}},
		{"workers too high", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "unpack": {"workers": 65}}`, []string{"unpack.workers"}},
		{"unpack queue capacity is gone", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "unpack": {"queueCapacity": 10}}`, []string{"queueCapacity"}},
		{"shard count zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "shards": {"count": 0}}`, []string{"shards.count"}},
		{"shard count too high", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "shards": {"count": 65}}`, []string{"shards.count"}},
		{"shard queue capacity zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "shards": {"queueCapacity": 0}}`, []string{"shards.queueCapacity"}},
		{"token counts capacity is no longer a setting", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "shards": {"tokenCountsCapacity": 10}}`, []string{"tokenCountsCapacity"}},
		{"writers zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u", "writers": 0}, "receiver": {"listenAddr": ":1"}}`, []string{"db.writers"}},
		{"writers too high", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u", "writers": 65}, "receiver": {"listenAddr": ":1"}}`, []string{"db.writers"}},
		{"flush window zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "flush": {"windowMs": 0}}`, []string{"flush.windowMs"}},
		{"batch size zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "flush": {"maxBatchSize": 0}}`, []string{"flush.maxBatchSize"}},
		{"batch size too high", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "flush": {"maxBatchSize": 100001}}`, []string{"flush.maxBatchSize"}},
		{"in flight zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "flush": {"maxInFlightPerShard": 0}}`, []string{"flush.maxInFlightPerShard"}},
		{"in flight too high", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "flush": {"maxInFlightPerShard": 65}}`, []string{"flush.maxInFlightPerShard"}},
		{"drain timeout zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "flush": {"drainTimeoutMs": 0}}`, []string{"flush.drainTimeoutMs"}},
		{"non-rate-limited code negative", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "flush": {"nonRateLimited": [6511, -1]}}`, []string{"flush.nonRateLimited"}},
		{"non-rate-limited code too high", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "flush": {"nonRateLimited": [65536]}}`, []string{"flush.nonRateLimited"}},
		{"schema file is no longer a setting", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u", "schemaFile": "schema.json"}, "receiver": {"listenAddr": ":1"}}`, []string{"schemaFile"}},
		{"commit timeout zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u", "commitTimeoutMs": 0}, "receiver": {"listenAddr": ":1"}}`, []string{"db.commitTimeoutMs"}},
		{"stats interval zero", `{"gomaxprocs": 4, "db": {"host": "h", "service": "s", "user": "u"}, "receiver": {"listenAddr": ":1"}, "stats": {"intervalMs": 0}}`, []string{"stats.intervalMs"}},
		{"all faults reported", `{"db": {}}`, []string{"gomaxprocs", "db.host", "db.service", "db.user", "receiver.listenAddr"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Load(writeFile(t, tt.body))
			if err == nil {
				t.Fatal("Load succeeded, want error")
			}
			for _, s := range tt.wantErr {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("error %q does not mention %q", err, s)
				}
			}
		})
	}
}
