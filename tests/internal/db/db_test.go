package db_test

import (
	"context"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/db"
)

func TestDescribe(t *testing.T) {
	tests := []struct {
		name string
		in   db.Target
		want string
	}{
		{"typical", db.Target{Host: "h", Port: 1521, Service: "svc", User: "u"}, "u@h:1521/svc"},
		{"fqdn and custom port", db.Target{Host: "ora.example.internal", Port: 1522, Service: "FEED", User: "feedvalve"}, "feedvalve@ora.example.internal:1522/FEED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := db.Describe(tt.in); got != tt.want {
				t.Errorf("Describe = %q, want %q", got, tt.want)
			}
		})
	}
}

// localTarget returns a db.Target pointing at 127.0.0.1:port.
func localTarget(port int) db.Target {
	return db.Target{Host: "127.0.0.1", Port: port, Service: "svc", User: "u"}
}

func assertRedacted(t *testing.T, err error, target db.Target, password string) {
	t.Helper()
	msg := err.Error()
	if !strings.Contains(msg, db.Describe(target)) {
		t.Errorf("error %q does not name the instance %q", msg, db.Describe(target))
	}
	for _, leak := range []string{password, url.QueryEscape(password), url.PathEscape(password)} {
		if strings.Contains(msg, leak) {
			t.Errorf("error leaks password form %q: %q", leak, msg)
		}
	}
}

const testPassword = "s3cr:t@/pw"

func TestOpenRefusedConnection(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() // nothing listens on port now

	target := localTarget(port)
	conn, err := db.Open(context.Background(), target, testPassword, 2*time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("Open succeeded against a closed port, want error")
	}
	assertRedacted(t, err, target, testPassword)
}

func TestOpenHonoursConnectTimeout(t *testing.T) {
	// A listener that accepts and then never says anything.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()

	target := localTarget(l.Addr().(*net.TCPAddr).Port)
	start := time.Now()
	conn, err := db.Open(context.Background(), target, testPassword, 300*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		conn.Close()
		t.Fatal("Open succeeded against a silent server, want error")
	}
	if elapsed > 3*time.Second {
		t.Errorf("Open took %v, want it bounded by the 300ms timeout", elapsed)
	}
	assertRedacted(t, err, target, testPassword)
}

// TestOpenIntegration needs a real Oracle instance. Set FEEDVALVE_TEST_CONFIG
// to a config file path and FEEDVALVE_DB_PASSWORD to run it.
func TestOpenIntegration(t *testing.T) {
	path, password := os.Getenv("FEEDVALVE_TEST_CONFIG"), os.Getenv("FEEDVALVE_DB_PASSWORD")
	if path == "" || password == "" {
		t.Skip("FEEDVALVE_TEST_CONFIG and FEEDVALVE_DB_PASSWORD not set")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	target := db.Target{Host: cfg.DB.Host, Port: cfg.DB.Port, Service: cfg.DB.Service, User: cfg.DB.User}
	timeout := time.Duration(cfg.DB.ConnectTimeoutMs) * time.Millisecond
	conn, err := db.Open(context.Background(), target, password, timeout)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()
	if err := conn.PingContext(context.Background()); err != nil {
		t.Errorf("second ping: %v", err)
	}
}
