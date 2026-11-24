package app_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"feedvalve/internal/app"
)

// writeConfig writes a config whose database listens on dbPort.
func writeConfig(t *testing.T, dbPort int) string {
	t.Helper()
	dir := t.TempDir()
	body := fmt.Sprintf(`{
		"gomaxprocs": 2,
		"db": {"host": "127.0.0.1", "port": %d, "service": "svc", "user": "u", "connectTimeoutMs": 2000},
		"receiver": {"listenAddr": "127.0.0.1:0"}
	}`, dbPort)
	p := filepath.Join(dir, "config.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// closedPort returns a loopback TCP port with nothing listening on it.
func closedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestRunStartupErrors(t *testing.T) {
	tests := []struct {
		name     string
		cfgPath  func(t *testing.T) string
		password string
		wantErr  string
	}{
		{
			name:     "missing config file",
			cfgPath:  func(t *testing.T) string { return filepath.Join(t.TempDir(), "nope.json") },
			password: "pw",
			wantErr:  "config",
		},
		{
			name:     "password not set",
			cfgPath:  func(t *testing.T) string { return writeConfig(t, closedPort(t)) },
			password: "",
			wantErr:  app.PasswordEnv,
		},
		{
			name:     "database unreachable",
			cfgPath:  func(t *testing.T) string { return writeConfig(t, closedPort(t)) },
			password: "pw-secret",
			wantErr:  "u@127.0.0.1:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(app.PasswordEnv, tt.password)
			err := app.Run(context.Background(), tt.cfgPath(t))
			if err == nil {
				t.Fatal("Run succeeded, want error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not mention %q", err, tt.wantErr)
			}
			if tt.password != "" && strings.Contains(err.Error(), tt.password) {
				t.Errorf("error leaks the password: %q", err)
			}
		})
	}
}
