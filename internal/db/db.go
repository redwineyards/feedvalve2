// Package db opens and checks the Oracle connection through the pure-Go
// go-ora driver (req §7).
//
// The DSN embeds the password, so it is never logged or returned in an
// error: errors name the instance by Describe instead (req §7, HLD §6.9).
// This package does not read configuration; callers pass in what it needs.
package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

// Target identifies an Oracle instance. It never carries the password.
type Target struct {
	Host    string
	Port    int
	Service string
	User    string
}

// Open connects to the Oracle instance and checks it answers within timeout.
// If it fails, the error names the instance but never the password.
func Open(ctx context.Context, t Target, password string, timeout time.Duration) (*sql.DB, error) {
	var (
		dsn    string             // connection URL; contains the password, never logged
		conn   *sql.DB            // the connection pool handed back to the caller
		cancel context.CancelFunc // releases the ping deadline
		err    error              // result of opening or pinging
	)

	dsn = go_ora.BuildUrl(t.Host, t.Port, t.Service, t.User, password, nil)

	conn, err = sql.Open("oracle", dsn)
	if err != nil {
		return nil, fmt.Errorf("oracle %s: %s", Describe(t), redact(err.Error(), dsn, password))
	}
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()
	if err = conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("oracle %s: ping: %s", Describe(t), redact(err.Error(), dsn, password))
	}
	return conn, nil
}

// Describe returns a log-safe name for the instance: "user@host:port/service".
func Describe(t Target) string {
	return t.User + "@" + t.Host + ":" + strconv.Itoa(t.Port) + "/" + t.Service
}

// redact hides the connection URL and the password (in every encoding) in s,
// in case the driver puts either into an error message.
func redact(s, dsn, password string) string {
	var (
		forms []string // the password as written, path-escaped and query-escaped
	)

	forms = []string{password, url.PathEscape(password), url.QueryEscape(password)}

	s = strings.ReplaceAll(s, dsn, "***")
	if password == "" {
		return s
	}
	for _, p := range forms {
		s = strings.ReplaceAll(s, p, "***")
	}
	return s
}
