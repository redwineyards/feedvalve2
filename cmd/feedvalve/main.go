// Command feedvalve ingests the exchange broadcast feed. All logic lives in
// internal/app; main only parses flags, wires signals and sets the exit code.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"feedvalve/internal/app"
)

// main reads the command-line flags, runs FeedValve until it is told to stop,
// and exits with status 1 if it stopped because of an error.
func main() {
	var (
		cfgPath *string            // path to the JSON config file, from -config
		ctx     context.Context    // cancelled on SIGINT or SIGTERM
		stop    context.CancelFunc // stops listening for those signals
		err     error              // why FeedValve stopped, if it failed
	)

	// Config reads.
	cfgPath = flag.String("config", "config.json", "path to the JSON config file")
	flag.Parse()

	// Allocations.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	// Logic.
	ctx, stop = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = app.Run(ctx, *cfgPath)
	stop()
	if err != nil {
		slog.Error("feedvalve stopped", "err", err)
		os.Exit(1)
	}
}
