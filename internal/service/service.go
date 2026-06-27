// Package service holds the small bootstrap glue shared by every cmd/* binary:
// loading bootstrap config, building the logger, and wiring OS-signal-driven
// graceful shutdown. Keeping this in one place ensures all binaries start and
// stop consistently and log their (redacted) config the same way.
//
// It deliberately does NOT open the database or Redis — each binary opens only
// the resources it needs, so e.g. the dashboard can restart without ever
// touching the trading path.
package service

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"v3TradeBot/internal/config"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/logging"
	"v3TradeBot/internal/migrate"
)

// Base holds the dependencies every binary needs.
type Base struct {
	Name string
	Cfg  config.Config
	Log  *slog.Logger
}

// ConfigFlag defines and parses the -config flag and returns the chosen path.
// This flag is the ONLY runtime input a binary takes besides the config file
// itself — the project does not read configuration from environment variables.
// Call it exactly once from main (it uses the global flag set).
func ConfigFlag() string {
	path := flag.String("config", config.DefaultConfigPath, "path to the bootstrap config TOML file")
	flag.Parse()
	return *path
}

// Bootstrap loads bootstrap config from configPath (file only; no env), builds
// the structured logger tagged with the binary name, and logs a redacted startup
// summary. It returns an error rather than exiting so main can control the exit
// code and emit a final log line.
func Bootstrap(name, configPath string) (Base, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return Base{}, err
	}
	log := logging.New(cfg.Logging()).With("binary", name)
	// cfg implements slog.LogValuer (LogValue → Redacted), so secrets never hit
	// the log here even though we pass the whole config.
	log.Info("bootstrap", "environment", cfg.App.Environment, "config", cfg)
	return Base{Name: name, Cfg: cfg, Log: log}, nil
}

// SignalContext returns a context cancelled on SIGINT or SIGTERM, plus a stop
// function to release the signal handler. Binaries select on ctx.Done() for
// graceful shutdown.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// RunWithDB is the standard lifecycle for a DB-backed service binary:
//
//  1. bootstrap config + logger,
//  2. open the MariaDB pool,
//  3. fail fast if the schema is behind the code (services never self-migrate),
//  4. run `run` until a shutdown signal, then clean up.
//
// `run` owns the service's lifetime: it should start its work and block until
// ctx is cancelled (returning nil on clean shutdown) or return a fatal error.
// Passing a nil `run` yields an idle service that simply waits for shutdown —
// used by the PR1 skeleton binaries whose features land in later PRs.
func RunWithDB(name, configPath string, run func(ctx context.Context, base Base, store *db.Store) error) error {
	base, err := Bootstrap(name, configPath)
	if err != nil {
		return err
	}
	ctx, stop := SignalContext()
	defer stop()

	store, err := db.New(ctx, base.Cfg.MySQL)
	if err != nil {
		return fmt.Errorf("%s: connect mysql: %w", name, err)
	}
	defer store.Close()

	// Hard gate: a binary must never operate on a schema older than its code.
	if err := migrate.EnsureCurrent(ctx, store.DB(), migrate.FS); err != nil {
		return err
	}

	if run == nil {
		base.Log.Info("ready (idle skeleton; feature wiring lands in a later PR)")
		<-ctx.Done()
	} else {
		if err := run(ctx, base, store); err != nil {
			return err
		}
	}
	base.Log.Info("shutting down")
	return nil
}
