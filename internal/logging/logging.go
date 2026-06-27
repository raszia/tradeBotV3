// Package logging centralises construction of the structured slog.Logger used
// by every binary. Centralising it keeps log level/format consistent across the
// fleet and gives a single place to evolve handlers (e.g. routing a copy of
// records into the async app_logs table in a later PR).
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Config controls logger construction. It is the bootstrap [config.LogConfig];
// kept as a local struct so this package does not import config (avoiding an
// import cycle once config grows).
type Config struct {
	// Level is one of debug|info|warn|error (case-insensitive). Unknown values
	// fall back to info.
	Level string
	// Format is json|text (case-insensitive). Unknown values fall back to json.
	Format string
}

// New builds a slog.Logger writing to stderr. JSON is the default because the
// logs are intended to be machine-ingested; text is offered for local dev.
func New(cfg Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(cfg.Level)}

	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(cfg.Format)) {
	case "text":
		handler = slog.NewTextHandler(os.Stderr, opts)
	default:
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(handler)
}

// parseLevel maps a string level to slog.Level, defaulting to Info so a
// misconfigured level never silences logging entirely.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
