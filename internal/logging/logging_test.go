package logging

import (
	"log/slog"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"DEBUG":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"":        slog.LevelInfo,
		"bogus":   slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
	}
	for in, want := range cases {
		if got := parseLevel(in); got != want {
			t.Errorf("parseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestNewDoesNotPanic(t *testing.T) {
	// New must always return a usable logger even with unknown format/level.
	for _, format := range []string{"json", "text", "weird", ""} {
		l := New(Config{Level: "info", Format: format})
		if l == nil {
			t.Fatalf("New returned nil for format %q", format)
		}
		l.Info("smoke", "format", format)
	}
}
