package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTempConfig writes a bootstrap config file and returns its path.
func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBootstrapLoadsConfigAndLogger(t *testing.T) {
	path := writeTempConfig(t, "[mysql]\ndsn = \"u:p@tcp(127.0.0.1:3306)/v3\"\n")

	base, err := Bootstrap("test-binary", path)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if base.Name != "test-binary" {
		t.Errorf("Name = %q", base.Name)
	}
	if base.Log == nil {
		t.Error("Log is nil")
	}
	if base.Cfg.MySQL.DSN == "" {
		t.Error("config not loaded")
	}
}

func TestBootstrapFailsWithoutDSN(t *testing.T) {
	// A config file present but missing the DSN must fail (no env fallback).
	path := writeTempConfig(t, "[app]\nenvironment = \"development\"\n")
	if _, err := Bootstrap("x", path); err == nil {
		t.Fatal("expected Bootstrap to fail without a DSN")
	}
}

func TestBootstrapFailsWithoutConfigFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.toml")
	if _, err := Bootstrap("x", missing); err == nil {
		t.Fatal("expected Bootstrap to fail when the config file is missing")
	}
}

func TestSignalContextCancels(t *testing.T) {
	ctx, stop := SignalContext()
	defer stop()
	// Without a signal it should stay alive; verify it is not already cancelled.
	select {
	case <-ctx.Done():
		t.Fatal("context cancelled prematurely")
	case <-time.After(10 * time.Millisecond):
	}

	// stop() releases the handler; the context remains usable.
	stop()
	if err := ctx.Err(); err != nil && err != context.Canceled {
		t.Fatalf("unexpected ctx error: %v", err)
	}
}
