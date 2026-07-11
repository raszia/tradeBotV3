package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"v3TradeBot/internal/config"
)

// TestRecoveryConfigWiredIntoExecutor (PR19 round 4 correction #1): the [execution.recovery]
// section is genuinely PARSED from the bootstrap file and WIRED into the executor's config
// types by this binary — a per-exchange partial override reaching the executor inherits the
// CONFIGURED global values, never hard-coded defaults.
func TestRecoveryConfigWiredIntoExecutor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	body := `
[mysql]
dsn = "u:p@tcp(h:3306)/db"
[execution]
mode = "off"
[execution.recovery]
max_attempts = 8
initial_delay_ms = 3000
max_delay_ms = 45000
total_timeout_ms = 720000
[execution.recovery.per_exchange.nobitex]
total_timeout_ms = 1800000
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	global, per := recoveryFromConfig(cfg.Execution)
	if global.MaxAttempts != 8 || global.InitialDelay != 3*time.Second ||
		global.MaxDelay != 45*time.Second || global.TotalTimeout != 12*time.Minute {
		t.Errorf("wired global recovery = %+v, want the file's 8/3s/45s/12m", global)
	}
	nb, ok := per["nobitex"]
	if !ok {
		t.Fatal("nobitex per-exchange override not wired")
	}
	if nb.TotalTimeout != 30*time.Minute {
		t.Errorf("nobitex total_timeout = %v, want the override 30m", nb.TotalTimeout)
	}
	// The partial override must inherit the CONFIGURED global (3s/45s/8), not defaults.
	if nb.MaxAttempts != 8 || nb.InitialDelay != 3*time.Second || nb.MaxDelay != 45*time.Second {
		t.Errorf("nobitex inherited %d/%v/%v, want the configured global 8/3s/45s (not hard defaults)", nb.MaxAttempts, nb.InitialDelay, nb.MaxDelay)
	}
}

// TestInvalidRecoveryConfigFailsStartup: a broken recovery window must fail config.Load — the
// binary never starts with a silently-corrected window.
func TestInvalidRecoveryConfigFailsStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	body := "[mysql]\ndsn = \"u:p@tcp(h:3306)/db\"\n[execution.recovery]\ninitial_delay_ms = 60000\nmax_delay_ms = 1000\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(path); err == nil {
		t.Fatal("Load succeeded with max_delay < initial_delay; want a hard startup error")
	}
}
