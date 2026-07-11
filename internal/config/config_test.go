package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadFromFileWithDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	contents := `
[app]
environment = "staging"

[mysql]
dsn = "u:p@tcp(127.0.0.1:3306)/v3"

[log]
level = "debug"
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.Environment != "staging" {
		t.Errorf("environment = %q, want staging", cfg.App.Environment)
	}
	// Defaults applied:
	if cfg.Redis.Addr != "127.0.0.1:6379" {
		t.Errorf("redis addr default = %q", cfg.Redis.Addr)
	}
	if cfg.Dashboard.ListenAddr != "127.0.0.1:8080" {
		t.Errorf("dashboard addr default = %q", cfg.Dashboard.ListenAddr)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("log format default = %q", cfg.Log.Format)
	}
}

func TestLoadMissingFileIsHardError(t *testing.T) {
	// No env fallback exists, so a missing config file must be a hard error
	// rather than a silent all-defaults startup.
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.toml"))
	if err == nil {
		t.Fatal("expected error when config file is missing")
	}
	if !strings.Contains(err.Error(), "read") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateRequiresDSN(t *testing.T) {
	// A file that omits the DSN must fail Validate (never run without source of
	// truth). There is no environment variable that could supply it.
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte("[app]\nenvironment=\"development\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when DSN missing")
	}
	if !strings.Contains(err.Error(), "mysql.dsn") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRedactedMasksSecrets(t *testing.T) {
	cfg := Config{
		MySQL:    MySQLConfig{DSN: "trader:supersecret@tcp(db:3306)/v3"},
		Redis:    RedisConfig{Password: "rpw"},
		Security: SecurityConfig{MasterKey: "mk"},
	}
	r := cfg.Redacted()

	if strings.Contains(r.MySQL.DSN, "supersecret") {
		t.Errorf("DSN password leaked: %q", r.MySQL.DSN)
	}
	if !strings.Contains(r.MySQL.DSN, "trader") || !strings.Contains(r.MySQL.DSN, "db:3306") {
		t.Errorf("redacted DSN lost identifying info: %q", r.MySQL.DSN)
	}
	if r.Redis.Password != "***" {
		t.Errorf("redis password not masked: %q", r.Redis.Password)
	}
	if r.Security.MasterKey != "***" {
		t.Errorf("master key not masked: %q", r.Security.MasterKey)
	}
	// Original must be untouched (Redacted returns a copy).
	if cfg.MySQL.DSN != "trader:supersecret@tcp(db:3306)/v3" {
		t.Error("Redacted mutated the original config")
	}
}

func TestLogValueNeverLeaksSecrets(t *testing.T) {
	cfg := Config{
		App:      AppConfig{Environment: "production"},
		MySQL:    MySQLConfig{DSN: "trader:supersecret@tcp(db:3306)/v3"},
		Redis:    RedisConfig{Addr: "r:6379", Password: "redispw"},
		Security: SecurityConfig{MasterKey: "topsecretkey"},
	}

	// Render through a real slog JSON handler the same way the binaries do.
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.Info("bootstrap", "config", cfg)
	out := buf.String()

	for _, secret := range []string{"supersecret", "redispw", "topsecretkey"} {
		if strings.Contains(out, secret) {
			t.Fatalf("secret %q leaked into log output: %s", secret, out)
		}
	}
	// Sanity: identifying (non-secret) info is present, and masks are emitted.
	if !strings.Contains(out, "trader") || !strings.Contains(out, "db:3306") {
		t.Errorf("expected redacted DSN identifiers in log: %s", out)
	}
	if !strings.Contains(out, "***") {
		t.Errorf("expected masked markers in log: %s", out)
	}
}

func TestRedactDSNEdgeCases(t *testing.T) {
	cases := map[string]string{
		"":                    "",
		"nopassworddsn":       "nopassworddsn",
		"user@tcp(h:1)/db":    "user@tcp(h:1)/db",
		"user:pw@tcp(h:1)/db": "user:***@tcp(h:1)/db",
		// Password contains '@'; driver splits on the LAST '@', so the whole
		// password (everything after the first ':') is masked.
		"u:p@w@tcp(h:1)/db": "u:***@tcp(h:1)/db",
	}
	for in, want := range cases {
		if got := redactDSN(in); got != want {
			t.Errorf("redactDSN(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestExecutionModeDefaultIsSafe: the zero value (no [execution] section) is "off" —
// dry-run/live must be EXPLICIT so a live or simulated order can never run by default.
func TestExecutionModeDefaultIsSafe(t *testing.T) {
	var e ExecutionConfig // zero value, as when the TOML omits [execution]
	if e.IsDryRun() {
		t.Error("default execution mode must NOT be dry-run")
	}
	if e.IsLive() {
		t.Error("default execution mode must NOT be live")
	}
	if dry := (ExecutionConfig{Mode: ExecutionDryRun}); !dry.IsDryRun() {
		t.Error("explicit dry_run should report IsDryRun")
	}
	if live := (ExecutionConfig{Mode: ExecutionLive}); !live.IsLive() {
		t.Error("explicit live should report IsLive")
	}
}

// loadTOML writes body to a temp config file and Loads it (DSN included so Validate runs).
func loadTOML(t *testing.T, body string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("[mysql]\ndsn=\"u:p@tcp(h:3306)/db\"\n"+body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

// TestRecoveryConfigParsedAndResolved (PR19 round 4): the [execution.recovery] section is
// genuinely runtime-parsed; a full per-exchange override wins; a PARTIAL per-exchange override
// inherits every unset field from the CONFIGURED global values — never hard-coded defaults.
func TestRecoveryConfigParsedAndResolved(t *testing.T) {
	cfg, err := loadTOML(t, `
[execution]
mode = "off"
[execution.recovery]
max_attempts = 9
initial_delay_ms = 2000
max_delay_ms = 40000
total_timeout_ms = 600000
[execution.recovery.per_exchange.nobitex]
max_attempts = 12
[execution.recovery.per_exchange.wallex]
max_attempts = 3
initial_delay_ms = 500
max_delay_ms = 5000
total_timeout_ms = 60000
`)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	g, per := cfg.Execution.ResolvedRecovery()
	if g.MaxAttempts != 9 || g.InitialDelay != 2*time.Second || g.MaxDelay != 40*time.Second || g.TotalTimeout != 10*time.Minute {
		t.Errorf("global resolved = %+v, want 9/2s/40s/10m (configured values, not defaults)", g)
	}
	// Partial override: max_attempts set, everything else INHERITS THE CONFIGURED GLOBAL
	// (2s/40s/10m) — not the hard defaults (1s/30s/5m).
	nb := per["nobitex"]
	if nb.MaxAttempts != 12 {
		t.Errorf("nobitex max_attempts = %d, want the override 12", nb.MaxAttempts)
	}
	if nb.InitialDelay != 2*time.Second || nb.MaxDelay != 40*time.Second || nb.TotalTimeout != 10*time.Minute {
		t.Errorf("nobitex partial override inherited %v/%v/%v, want the CONFIGURED global 2s/40s/10m", nb.InitialDelay, nb.MaxDelay, nb.TotalTimeout)
	}
	// Full override wins outright.
	wx := per["wallex"]
	if wx.MaxAttempts != 3 || wx.InitialDelay != 500*time.Millisecond || wx.MaxDelay != 5*time.Second || wx.TotalTimeout != time.Minute {
		t.Errorf("wallex full override = %+v, want 3/500ms/5s/1m", wx)
	}
}

// TestRecoveryConfigDefaults: an omitted [execution.recovery] section takes the documented
// safe defaults (6 attempts, 1s initial, 30s cap, 5m total) with no per-exchange overrides.
func TestRecoveryConfigDefaults(t *testing.T) {
	cfg, err := loadTOML(t, "[execution]\nmode = \"off\"\n")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	g, per := cfg.Execution.ResolvedRecovery()
	if g.MaxAttempts != 6 || g.InitialDelay != time.Second || g.MaxDelay != 30*time.Second || g.TotalTimeout != 5*time.Minute {
		t.Errorf("defaults = %+v, want 6/1s/30s/5m", g)
	}
	if per != nil {
		t.Errorf("per-exchange = %v, want nil when unconfigured", per)
	}
}

// TestRecoveryConfigValidation: an invalid recovery window is a HARD STARTUP ERROR — never
// silently corrected: negatives, inverted delays, out-of-bounds values, and per-exchange
// overrides whose RESOLVED window is invalid.
func TestRecoveryConfigValidation(t *testing.T) {
	bad := map[string]string{
		"negative max_attempts":  "[execution.recovery]\nmax_attempts = -1\n",
		"max_attempts over 100":  "[execution.recovery]\nmax_attempts = 101\n",
		"negative initial delay": "[execution.recovery]\ninitial_delay_ms = -5\n",
		"max below initial":      "[execution.recovery]\ninitial_delay_ms = 10000\nmax_delay_ms = 2000\n",
		"max delay over 1h":      "[execution.recovery]\ninitial_delay_ms = 1000\nmax_delay_ms = 3600001\n",
		"negative total":         "[execution.recovery]\ntotal_timeout_ms = -1\n",
		"total over 24h":         "[execution.recovery]\ntotal_timeout_ms = 86400001\n",
		"negative per-exchange":  "[execution.recovery.per_exchange.nobitex]\nmax_attempts = -2\n",
		// Resolved per-exchange window invalid: global initial 5s, override caps max at 2s.
		"per-exchange resolved max below inherited initial": "[execution.recovery]\ninitial_delay_ms = 5000\nmax_delay_ms = 30000\n[execution.recovery.per_exchange.wallex]\nmax_delay_ms = 2000\n",
	}
	for name, body := range bad {
		if _, err := loadTOML(t, body); err == nil {
			t.Errorf("%s: Load succeeded, want a hard startup error", name)
		} else if !strings.Contains(err.Error(), "execution.recovery") {
			t.Errorf("%s: error %v should mention execution.recovery", name, err)
		}
	}
	// Sane explicit values (and a sane partial override) must pass.
	if _, err := loadTOML(t, "[execution.recovery]\nmax_attempts = 10\ninitial_delay_ms = 500\nmax_delay_ms = 60000\ntotal_timeout_ms = 900000\n[execution.recovery.per_exchange.nobitex]\ntotal_timeout_ms = 1200000\n"); err != nil {
		t.Errorf("valid recovery config rejected: %v", err)
	}
}

// TestExecutionModeValidation: exactly off/dry_run/live are accepted; empty normalizes to
// off; a typo/unknown value is a hard startup error (never silently treated as off).
func TestExecutionModeValidation(t *testing.T) {
	load := func(mode string) (*Config, error) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.toml")
		body := "[mysql]\ndsn=\"u:p@tcp(h:3306)/db\"\n"
		if mode != "<omit>" {
			body += "[execution]\nmode=\"" + mode + "\"\n"
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		return &cfg, err
	}

	// Empty / omitted → normalized to off, valid.
	for _, m := range []string{"", "<omit>"} {
		cfg, err := load(m)
		if err != nil {
			t.Errorf("mode %q should load (normalize to off), got %v", m, err)
		} else if cfg.Execution.Mode != ExecutionOff || !cfg.Execution.IsOff() {
			t.Errorf("mode %q normalized to %q, want off", m, cfg.Execution.Mode)
		}
	}
	// Valid explicit values.
	for _, m := range []string{ExecutionOff, ExecutionDryRun, ExecutionLive} {
		if _, err := load(m); err != nil {
			t.Errorf("mode %q must be valid, got %v", m, err)
		}
	}
	// Typos / unknown → validation error mentioning execution.mode.
	for _, m := range []string{"dryrun", "DRY_RUN", "simulate", "on", "Off", "unknown"} {
		_, err := load(m)
		if err == nil {
			t.Errorf("mode %q must be rejected (not silently treated as off)", m)
		} else if !strings.Contains(err.Error(), "execution.mode") {
			t.Errorf("mode %q error = %v, want it to mention execution.mode", m, err)
		}
	}
}
