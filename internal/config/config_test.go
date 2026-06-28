package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
