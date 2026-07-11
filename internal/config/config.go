// Package config loads BOOTSTRAP configuration only: the few values a binary
// needs before it can reach the database — MySQL/MariaDB DSN, Redis address,
// dashboard bind address, log settings, and the master encryption key.
//
// Configuration is read from a TOML FILE ONLY. This project deliberately does
// NOT read configuration from environment variables (no DSN/password/key/param
// is taken from the environment); the file is located via the binary's -config
// flag (default DefaultConfigPath). Bootstrap secrets needed before the database
// is reachable (DB password inside the DSN, Redis password, master key) live in
// the file.
//
// IMPORTANT: trading and operational configuration (enabled exchanges/symbols,
// spreads, sizes, fees, retention, regime baskets, concurrency limits, ...) is
// NOT here. Per the architecture, that lives in the database as versioned config
// (PR6), is edited from the dashboard, and is loaded into an in-memory cache by
// the trade-engine. Likewise, exchange API keys/secrets are NOT in this file —
// they are stored encrypted in the database and edited from the dashboard (PR2+).
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"

	"v3TradeBot/internal/logging"
)

// Environment names. Used for log context and (later) to gate dangerous actions.
const (
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
)

// DefaultConfigPath is where binaries look for the bootstrap config TOML when no
// -config flag value is given. The file path is the only runtime input a binary
// needs up front; everything else (including bootstrap secrets) comes from the
// file. There is intentionally no environment-variable fallback.
const DefaultConfigPath = "configs/config.toml"

// Config is the full bootstrap configuration for any binary in the fleet.
type Config struct {
	App       AppConfig       `toml:"app"`
	Log       LogConfig       `toml:"log"`
	MySQL     MySQLConfig     `toml:"mysql"`
	Redis     RedisConfig     `toml:"redis"`
	Dashboard DashboardConfig `toml:"dashboard"`
	Security  SecurityConfig  `toml:"security"`
	Execution ExecutionConfig `toml:"execution"`
}

// ExecutionConfig selects the trade-execution mode (config-driven, NEVER a runtime
// environment variable). Default (empty / "off") is the SAFE default: no real and no
// simulated orders. "dry_run" runs the full lifecycle against a simulated exchange (no
// real orders). "live" sends real orders (requires real credentials — a later PR).
type ExecutionConfig struct {
	Mode string `toml:"mode"` // "off" (default) | "dry_run" | "live"
	// Recovery bounds the order-executor's read-only recovery of AMBIGUOUS mutation timeouts
	// (an order/cancel may have succeeded at the exchange while the HTTP response timed out).
	// See PROJECT_ARCHITECTURE.md §16b. Zero fields take the documented safe defaults.
	Recovery ExecutionRecoveryConfig `toml:"recovery"`
}

// ExecutionRecoveryConfig is the RUNTIME-CONFIGURABLE ambiguous-mutation recovery window
// (PR19 round 4 correction): how many read-only probes, with what bounded exponential backoff
// (+ jitter), and for how long overall, before an unresolved order goes to NEEDS_RECONCILE
// with the symbol lock held. Durations are integer MILLISECONDS (the project's convention for
// config durations). A zero field means "unset" and takes the safe default; NEGATIVE values
// and out-of-bounds values are hard startup errors (never silently corrected).
//
// Safe defaults (applied per-field when unset): max_attempts=6, initial_delay_ms=1000 (1s),
// max_delay_ms=30000 (30s), total_timeout_ms=300000 (5m) — sized for Iranian venues with
// eventual-consistency order visibility; venues that surface orders more slowly should raise
// the per-exchange window rather than the global one.
type ExecutionRecoveryConfig struct {
	MaxAttempts    int   `toml:"max_attempts"`
	InitialDelayMs int64 `toml:"initial_delay_ms"`
	MaxDelayMs     int64 `toml:"max_delay_ms"`
	TotalTimeoutMs int64 `toml:"total_timeout_ms"`
	// PerExchange overrides the window per exchange code. A PARTIAL override inherits every
	// unset (zero) field from the CONFIGURED global values above — never from hard-coded
	// defaults — so raising e.g. only nobitex's total_timeout_ms keeps the tuned global
	// attempts/backoff.
	PerExchange map[string]ExecutionRecoveryOverride `toml:"per_exchange"`
}

// ExecutionRecoveryOverride is a per-exchange partial override of ExecutionRecoveryConfig.
// Zero fields inherit the configured global value.
type ExecutionRecoveryOverride struct {
	MaxAttempts    int   `toml:"max_attempts"`
	InitialDelayMs int64 `toml:"initial_delay_ms"`
	MaxDelayMs     int64 `toml:"max_delay_ms"`
	TotalTimeoutMs int64 `toml:"total_timeout_ms"`
}

// Documented safe defaults + hard upper safety bounds for the recovery window. The upper
// bounds keep a typo (e.g. an extra zero) from configuring a runaway probe loop or a
// day-long silent window.
const (
	recoveryDefaultMaxAttempts  = 6
	recoveryDefaultInitialMs    = 1_000   // 1s
	recoveryDefaultMaxDelayMs   = 30_000  // 30s
	recoveryDefaultTotalMs      = 300_000 // 5m
	recoveryMaxAttemptsBound    = 100
	recoveryMaxDelayBoundMs     = 3_600_000  // 1h
	recoveryTotalTimeoutBoundMs = 86_400_000 // 24h
)

// ExecutionRecoveryValues is a fully-resolved recovery window (typed durations), ready to be
// handed to the order-executor.
type ExecutionRecoveryValues struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	TotalTimeout time.Duration
}

// ResolvedRecovery returns the effective GLOBAL recovery window (defaults applied) and the
// fully-resolved per-exchange overrides: each override field that is unset (zero) inherits the
// CONFIGURED global value, not a hard-coded default. Call only on a validated config.
func (e ExecutionConfig) ResolvedRecovery() (ExecutionRecoveryValues, map[string]ExecutionRecoveryValues) {
	g := e.Recovery.globalWithDefaults()
	global := ExecutionRecoveryValues{
		MaxAttempts:  g.MaxAttempts,
		InitialDelay: time.Duration(g.InitialDelayMs) * time.Millisecond,
		MaxDelay:     time.Duration(g.MaxDelayMs) * time.Millisecond,
		TotalTimeout: time.Duration(g.TotalTimeoutMs) * time.Millisecond,
	}
	if len(e.Recovery.PerExchange) == 0 {
		return global, nil
	}
	per := make(map[string]ExecutionRecoveryValues, len(e.Recovery.PerExchange))
	for code, ov := range e.Recovery.PerExchange {
		r := ov.resolveOver(g)
		per[strings.ToLower(strings.TrimSpace(code))] = ExecutionRecoveryValues{
			MaxAttempts:  r.MaxAttempts,
			InitialDelay: time.Duration(r.InitialDelayMs) * time.Millisecond,
			MaxDelay:     time.Duration(r.MaxDelayMs) * time.Millisecond,
			TotalTimeout: time.Duration(r.TotalTimeoutMs) * time.Millisecond,
		}
	}
	return global, per
}

// globalWithDefaults fills UNSET (zero) global fields with the documented safe defaults.
// Negative values are left untouched so Validate rejects them.
func (r ExecutionRecoveryConfig) globalWithDefaults() ExecutionRecoveryConfig {
	if r.MaxAttempts == 0 {
		r.MaxAttempts = recoveryDefaultMaxAttempts
	}
	if r.InitialDelayMs == 0 {
		r.InitialDelayMs = recoveryDefaultInitialMs
	}
	if r.MaxDelayMs == 0 {
		r.MaxDelayMs = recoveryDefaultMaxDelayMs
	}
	if r.TotalTimeoutMs == 0 {
		r.TotalTimeoutMs = recoveryDefaultTotalMs
	}
	return r
}

// resolveOver merges a per-exchange partial override over the (defaults-applied) global
// config: zero fields inherit the configured global value.
func (o ExecutionRecoveryOverride) resolveOver(g ExecutionRecoveryConfig) ExecutionRecoveryOverride {
	if o.MaxAttempts == 0 {
		o.MaxAttempts = g.MaxAttempts
	}
	if o.InitialDelayMs == 0 {
		o.InitialDelayMs = g.InitialDelayMs
	}
	if o.MaxDelayMs == 0 {
		o.MaxDelayMs = g.MaxDelayMs
	}
	if o.TotalTimeoutMs == 0 {
		o.TotalTimeoutMs = g.TotalTimeoutMs
	}
	return o
}

// validateRecovery enforces the recovery window's safety envelope: every RESOLVED window
// (global and each per-exchange) must have max_attempts > 0, initial_delay > 0,
// max_delay >= initial_delay, total_timeout > 0, and stay within the hard upper bounds.
// Negative inputs are rejected explicitly (a negative is a typo, never "unset").
func validateRecovery(r ExecutionRecoveryConfig) error {
	check := func(scope string, o ExecutionRecoveryOverride) error {
		switch {
		case o.MaxAttempts <= 0:
			return fmt.Errorf("config: execution.recovery%s: max_attempts must be > 0 (got %d)", scope, o.MaxAttempts)
		case o.MaxAttempts > recoveryMaxAttemptsBound:
			return fmt.Errorf("config: execution.recovery%s: max_attempts %d exceeds the safety bound %d", scope, o.MaxAttempts, recoveryMaxAttemptsBound)
		case o.InitialDelayMs <= 0:
			return fmt.Errorf("config: execution.recovery%s: initial_delay_ms must be > 0 (got %d)", scope, o.InitialDelayMs)
		case o.MaxDelayMs < o.InitialDelayMs:
			return fmt.Errorf("config: execution.recovery%s: max_delay_ms (%d) must be >= initial_delay_ms (%d)", scope, o.MaxDelayMs, o.InitialDelayMs)
		case o.MaxDelayMs > recoveryMaxDelayBoundMs:
			return fmt.Errorf("config: execution.recovery%s: max_delay_ms %d exceeds the safety bound %d (1h)", scope, o.MaxDelayMs, recoveryMaxDelayBoundMs)
		case o.TotalTimeoutMs <= 0:
			return fmt.Errorf("config: execution.recovery%s: total_timeout_ms must be > 0 (got %d)", scope, o.TotalTimeoutMs)
		case o.TotalTimeoutMs > recoveryTotalTimeoutBoundMs:
			return fmt.Errorf("config: execution.recovery%s: total_timeout_ms %d exceeds the safety bound %d (24h)", scope, o.TotalTimeoutMs, recoveryTotalTimeoutBoundMs)
		}
		return nil
	}
	g := r.globalWithDefaults()
	if err := check("", ExecutionRecoveryOverride{
		MaxAttempts: g.MaxAttempts, InitialDelayMs: g.InitialDelayMs,
		MaxDelayMs: g.MaxDelayMs, TotalTimeoutMs: g.TotalTimeoutMs,
	}); err != nil {
		return err
	}
	for code, ov := range r.PerExchange {
		if strings.TrimSpace(code) == "" {
			return errors.New("config: execution.recovery.per_exchange: empty exchange code key")
		}
		// Reject explicit negatives in the override itself (zero = inherit is fine).
		if ov.MaxAttempts < 0 || ov.InitialDelayMs < 0 || ov.MaxDelayMs < 0 || ov.TotalTimeoutMs < 0 {
			return fmt.Errorf("config: execution.recovery.per_exchange.%s: negative values are invalid (zero = inherit the global value)", code)
		}
		if err := check(".per_exchange."+code, ov.resolveOver(g)); err != nil {
			return err
		}
	}
	return nil
}

// Execution modes.
const (
	ExecutionOff    = "off"
	ExecutionDryRun = "dry_run"
	ExecutionLive   = "live"
)

// IsDryRun reports whether dry-run mode is selected.
func (e ExecutionConfig) IsDryRun() bool { return e.Mode == ExecutionDryRun }

// IsLive reports whether live (real-order) mode is selected.
func (e ExecutionConfig) IsLive() bool { return e.Mode == ExecutionLive }

// IsOff reports whether execution is off (the safe default — no cycles, no orders).
func (e ExecutionConfig) IsOff() bool { return e.Mode == ExecutionOff }

// validExecutionMode reports whether m is one of the exactly-three accepted values.
func validExecutionMode(m string) bool {
	return m == ExecutionOff || m == ExecutionDryRun || m == ExecutionLive
}

// AppConfig holds process-wide settings.
type AppConfig struct {
	Environment string `toml:"environment"`
}

// LogConfig controls the structured logger.
type LogConfig struct {
	Level  string `toml:"level"`  // debug|info|warn|error
	Format string `toml:"format"` // json|text
}

// MySQLConfig configures the MariaDB connection pool. The DSN is the only
// required field. Pool sizes default to values tuned in the sibling system
// (see internal/db) when left at zero.
type MySQLConfig struct {
	DSN                string `toml:"dsn"`
	MaxOpenConns       int    `toml:"max_open_conns"`
	MaxIdleConns       int    `toml:"max_idle_conns"`
	ConnMaxLifetimeSec int    `toml:"conn_max_lifetime_sec"`
	ConnMaxIdleTimeSec int    `toml:"conn_max_idle_time_sec"`
}

// RedisConfig configures the Redis connection used for live market data only.
// Redis is never the source of truth (cycles/orders/locks/balances live in
// MySQL), so losing this connection or flushing Redis must not lose state.
type RedisConfig struct {
	Addr     string `toml:"addr"`
	Password string `toml:"password"`
	DB       int    `toml:"db"`
	PoolSize int    `toml:"pool_size"`
}

// DashboardConfig configures the dashboard HTTP/WebSocket server. It lives in
// its own binary so restarting it never touches the trading path.
type DashboardConfig struct {
	ListenAddr string `toml:"listen_addr"`
	// SecureCookies sets the Secure flag on the login session cookie. Enable it when the
	// dashboard is served over HTTPS (e.g. behind a TLS-terminating reverse proxy). Leave
	// false for a plain-HTTP localhost bind, or the cookie won't be sent.
	SecureCookies bool `toml:"secure_cookies"`
	// SessionTTLMinutes is the login session lifetime in minutes (default 720 = 12h).
	SessionTTLMinutes int `toml:"session_ttl_minutes"`
}

// SecurityConfig holds secrets used for at-rest protection of sensitive data
// (e.g. encrypting exchange API credentials and masking raw API logs). The
// MasterKey must never be logged.
type SecurityConfig struct {
	MasterKey string `toml:"master_key"`
}

// Load reads bootstrap config from a REQUIRED TOML file, fills defaults, and
// validates. If path is empty, DefaultConfigPath is used. There is no
// environment-variable fallback, so a missing file is a hard error rather than a
// silent all-defaults startup — the system must never come up without a
// deliberately provided config (and thus a deliberately provided DSN).
func Load(path string) (Config, error) {
	if path == "" {
		path = DefaultConfigPath
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %q: %w", path, err)
	}

	var cfg Config
	if err := toml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse %q: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyDefaults fills zero values with safe defaults. Pool sizes intentionally
// stay zero here and are defaulted in internal/db so the tuned values live next
// to the code that uses them.
func (c *Config) applyDefaults() {
	if c.App.Environment == "" {
		c.App.Environment = EnvDevelopment
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = "127.0.0.1:6379"
	}
	if c.Dashboard.ListenAddr == "" {
		c.Dashboard.ListenAddr = "127.0.0.1:8080"
	}
	// Execution mode: an EMPTY value normalizes to the safe "off". A non-empty value is
	// left as-is so Validate can reject a typo (e.g. "dryrun") rather than silently
	// treating it as off.
	if strings.TrimSpace(c.Execution.Mode) == "" {
		c.Execution.Mode = ExecutionOff
	}
}

// Validate enforces the minimum needed to operate safely. A missing DSN is a
// hard error: the system must never run without its source of truth.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.MySQL.DSN) == "" {
		return errors.New("config: mysql.dsn is required (set [mysql].dsn in the config file)")
	}
	// Execution mode must be EXACTLY one of off/dry_run/live — a typo like "dryrun" must
	// fail startup, never silently behave like "off".
	if !validExecutionMode(c.Execution.Mode) {
		return fmt.Errorf("config: execution.mode = %q is invalid (must be one of: %s, %s, %s)",
			c.Execution.Mode, ExecutionOff, ExecutionDryRun, ExecutionLive)
	}
	// The ambiguous-mutation recovery window must be sane at STARTUP (PR19 round 4): an
	// invalid window (negative, inverted delays, out of safety bounds) is a hard error,
	// never silently corrected — it governs how long a real ambiguous order stays in
	// automatic recovery before manual reconciliation.
	if err := validateRecovery(c.Execution.Recovery); err != nil {
		return err
	}
	return nil
}

// Logging converts the bootstrap LogConfig into a logging.Config.
func (c *Config) Logging() logging.Config {
	return logging.Config{Level: c.Log.Level, Format: c.Log.Format}
}

// Redacted returns a copy of the config safe to log: every secret-bearing field
// is masked. ALWAYS log Redacted(), never the raw Config — the DSN embeds the
// DB password and Security.MasterKey is a key.
func (c Config) Redacted() Config {
	c.MySQL.DSN = redactDSN(c.MySQL.DSN)
	c.Redis.Password = mask(c.Redis.Password)
	c.Security.MasterKey = mask(c.Security.MasterKey)
	return c
}

// LogValue implements slog.LogValuer (note: the interface requires the return
// type slog.Value, NOT any — getting that wrong silently logs the raw struct and
// leaks secrets). It emits an explicit group of REDACTED fields so passing a
// Config to the logger can never leak the DB password, Redis password, or master
// key. Strings are emitted (not the Config) so there is no LogValuer recursion.
func (c Config) LogValue() slog.Value {
	r := c.Redacted()
	return slog.GroupValue(
		slog.String("environment", r.App.Environment),
		slog.Group("log",
			slog.String("level", r.Log.Level),
			slog.String("format", r.Log.Format),
		),
		slog.Group("mysql",
			slog.String("dsn", r.MySQL.DSN), // password already stripped
			slog.Int("max_open_conns", r.MySQL.MaxOpenConns),
			slog.Int("max_idle_conns", r.MySQL.MaxIdleConns),
		),
		slog.Group("redis",
			slog.String("addr", r.Redis.Addr),
			slog.String("password", r.Redis.Password), // masked
			slog.Int("db", r.Redis.DB),
		),
		slog.Group("dashboard",
			slog.String("listen_addr", r.Dashboard.ListenAddr),
		),
		slog.Group("security",
			slog.String("master_key", r.Security.MasterKey), // masked
		),
	)
}

// mask hides a secret while still signalling whether one was set.
func mask(s string) string {
	if s == "" {
		return ""
	}
	return "***"
}

// redactDSN strips the password from a Go MySQL DSN of the form
// user:pass@tcp(host:port)/db?params, keeping enough to identify the target.
func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	at := strings.LastIndex(dsn, "@")
	if at < 0 {
		return dsn // no credentials section
	}
	creds := dsn[:at]
	rest := dsn[at:]
	if colon := strings.IndexByte(creds, ':'); colon >= 0 {
		creds = creds[:colon] + ":***"
	}
	return creds + rest
}
