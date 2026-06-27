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
}

// Validate enforces the minimum needed to operate safely. A missing DSN is a
// hard error: the system must never run without its source of truth.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.MySQL.DSN) == "" {
		return errors.New("config: mysql.dsn is required (set [mysql].dsn in the config file)")
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
