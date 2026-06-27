// Package db is the MySQL/MariaDB access layer. MariaDB is the SYSTEM OF RECORD:
// cycles, orders, fills, symbol locks, the exchange-request queue, config
// versions and recovery state all live here. Redis is only a cache of live
// market data and must never hold authoritative state.
//
// The package intentionally exposes a thin Store wrapping *sql.DB plus a WithTx
// helper. Higher layers (state machine, queue, symbol lock) compose their
// critical writes inside a single WithTx call so that, e.g., "create cycle +
// acquire lock + enqueue buy request" is atomic — a partial write can never let
// an order be sent without its database record.
package db

import (
	"context"
	"database/sql"
	"time"

	// Registers the MariaDB/MySQL driver under the name "mysql".
	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/config"
)

// Pool defaults. Mirrors the tuning proven in the sibling system: 25 open
// connections give headroom so one slow analytical query (dashboard) cannot
// starve the trading path; 30m lifetime bounds reconnect churn; 5m idle time
// releases connections between scans. Each binary gets its own pool, so the
// server-side max_connections budget is shared across the fleet.
const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 10
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
)

// Store wraps a connection pool. It is safe for concurrent use.
type Store struct {
	db *sql.DB
}

// New opens the pool, applies tuning, and verifies connectivity with a ping so
// a misconfigured DSN fails fast at startup rather than on first query.
func New(ctx context.Context, cfg config.MySQLConfig) (*Store, error) {
	sqlDB, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(orDefaultInt(cfg.MaxOpenConns, defaultMaxOpenConns))
	sqlDB.SetMaxIdleConns(orDefaultInt(cfg.MaxIdleConns, defaultMaxIdleConns))
	sqlDB.SetConnMaxLifetime(orDefaultDur(cfg.ConnMaxLifetimeSec, defaultConnMaxLifetime))
	sqlDB.SetConnMaxIdleTime(orDefaultDur(cfg.ConnMaxIdleTimeSec, defaultConnMaxIdleTime))

	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return &Store{db: sqlDB}, nil
}

// NewFromDB wraps an already-open *sql.DB. Intended for tests (e.g. sqlmock) and
// subsystems that manage their own pool but want the Store helpers.
func NewFromDB(db *sql.DB) *Store { return &Store{db: db} }

// DB exposes the underlying pool for subsystems that need raw access (the
// migration runner pins a single connection from it; the async API logger
// shares it).
func (s *Store) DB() *sql.DB { return s.db }

// Close releases the pool.
func (s *Store) Close() error { return s.db.Close() }

// Ping verifies connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// WithTx runs fn inside a single transaction, committing on success and rolling
// back on error OR panic. Callers MUST funnel paired critical writes through one
// WithTx call to keep them atomic — this is the mechanism that upholds the hard
// rule "no order row without its cycle/lock/queue rows, all-or-nothing".
//
// On panic the transaction is rolled back and the panic re-raised, so a bug in
// fn can never leave a transaction dangling and holding row locks.
func (s *Store) WithTx(ctx context.Context, fn func(*sql.Tx) error) (retErr error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p) // preserve the original panic after cleaning up
		}
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func orDefaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func orDefaultDur(seconds int, def time.Duration) time.Duration {
	if seconds <= 0 {
		return def
	}
	return time.Duration(seconds) * time.Second
}
