// Package retention is the controlled retention worker (PR18). It deletes OLD rows
// from high-volume OPERATIONAL tables only, in bounded batches, driven entirely by DB
// config (retention_settings). It NEVER deletes permanent trading records
// (cycles/orders/fills/signals/symbol_locks/exchange_requests) — those tables are
// simply not in the retainable whitelist, so they cannot be targeted even if a
// retention_settings row names them. It makes no exchange calls and uses no Redis.
package retention

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"time"

	"v3TradeBot/internal/clock"
)

// retainable is the WHITELIST of high-volume operational tables that retention may
// delete from, mapped to their explicit timestamp column. PERMANENT trading tables are
// intentionally ABSENT (cycles/orders/fills/signals/symbol_locks/exchange_requests), so
// retention can never touch them. A timestamp column is required + whitelisted here, so
// retention never runs against a table with no clear time column.
var retainable = map[string]string{
	"api_call_logs":           "created_at",
	"comparison_events":       "created_at",
	"exchange_health_samples": "created_at",
	"app_logs":                "created_at",
	"wallet_balance_history":  "created_at",
	"market_regime_history":   "created_at",
}

// Retainable returns a copy of the whitelist (table -> timestamp column).
func Retainable() map[string]string {
	out := make(map[string]string, len(retainable))
	for k, v := range retainable {
		out[k] = v
	}
	return out
}

const advisoryLock = "v3tradebot_retention"

// Config tunes the worker.
type Config struct {
	// LockTimeoutSec is how long GET_LOCK waits before giving up (default 0 = no wait).
	LockTimeoutSec int
	// StopOnError stops the run after the first table error (default false = continue,
	// recording the error and moving on).
	StopOnError bool
}

// Worker performs retention. It holds ONLY a DB handle (+ clock/log/config) — no Redis,
// no exchange client.
type Worker struct {
	db     *sql.DB
	clk    clock.Clock
	log    *slog.Logger
	cfg    Config
	tables map[string]string // table -> timestamp column (the whitelist; overridable in tests)
}

// New builds a Worker whose targets are exactly the retainable whitelist.
func New(db *sql.DB, clk clock.Clock, log *slog.Logger, cfg Config) *Worker {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Worker{db: db, clk: clk, log: log, cfg: cfg, tables: Retainable()}
}

// TableResult is the per-table outcome of a run.
type TableResult struct {
	Table         string    `json:"table"`
	TimestampCol  string    `json:"timestamp_col"`
	Configured    bool      `json:"configured"`
	Enabled       bool      `json:"enabled"`
	RetentionDays int       `json:"retention_days"`
	Cutoff        time.Time `json:"cutoff,omitempty"`
	BatchSize     int       `json:"batch_size"`
	EstimatedRows int64     `json:"estimated_rows,omitempty"` // dry-run
	Deleted       int64     `json:"deleted"`
	Batches       int       `json:"batches"`
	Skipped       string    `json:"skipped,omitempty"` // reason, when nothing was done
	Err           string    `json:"error,omitempty"`
}

// Report is the full outcome of one run.
type Report struct {
	StartedAt    time.Time     `json:"started_at"`
	FinishedAt   time.Time     `json:"finished_at"`
	DurationMs   int64         `json:"duration_ms"`
	DryRun       bool          `json:"dry_run"`
	LockAcquired bool          `json:"lock_acquired"`
	Tables       []TableResult `json:"tables"`
}

type tableSetting struct {
	enabled    bool
	days       int
	batchSize  int
	maxBatches int
	pauseMs    int
}

// RunOnce performs one retention pass under a DB advisory lock. If the lock cannot be
// acquired (another worker is running) it returns cleanly with LockAcquired=false and
// no deletes. dryRun reports the cutoff + estimated rows WITHOUT deleting anything.
func (w *Worker) RunOnce(ctx context.Context, dryRun bool) (Report, error) {
	start := w.clk.Now().UTC()
	rep := Report{StartedAt: start, DryRun: dryRun}

	// Pin a connection for the advisory lock; hold it for the whole run.
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return rep, err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", advisoryLock, w.cfg.LockTimeoutSec).Scan(&got); err != nil {
		return rep, err
	}
	if !got.Valid || got.Int64 != 1 {
		// Another retention-worker holds the lock: exit cleanly, run nothing.
		w.log.Info("retention: advisory lock not acquired; another run is active")
		rep.FinishedAt = w.clk.Now().UTC()
		return rep, nil
	}
	rep.LockAcquired = true
	defer conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", advisoryLock)

	settings, err := w.loadSettings(ctx)
	if err != nil {
		return rep, err
	}

	for _, table := range sortedKeys(w.tables) {
		if ctx.Err() != nil {
			break
		}
		tscol := w.tables[table]
		res := TableResult{Table: table, TimestampCol: tscol}
		cfg, ok := settings[table]
		// Missing config or non-positive retention_days => not configured: do nothing
		// (never guess a deletion window).
		if !ok || cfg.days <= 0 {
			res.Skipped = "not configured"
			rep.Tables = append(rep.Tables, res)
			continue
		}
		res.Configured, res.Enabled, res.RetentionDays, res.BatchSize = true, cfg.enabled, cfg.days, cfg.batchSize
		if !cfg.enabled {
			res.Skipped = "disabled"
			rep.Tables = append(rep.Tables, res)
			continue
		}
		res.Cutoff = start.Add(-time.Duration(cfg.days) * 24 * time.Hour)

		if dryRun {
			var n int64
			if err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE "+tscol+" < ?", res.Cutoff).Scan(&n); err != nil {
				res.Err = err.Error()
			}
			res.EstimatedRows = n
			rep.Tables = append(rep.Tables, res)
			continue
		}

		deleted, batches, derr := w.batchDelete(ctx, table, tscol, res.Cutoff, cfg)
		res.Deleted, res.Batches = deleted, batches
		if derr != nil && !errors.Is(derr, context.Canceled) {
			res.Err = derr.Error()
		}
		rep.Tables = append(rep.Tables, res)
		if res.Err != "" && w.cfg.StopOnError {
			break
		}
	}

	rep.FinishedAt = w.clk.Now().UTC()
	rep.DurationMs = rep.FinishedAt.Sub(rep.StartedAt).Milliseconds()
	w.recordRun(ctx, rep)
	return rep, nil
}

// batchDelete deletes rows older than cutoff in bounded batches (never one huge
// delete), stopping at max_batches_per_run, when a batch is short (no more old rows),
// on error, or on context cancellation.
func (w *Worker) batchDelete(ctx context.Context, table, tscol string, cutoff time.Time, cfg tableSetting) (int64, int, error) {
	var deleted int64
	batches := 0
	for batches < cfg.maxBatches {
		if err := ctx.Err(); err != nil {
			return deleted, batches, err
		}
		res, err := w.db.ExecContext(ctx, "DELETE FROM "+table+" WHERE "+tscol+" < ? LIMIT ?", cutoff, cfg.batchSize)
		if err != nil {
			return deleted, batches, err
		}
		n, _ := res.RowsAffected()
		deleted += n
		batches++
		if n < int64(cfg.batchSize) {
			break // fewer than a full batch => no more old rows
		}
		if cfg.pauseMs > 0 {
			select {
			case <-ctx.Done():
				return deleted, batches, ctx.Err()
			case <-time.After(time.Duration(cfg.pauseMs) * time.Millisecond):
			}
		}
	}
	return deleted, batches, nil
}

// loadSettings reads retention_settings for the whitelisted tables.
func (w *Worker) loadSettings(ctx context.Context) (map[string]tableSetting, error) {
	rows, err := w.db.QueryContext(ctx,
		"SELECT table_name, enabled, retention_days, batch_size, max_batches_per_run, pause_ms FROM retention_settings")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]tableSetting{}
	for rows.Next() {
		var name string
		var enabled int
		var days, batch, maxB, pause sql.NullInt64
		if err := rows.Scan(&name, &enabled, &days, &batch, &maxB, &pause); err != nil {
			return nil, err
		}
		out[name] = tableSetting{
			enabled:    enabled != 0,
			days:       int(days.Int64),
			batchSize:  intOr(batch, 1000),
			maxBatches: intOr(maxB, 100),
			pauseMs:    int(pause.Int64),
		}
	}
	return out, rows.Err()
}

// recordRun writes the run summary to app_logs (no secrets). Best-effort.
func (w *Worker) recordRun(ctx context.Context, rep Report) {
	fields, _ := json.Marshal(rep)
	if _, err := w.db.ExecContext(ctx,
		"INSERT INTO app_logs (level, source_binary, message, fields) VALUES ('info', 'retention-worker', ?, ?)",
		summaryMessage(rep), string(fields)); err != nil {
		w.log.Warn("retention: failed to record run to app_logs", "err", err)
	}
	w.log.Info("retention run complete", "dry_run", rep.DryRun, "lock", rep.LockAcquired, "tables", len(rep.Tables), "duration_ms", rep.DurationMs)
}

func summaryMessage(rep Report) string {
	if !rep.LockAcquired {
		return "retention skipped (lock not acquired)"
	}
	if rep.DryRun {
		return "retention dry-run complete"
	}
	return "retention run complete"
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func intOr(v sql.NullInt64, def int) int {
	if !v.Valid || v.Int64 <= 0 {
		return def
	}
	return int(v.Int64)
}
