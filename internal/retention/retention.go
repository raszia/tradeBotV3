// Package retention is the controlled retention worker (PR18). It deletes OLD rows
// from high-volume OPERATIONAL tables only, in bounded batches, driven entirely by DB
// config (retention_settings). It NEVER deletes permanent trading records
// (cycles/orders/fills/signals/symbol_locks/exchange_requests) — those tables are
// simply not in the retainable whitelist, so they cannot be targeted even if a
// retention_settings row names them. It makes no exchange calls and uses no Redis.
//
// One run holds a MariaDB advisory lock on a PINNED *sql.Conn and does ALL of its work
// (load settings, count, batch-DELETE, record report) on that SAME connection, so the
// lock and the deletes can never end up on different pooled connections — safe even with
// SetMaxOpenConns(1), and the lock ownership stays tied to the deleting connection.
package retention

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"v3TradeBot/internal/clock"
)

// dbExecutor is the read/write surface retention uses. Both *sql.DB and *sql.Conn satisfy
// it; production always passes the pinned *sql.Conn that owns the advisory lock.
type dbExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

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

// Documented SAFE BOUNDS for every retention setting. A DB value outside its range is a
// per-table validation error → that table is skipped (no DELETE), never silently defaulted.
// (Migration 029 mirrors these as CHECK constraints; runtime validation is the primary
// guard and also covers rows that predate the constraints.)
const (
	minRetentionDays = 1
	maxRetentionDays = 3650 // 10 years

	minBatchSize = 1
	maxBatchSize = 50000 // a bounded single DELETE ... LIMIT

	minMaxBatches = 1
	maxMaxBatches = 10000 // per-run work ceiling

	minPauseMs = 0
	maxPauseMs = 60000 // 60s between batches
)

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
	StartedAt     time.Time     `json:"started_at"`
	FinishedAt    time.Time     `json:"finished_at"`
	DurationMs    int64         `json:"duration_ms"`
	DryRun        bool          `json:"dry_run"`
	LockAcquired  bool          `json:"lock_acquired"`
	SkippedReason string        `json:"skipped_reason,omitempty"` // set when LockAcquired=false
	Tables        []TableResult `json:"tables"`
}

// HasErrors reports whether any table in the run recorded an error.
func (r Report) HasErrors() bool {
	for _, t := range r.Tables {
		if t.Err != "" {
			return true
		}
	}
	return false
}

type tableSetting struct {
	enabled    bool
	daysSet    bool // whether retention_days is non-NULL
	days       int
	batchSize  int
	maxBatches int
	pauseMs    int
}

// validate enforces the documented safe bounds. Returns a descriptive error when any value
// is out of range (the table is then skipped with that error — never silently defaulted).
func (s tableSetting) validate() error {
	if s.days < minRetentionDays || s.days > maxRetentionDays {
		return fmt.Errorf("retention_days=%d out of range [%d,%d]", s.days, minRetentionDays, maxRetentionDays)
	}
	if s.batchSize < minBatchSize || s.batchSize > maxBatchSize {
		return fmt.Errorf("batch_size=%d out of range [%d,%d]", s.batchSize, minBatchSize, maxBatchSize)
	}
	if s.maxBatches < minMaxBatches || s.maxBatches > maxMaxBatches {
		return fmt.Errorf("max_batches_per_run=%d out of range [%d,%d]", s.maxBatches, minMaxBatches, maxMaxBatches)
	}
	if s.pauseMs < minPauseMs || s.pauseMs > maxPauseMs {
		return fmt.Errorf("pause_ms=%d out of range [%d,%d]", s.pauseMs, minPauseMs, maxPauseMs)
	}
	return nil
}

// RunOnce performs one retention pass under a DB advisory lock held on a PINNED connection;
// all of its work runs on that same connection. If the lock cannot be acquired (another
// worker is running) it returns cleanly with LockAcquired=false, a SkippedReason, and a
// recorded (logged) skipped report — no deletes. dryRun reports the cutoff + estimated rows
// WITHOUT deleting anything.
func (w *Worker) RunOnce(ctx context.Context, dryRun bool) (Report, error) {
	start := w.clk.Now().UTC()
	rep := Report{StartedAt: start, DryRun: dryRun}

	// Pin ONE connection for the whole run (lock + every query/DELETE + the report insert).
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
		// Another retention-worker holds the lock: run nothing, but STILL produce and log a
		// completed skipped report so operators can tell "skipped because busy" from "did not run".
		rep.LockAcquired = false
		rep.SkippedReason = "another retention run is active"
		w.finish(ctx, conn, &rep, start)
		w.log.Info("retention: advisory lock not acquired; another run is active")
		return rep, nil
	}
	rep.LockAcquired = true
	// Release the lock on the SAME pinned connection BEFORE it closes (defer LIFO: release
	// runs before conn.Close). Uses a background context so it releases even on cancellation.
	defer conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", advisoryLock)

	settings, err := w.loadSettings(ctx, conn)
	if err != nil {
		return rep, err // deferred RELEASE_LOCK + Close still run
	}

	for _, table := range sortedKeys(w.tables) {
		if ctx.Err() != nil {
			break
		}
		tscol := w.tables[table]
		res := TableResult{Table: table, TimestampCol: tscol}
		cfg, ok := settings[table]
		// Missing config, NULL days, or days < 1 => no retention window: do nothing
		// (never guess a deletion window).
		if !ok || !cfg.daysSet || cfg.days < minRetentionDays {
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
		// Validate the safe bounds. An invalid setting deletes NOTHING for this table,
		// records a clear error, and does not stop the other tables.
		if verr := cfg.validate(); verr != nil {
			res.Err = "invalid retention config: " + verr.Error()
			rep.Tables = append(rep.Tables, res)
			if w.cfg.StopOnError {
				break
			}
			continue
		}
		// Overflow-safe cutoff (calendar subtraction; never Duration arithmetic that can
		// wrap for a large day count and move the cutoff into the future).
		res.Cutoff = start.AddDate(0, 0, -cfg.days)

		if dryRun {
			var n int64
			if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE "+tscol+" < ?", res.Cutoff).Scan(&n); err != nil {
				res.Err = err.Error()
			}
			res.EstimatedRows = n
			rep.Tables = append(rep.Tables, res)
			continue
		}

		deleted, batches, derr := w.batchDelete(ctx, conn, table, tscol, res.Cutoff, cfg)
		res.Deleted, res.Batches = deleted, batches
		if derr != nil && !errors.Is(derr, context.Canceled) {
			res.Err = derr.Error()
		}
		rep.Tables = append(rep.Tables, res)
		if res.Err != "" && w.cfg.StopOnError {
			break
		}
	}

	w.finish(ctx, conn, &rep, start)
	return rep, nil
}

// finish stamps the timing and records the run to app_logs (via the pinned conn).
func (w *Worker) finish(ctx context.Context, exec dbExecutor, rep *Report, start time.Time) {
	rep.FinishedAt = w.clk.Now().UTC()
	rep.DurationMs = rep.FinishedAt.Sub(start).Milliseconds()
	w.recordRun(ctx, exec, *rep)
}

// batchDelete deletes rows older than cutoff in bounded batches (never one huge delete),
// stopping at max_batches_per_run, when a batch is short (no more old rows), on error, or
// on context cancellation. Runs on the passed executor (the pinned conn).
func (w *Worker) batchDelete(ctx context.Context, exec dbExecutor, table, tscol string, cutoff time.Time, cfg tableSetting) (int64, int, error) {
	var deleted int64
	batches := 0
	for batches < cfg.maxBatches {
		if err := ctx.Err(); err != nil {
			return deleted, batches, err
		}
		res, err := exec.ExecContext(ctx, "DELETE FROM "+table+" WHERE "+tscol+" < ? LIMIT ?", cutoff, cfg.batchSize)
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

// loadSettings reads retention_settings for the whitelisted tables. It loads RAW values
// (no defaulting) so out-of-bounds values surface to validation rather than being silently
// replaced.
func (w *Worker) loadSettings(ctx context.Context, exec dbExecutor) (map[string]tableSetting, error) {
	rows, err := exec.QueryContext(ctx,
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
			daysSet:    days.Valid,
			days:       int(days.Int64),
			batchSize:  int(batch.Int64),
			maxBatches: int(maxB.Int64),
			pauseMs:    int(pause.Int64),
		}
	}
	return out, rows.Err()
}

// recordRun writes the run summary to app_logs (no secrets). Best-effort. The log LEVEL is
// warn when any table errored (so a partial failure is not reported as a clean success),
// else info.
func (w *Worker) recordRun(ctx context.Context, exec dbExecutor, rep Report) {
	fields, _ := json.Marshal(rep)
	level := "info"
	if rep.HasErrors() {
		level = "warn"
	}
	if _, err := exec.ExecContext(ctx,
		"INSERT INTO app_logs (level, source_binary, message, fields) VALUES (?, 'retention-worker', ?, ?)",
		level, summaryMessage(rep), string(fields)); err != nil {
		w.log.Warn("retention: failed to record run to app_logs", "err", err)
	}
	if rep.HasErrors() {
		w.log.Warn("retention run completed WITH table errors", "dry_run", rep.DryRun, "lock", rep.LockAcquired, "tables", len(rep.Tables), "duration_ms", rep.DurationMs)
	} else {
		w.log.Info("retention run complete", "dry_run", rep.DryRun, "lock", rep.LockAcquired, "tables", len(rep.Tables), "duration_ms", rep.DurationMs)
	}
}

func summaryMessage(rep Report) string {
	if !rep.LockAcquired {
		return "retention skipped: " + rep.SkippedReason
	}
	errs := 0
	for _, t := range rep.Tables {
		if t.Err != "" {
			errs++
		}
	}
	if rep.DryRun {
		return "retention dry-run complete"
	}
	if errs > 0 {
		return fmt.Sprintf("retention run completed with %d table error(s)", errs)
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
