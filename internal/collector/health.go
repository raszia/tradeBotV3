package collector

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// DBHealthRecorder records per-exchange market-data health into the PR2 health
// tables: it upserts exchange_health_current and appends exchange_health_samples.
// The collector holds exchange CODES, but the tables key on exchange_id, so this
// resolves and caches code -> id. A code with no exchanges row is skipped (health
// is best-effort and must never block ingestion).
type DBHealthRecorder struct {
	db *sql.DB

	mu  sync.Mutex
	ids map[string]int64 // exchange code -> id (cached)
}

// NewDBHealthRecorder builds a recorder backed by db.
func NewDBHealthRecorder(db *sql.DB) *DBHealthRecorder {
	return &DBHealthRecorder{db: db, ids: map[string]int64{}}
}

// resolveID returns the exchange_id for a code, caching it. ok=false if unknown.
func (r *DBHealthRecorder) resolveID(ctx context.Context, code string) (int64, bool) {
	r.mu.Lock()
	if id, ok := r.ids[code]; ok {
		r.mu.Unlock()
		return id, true
	}
	r.mu.Unlock()

	var id int64
	err := r.db.QueryRowContext(ctx, "SELECT id FROM exchanges WHERE code = ?", code).Scan(&id)
	if err != nil {
		return 0, false
	}
	r.mu.Lock()
	r.ids[code] = id
	r.mu.Unlock()
	return id, true
}

// RecordSuccess marks the exchange REST-healthy and appends a sample.
func (r *DBHealthRecorder) RecordSuccess(ctx context.Context, exchange string, latency time.Duration) {
	id, ok := r.resolveID(ctx, exchange)
	if !ok {
		return
	}
	latencyMs := latency.Milliseconds()
	_, _ = r.db.ExecContext(ctx, `
		INSERT INTO exchange_health_current (exchange_id, rest_status, latency_ms, last_success_at, updated_at)
		VALUES (?, 'up', ?, NOW(6), NOW(6))
		ON DUPLICATE KEY UPDATE rest_status='up', latency_ms=VALUES(latency_ms), last_success_at=NOW(6), updated_at=NOW(6)`,
		id, latencyMs)
	_, _ = r.db.ExecContext(ctx,
		"INSERT INTO exchange_health_samples (exchange_id, sample_type, ok, latency_ms) VALUES (?, 'rest', 1, ?)",
		id, latencyMs)
}

// RecordFailure marks the exchange REST-down, bumps error_count, and appends a
// sample carrying the (already-safe) error string.
func (r *DBHealthRecorder) RecordFailure(ctx context.Context, exchange string, cause error) {
	id, ok := r.resolveID(ctx, exchange)
	if !ok {
		return
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	_, _ = r.db.ExecContext(ctx, `
		INSERT INTO exchange_health_current (exchange_id, rest_status, error_count, last_failure_at, updated_at)
		VALUES (?, 'down', 1, NOW(6), NOW(6))
		ON DUPLICATE KEY UPDATE rest_status='down', error_count=error_count+1, last_failure_at=NOW(6), updated_at=NOW(6)`,
		id)
	_, _ = r.db.ExecContext(ctx,
		"INSERT INTO exchange_health_samples (exchange_id, sample_type, ok, error) VALUES (?, 'rest', 0, ?)",
		id, sqlNullStr(msg))
}

func sqlNullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
