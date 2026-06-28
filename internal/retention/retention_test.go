package retention

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/migrate"
)

// ---- offline ----

// TestWhitelistExcludesPermanentTables: retention can ONLY target high-volume
// operational tables; permanent trading tables must never be in the whitelist.
func TestWhitelistExcludesPermanentTables(t *testing.T) {
	w := Retainable()
	for _, permanent := range []string{"cycles", "orders", "fills", "signals", "symbol_locks", "exchange_requests"} {
		if _, ok := w[permanent]; ok {
			t.Errorf("permanent table %q must NOT be retainable", permanent)
		}
	}
	for _, hv := range []string{"api_call_logs", "comparison_events", "exchange_health_samples", "app_logs", "wallet_balance_history", "market_regime_history"} {
		if col, ok := w[hv]; !ok || col == "" {
			t.Errorf("high-volume table %q must be retainable with a timestamp column", hv)
		}
	}
}

// ---- gated ----

var rseq int

type rfix struct {
	t   *testing.T
	ctx context.Context
	db  *sql.DB
	w   *Worker
}

func setupR(t *testing.T) *rfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the retention integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return &rfix{t: t, ctx: ctx, db: db, w: New(db, clock.NewSystem(), nil, Config{})}
}

func (f *rfix) exec(q string, a ...any) {
	if _, err := f.db.Exec(q, a...); err != nil {
		f.t.Fatalf("exec %q: %v", q, err)
	}
}

// tag uniquely identifies a test's seeded app_logs so the count is isolated from the
// shared DB (and from the worker's own run-summary row).
func (f *rfix) seedAppLogs(tag string, ageDays int, n int) {
	for i := 0; i < n; i++ {
		f.exec("INSERT INTO app_logs (level, source_binary, message, created_at) VALUES ('info', 'test', ?, NOW(6) - INTERVAL ? DAY)", tag, ageDays)
	}
}

func (f *rfix) countAppLogs(tag string) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM app_logs WHERE message=?", tag).Scan(&n)
	return n
}

func (f *rfix) setSetting(table string, enabled, days, batch, maxB int) {
	rseq++
	f.exec("DELETE FROM retention_settings WHERE table_name=?", table)
	f.exec("INSERT INTO retention_settings (table_name, enabled, retention_days, batch_size, max_batches_per_run, pause_ms) VALUES (?, ?, ?, ?, ?, 0)",
		table, enabled, days, batch, maxB)
}

func (f *rfix) tableResult(rep Report, table string) (TableResult, bool) {
	for _, r := range rep.Tables {
		if r.Table == table {
			return r, true
		}
	}
	return TableResult{}, false
}

func TestMissingConfigDeletesNothing(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_MISS_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 3)
	// No retention_settings row for app_logs.
	f.exec("DELETE FROM retention_settings WHERE table_name='app_logs'")
	rep, err := f.w.RunOnce(f.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if f.countAppLogs(tag) != 3 {
		t.Errorf("unconfigured table must not be touched, %d remain of 3", f.countAppLogs(tag))
	}
	if r, _ := f.tableResult(rep, "app_logs"); r.Skipped != "not configured" {
		t.Errorf("app_logs skipped reason = %q, want 'not configured'", r.Skipped)
	}
}

func TestDisabledTableDeletesNothing(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_DIS_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 3)
	f.setSetting("app_logs", 0, 30, 1000, 100) // disabled
	if _, err := f.w.RunOnce(f.ctx, false); err != nil {
		t.Fatal(err)
	}
	if f.countAppLogs(tag) != 3 {
		t.Errorf("disabled table must not be touched, %d remain", f.countAppLogs(tag))
	}
}

func TestDryRunDeletesNothingReportsCutoff(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_DRY_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 4)
	f.setSetting("app_logs", 1, 30, 1000, 100)
	rep, err := f.w.RunOnce(f.ctx, true) // dry run
	if err != nil {
		t.Fatal(err)
	}
	if f.countAppLogs(tag) != 4 {
		t.Errorf("dry-run must delete nothing, %d remain of 4", f.countAppLogs(tag))
	}
	r, _ := f.tableResult(rep, "app_logs")
	if r.EstimatedRows < 4 {
		t.Errorf("dry-run estimated %d rows, want >= 4", r.EstimatedRows)
	}
	wantCutoff := time.Now().Add(-30 * 24 * time.Hour)
	if r.Cutoff.After(time.Now()) || r.Cutoff.Before(wantCutoff.Add(-time.Hour)) || r.Cutoff.After(wantCutoff.Add(time.Hour)) {
		t.Errorf("dry-run cutoff = %v, want ~%v", r.Cutoff, wantCutoff)
	}
}

func TestBatchDeleteOnlyOldRowsPreservesRecent(t *testing.T) {
	f := setupR(t)
	old := fmt.Sprintf("RET_OLD_%d", time.Now().UnixNano())
	recent := fmt.Sprintf("RET_NEW_%d", time.Now().UnixNano())
	f.seedAppLogs(old, 100, 5)  // older than 30d
	f.seedAppLogs(recent, 1, 5) // within 30d
	f.setSetting("app_logs", 1, 30, 1000, 100)
	rep, err := f.w.RunOnce(f.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if f.countAppLogs(old) != 0 {
		t.Errorf("old rows should be deleted, %d remain", f.countAppLogs(old))
	}
	if f.countAppLogs(recent) != 5 {
		t.Errorf("recent rows must be preserved, %d remain of 5", f.countAppLogs(recent))
	}
	if r, _ := f.tableResult(rep, "app_logs"); r.Deleted < 5 {
		t.Errorf("reported deleted = %d, want >= 5", r.Deleted)
	}
}

func TestBatchSizeAndMaxBatchesHonored(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_BATCH_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 25)
	f.setSetting("app_logs", 1, 30, 10, 2) // batch 10, max 2 batches => delete at most 20
	rep, err := f.w.RunOnce(f.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.countAppLogs(tag); got != 5 {
		t.Errorf("after 2 batches of 10, %d remain, want 5", got)
	}
	r, _ := f.tableResult(rep, "app_logs")
	if r.Deleted != 20 || r.Batches != 2 {
		t.Errorf("deleted=%d batches=%d, want 20/2", r.Deleted, r.Batches)
	}
}

func TestPermanentTableNeverTargeted(t *testing.T) {
	f := setupR(t)
	// Even with a retention_settings row naming a permanent table, it is never touched.
	f.setSetting("cycles", 1, 1, 1000, 100)
	rep, err := f.w.RunOnce(f.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.tableResult(rep, "cycles"); ok {
		t.Error("permanent table 'cycles' must never appear in a retention run")
	}
}

func TestOneTableFailureRecordedAndContinues(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_FAIL_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 3)
	f.setSetting("app_logs", 1, 30, 1000, 100)
	f.setSetting("no_such_table", 1, 30, 1000, 100)
	// Inject a bogus table (test seam) alongside a real one.
	f.w.tables = map[string]string{"app_logs": "created_at", "no_such_table": "created_at"}

	rep, err := f.w.RunOnce(f.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	bad, _ := f.tableResult(rep, "no_such_table")
	if bad.Err == "" {
		t.Error("a failing table must record its error")
	}
	// The real table was still processed despite the other's failure.
	if f.countAppLogs(tag) != 0 {
		t.Errorf("real table should still be cleaned despite another's failure, %d remain", f.countAppLogs(tag))
	}
}

func TestAdvisoryLockPreventsConcurrentRuns(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_LOCK_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 3)
	f.setSetting("app_logs", 1, 30, 1000, 100)

	// Hold the advisory lock on a separate connection.
	conn, err := f.db.Conn(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var got int
	conn.QueryRowContext(f.ctx, "SELECT GET_LOCK(?, 0)", advisoryLock).Scan(&got)
	if got != 1 {
		t.Fatalf("could not pre-acquire the lock (got %d)", got)
	}
	defer conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", advisoryLock)

	rep, err := f.w.RunOnce(f.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.LockAcquired {
		t.Error("a second run must NOT acquire the lock")
	}
	if f.countAppLogs(tag) != 3 {
		t.Errorf("a lock-blocked run must delete nothing, %d remain of 3", f.countAppLogs(tag))
	}
}

func TestRunRecordedToAppLog(t *testing.T) {
	f := setupR(t)
	f.setSetting("app_logs", 1, 30, 1000, 100)
	before := f.retentionLogCount()
	if _, err := f.w.RunOnce(f.ctx, false); err != nil {
		t.Fatal(err)
	}
	if f.retentionLogCount() <= before {
		t.Error("retention run should be recorded to app_logs")
	}
}

func (f *rfix) retentionLogCount() int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM app_logs WHERE source_binary='retention-worker'").Scan(&n)
	return n
}

func TestContextCancelStopsClean(t *testing.T) {
	f := setupR(t)
	f.setSetting("app_logs", 1, 30, 1000, 100)
	ctx, cancel := context.WithCancel(f.ctx)
	cancel() // already cancelled
	// Must not panic; returns cleanly (an error from the cancelled conn is acceptable).
	_, _ = f.w.RunOnce(ctx, false)
}
