package retention

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
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
	dsn string
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
	return &rfix{t: t, ctx: ctx, dsn: dsn, db: db, w: New(db, clock.NewSystem(), nil, Config{})}
}

// newDB opens a fresh *sql.DB handle (its own pool) — used to give concurrent workers
// independent pools and to exercise SetMaxOpenConns.
func (f *rfix) newDB() *sql.DB {
	db, err := sql.Open("mysql", f.dsn)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { db.Close() })
	return db
}

// setSettingFull seeds a retention_settings row with all knobs (including pause_ms).
func (f *rfix) setSettingFull(table string, enabled, days, batch, maxB, pause int) {
	f.exec("DELETE FROM retention_settings WHERE table_name=?", table)
	f.exec("INSERT INTO retention_settings (table_name, enabled, retention_days, batch_size, max_batches_per_run, pause_ms) VALUES (?, ?, ?, ?, ?, ?)",
		table, enabled, days, batch, maxB, pause)
}

func (f *rfix) lockIsFree() bool {
	conn, err := f.db.Conn(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	defer conn.Close()
	var got int
	conn.QueryRowContext(f.ctx, "SELECT GET_LOCK(?, 0)", advisoryLock).Scan(&got)
	if got == 1 {
		conn.ExecContext(f.ctx, "SELECT RELEASE_LOCK(?)", advisoryLock)
		return true
	}
	return false
}

// lastRetentionLog returns the level + message of the most recent retention-worker app_log.
func (f *rfix) lastRetentionLog() (level, message string) {
	f.db.QueryRow("SELECT level, message FROM app_logs WHERE source_binary='retention-worker' ORDER BY id DESC LIMIT 1").Scan(&level, &message)
	return
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

// ---- PR18 correction #3: bounds validation (offline) ----

func TestTableSettingValidate(t *testing.T) {
	valid := tableSetting{enabled: true, daysSet: true, days: 30, batchSize: 1000, maxBatches: 100, pauseMs: 0}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid setting rejected: %v", err)
	}
	// Boundary values that must be ACCEPTED.
	for _, s := range []tableSetting{
		{days: minRetentionDays, batchSize: minBatchSize, maxBatches: minMaxBatches, pauseMs: minPauseMs},
		{days: maxRetentionDays, batchSize: maxBatchSize, maxBatches: maxMaxBatches, pauseMs: maxPauseMs},
	} {
		if err := s.validate(); err != nil {
			t.Errorf("boundary setting %+v rejected: %v", s, err)
		}
	}
	// Out-of-range values that must be REJECTED.
	bad := map[string]tableSetting{
		"days huge":     {days: 100000, batchSize: 1000, maxBatches: 100},
		"days zero":     {days: 0, batchSize: 1000, maxBatches: 100},
		"batch zero":    {days: 30, batchSize: 0, maxBatches: 100},
		"batch neg":     {days: 30, batchSize: -1, maxBatches: 100},
		"batch huge":    {days: 30, batchSize: 2147483647, maxBatches: 100},
		"maxbatch zero": {days: 30, batchSize: 1000, maxBatches: 0},
		"maxbatch neg":  {days: 30, batchSize: 1000, maxBatches: -5},
		"maxbatch huge": {days: 30, batchSize: 1000, maxBatches: 2000000},
		"pause neg":     {days: 30, batchSize: 1000, maxBatches: 100, pauseMs: -1},
		"pause huge":    {days: 30, batchSize: 1000, maxBatches: 100, pauseMs: 3600000},
	}
	for name, s := range bad {
		if err := s.validate(); err == nil {
			t.Errorf("%s (%+v) must be rejected", name, s)
		}
	}
}

// ---- PR18 correction #3: invalid setting → no deletion, others continue (gated) ----

// TestInvalidSettingSkipsTableContinuesOthers seeds an out-of-range retention_days on one
// table (bypassing the CHECK for this test) alongside a valid table, and asserts the invalid
// table deletes nothing + records a validation error while the valid table is still cleaned.
func TestInvalidSettingSkipsTableContinuesOthers(t *testing.T) {
	f := setupR(t)
	bad := fmt.Sprintf("RET_BAD_%d", time.Now().UnixNano())
	good := fmt.Sprintf("RET_GOOD_%d", time.Now().UnixNano())
	// api_call_logs is our "bad-config" target; app_logs is the "good" one.
	f.exec("INSERT INTO api_call_logs (method, url, response_status, created_at) VALUES ('GET', ?, 200, NOW(6) - INTERVAL 100 DAY)", bad)
	f.seedAppLogs(good, 100, 3)

	// Drop the days CHECK to inject an out-of-range value, then restore it.
	if _, err := f.db.Exec("ALTER TABLE retention_settings DROP CONSTRAINT chk_ret_days"); err != nil {
		t.Skipf("cannot drop chk_ret_days to inject an invalid row: %v", err)
	}
	t.Cleanup(func() {
		f.db.Exec("DELETE FROM retention_settings WHERE table_name='api_call_logs' AND retention_days > 3650")
		f.db.Exec("ALTER TABLE retention_settings ADD CONSTRAINT chk_ret_days CHECK (retention_days IS NULL OR (retention_days >= 1 AND retention_days <= 3650))")
	})
	f.setSetting("api_call_logs", 1, 100000, 1000, 100) // out-of-range days
	f.setSetting("app_logs", 1, 30, 1000, 100)          // valid

	rep, err := f.w.RunOnce(f.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	badRes, _ := f.tableResult(rep, "api_call_logs")
	if badRes.Err == "" || badRes.Deleted != 0 {
		t.Errorf("invalid-config table: err=%q deleted=%d, want a validation error + 0 deleted", badRes.Err, badRes.Deleted)
	}
	var badRemain int
	f.db.QueryRow("SELECT COUNT(*) FROM api_call_logs WHERE url=?", bad).Scan(&badRemain)
	if badRemain != 1 {
		t.Errorf("invalid-config table must not be deleted, %d remain of 1", badRemain)
	}
	if f.countAppLogs(good) != 0 {
		t.Errorf("valid table must still be cleaned despite the invalid one, %d remain", f.countAppLogs(good))
	}
	if lvl, _ := f.lastRetentionLog(); lvl != "warn" {
		t.Errorf("a run with a table error should log at warn, got %q", lvl)
	}
}

// TestRetentionBoundsRejectedAtDB proves the migration-029 CHECK constraints reject
// out-of-range settings at insert time (defence in depth).
func TestRetentionBoundsRejectedAtDB(t *testing.T) {
	f := setupR(t)
	rejects := func(name string, days, batch, maxB, pause int) {
		f.db.Exec("DELETE FROM retention_settings WHERE table_name='app_logs'")
		_, err := f.db.Exec("INSERT INTO retention_settings (table_name, enabled, retention_days, batch_size, max_batches_per_run, pause_ms) VALUES ('app_logs', 1, ?, ?, ?, ?)",
			days, batch, maxB, pause)
		if err == nil {
			t.Errorf("%s: DB should reject the out-of-range insert", name)
		}
	}
	rejects("days huge", 100000, 1000, 100, 0)
	rejects("batch zero", 30, 0, 100, 0)
	rejects("batch huge", 30, 60000, 100, 0)
	rejects("maxbatch zero", 30, 1000, 0, 0)
	rejects("maxbatch huge", 30, 1000, 20000, 0)
	rejects("pause neg", 30, 1000, 100, -1)
	rejects("pause huge", 30, 1000, 100, 120000)
	f.db.Exec("DELETE FROM retention_settings WHERE table_name='app_logs'")
}

// ---- PR18 correction #2: pinned connection ----

// TestRunOnceMaxOpenConns1 proves the whole run (lock + queries + DELETE + report) happens
// on ONE pinned connection: it must complete with SetMaxOpenConns(1) and not hang.
func TestRunOnceMaxOpenConns1(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_ONE_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 5)
	f.setSetting("app_logs", 1, 30, 1000, 100)

	db1 := f.newDB()
	db1.SetMaxOpenConns(1)
	w := New(db1, clock.NewSystem(), nil, Config{})

	done := make(chan error, 1)
	go func() { _, e := w.RunOnce(f.ctx, false); done <- e }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("RunOnce with MaxOpenConns(1) errored: %v", e)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("RunOnce HUNG with MaxOpenConns(1) — lock and work must share one pinned conn")
	}
	if f.countAppLogs(tag) != 0 {
		t.Errorf("MaxOpenConns(1) run should delete old rows, %d remain", f.countAppLogs(tag))
	}
}

// TestTwoWorkersNoOverlap runs two REAL workers concurrently; exactly one acquires the lock
// and deletes, the other reports lock-not-acquired — no batch overlap.
func TestTwoWorkersNoOverlap(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_TWO_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 40)
	f.setSettingFull("app_logs", 1, 30, 5, 20, 40) // slow: 5/batch, up to 20 batches, 40ms pause

	wA := New(f.newDB(), clock.NewSystem(), nil, Config{})
	wB := New(f.newDB(), clock.NewSystem(), nil, Config{})
	var repA, repB Report
	var errA, errB error
	done := make(chan struct{}, 2)
	go func() { repA, errA = wA.RunOnce(f.ctx, false); done <- struct{}{} }()
	time.Sleep(40 * time.Millisecond) // let A grab the lock and enter its paced batches
	go func() { repB, errB = wB.RunOnce(f.ctx, false); done <- struct{}{} }()
	<-done
	<-done
	if errA != nil || errB != nil {
		t.Fatalf("worker errors: A=%v B=%v", errA, errB)
	}
	acquired := 0
	var skipped Report
	if repA.LockAcquired {
		acquired++
		skipped = repB
	} else {
		skipped = repA
	}
	if repB.LockAcquired {
		acquired++
	}
	if acquired != 1 {
		t.Fatalf("exactly one worker must acquire the lock, got %d (A=%v B=%v)", acquired, repA.LockAcquired, repB.LockAcquired)
	}
	if skipped.SkippedReason == "" {
		t.Error("the skipped worker must report a SkippedReason")
	}
	// Only the winner deleted; no overlap/double work — all 40 old rows gone, exactly.
	if n := f.countAppLogs(tag); n != 0 {
		t.Errorf("winner should delete all 40 old rows, %d remain", n)
	}
}

// TestLockReleasedOnError proves the advisory lock is released even when RunOnce returns an
// error (loadSettings fails because the table is temporarily gone).
func TestLockReleasedOnError(t *testing.T) {
	f := setupR(t)
	if _, err := f.db.Exec("RENAME TABLE retention_settings TO retention_settings_x"); err != nil {
		t.Skipf("cannot rename retention_settings to force an error: %v", err)
	}
	restored := false
	restore := func() {
		if !restored {
			f.db.Exec("RENAME TABLE retention_settings_x TO retention_settings")
			restored = true
		}
	}
	t.Cleanup(restore)

	if _, err := f.w.RunOnce(f.ctx, false); err == nil {
		t.Error("RunOnce should error when retention_settings is missing")
	}
	if !f.lockIsFree() {
		t.Error("the advisory lock must be released even when the run errors")
	}
	restore()
}

// ---- PR18 correction #4: lock-skip reported + logged ----

// TestLockSkipReportedAndLogged: when the lock is already held, RunOnce returns a completed
// skipped report AND writes an app_logs entry naming the lock-not-acquired reason.
func TestLockSkipReportedAndLogged(t *testing.T) {
	f := setupR(t)
	tag := fmt.Sprintf("RET_SKIP_%d", time.Now().UnixNano())
	f.seedAppLogs(tag, 100, 3)
	f.setSetting("app_logs", 1, 30, 1000, 100)

	// Hold the lock on a separate connection.
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
	if rep.SkippedReason == "" || rep.FinishedAt.IsZero() {
		t.Errorf("lock-blocked run must be a COMPLETED skipped report (reason=%q finished=%v)", rep.SkippedReason, rep.FinishedAt)
	}
	if f.countAppLogs(tag) != 3 {
		t.Errorf("lock-blocked run must delete nothing, %d remain of 3", f.countAppLogs(tag))
	}
	// And it must be written to app_logs with a lock-not-acquired message.
	_, msg := f.lastRetentionLog()
	if !strings.Contains(msg, "skipped") {
		t.Errorf("lock-skip app_log message = %q, want it to mention the skip", msg)
	}
}
