package preflight

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

var pfseq int

type pffix struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	chk    *Checker
	exID   int64
	mkID   int64
	credID int64
	symbol string
}

func setupPF(t *testing.T) *pffix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the preflight integration test")
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
	f := &pffix{t: t, ctx: ctx, db: db, chk: New(db, clock.NewSystem(), nil, "live")}
	f.seedReady()
	return f
}

func (f *pffix) exec(q string, a ...any) sql.Result {
	r, err := f.db.Exec(q, a...)
	if err != nil {
		f.t.Fatalf("exec %q: %v", q, err)
	}
	return r
}
func (f *pffix) lastID(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }

// seedReady builds a FULLY-READY live scenario (every check passes) scoped to a fresh
// exchange/market, with live_controls' canary scope pointing at it.
func (f *pffix) seedReady() {
	pfseq++
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), pfseq) }
	f.symbol = u("BAS") + "/IRT"
	f.exID = f.lastID(f.exec("INSERT INTO exchanges (code, name, enabled, live_enabled) VALUES (?, 'PF', 1, 1)", u("pfx")))
	b := f.lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := f.lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := f.lastID(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", f.symbol, b, qa))
	f.mkID = f.lastID(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, live_enabled) VALUES (?, ?, ?, ?, 1)", f.exID, m, u("ES"), f.symbol))

	// Active, recently-validated credential.
	f.credID = f.lastID(f.exec("INSERT INTO exchange_credentials (exchange_id, label, enabled, status, key_version, last_checked_at) VALUES (?, 'default', 1, 'active', 1, NOW(6))", f.exID))
	// Healthy private probe.
	f.exec("INSERT INTO exchange_health_current (exchange_id, api_key_status, rest_status, last_success_at) VALUES (?, 'ok', 'up', NOW(6))", f.exID)
	// Fresh balance.
	f.exec("INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total, last_seen_at) VALUES (?, 'IRT', '1', '0', '1', NOW(6))", f.exID)
	// Fresh market data (a recent comparison_event with a Binance price).
	f.exec("INSERT INTO comparison_events (exchange_id, canonical_symbol, binance_price, created_at) VALUES (?, ?, '100', NOW(6))", f.exID, f.symbol)
	// Recent successful dry-run for this market.
	f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run, closed_at) VALUES (?, ?, ?, 'CLOSED', 1, NOW(6))", f.mkID, f.exID, f.symbol)
	// An enabled dashboard token (auth path).
	f.exec("INSERT INTO dashboard_tokens (name, token_hash, role, enabled) VALUES (?, ?, 'admin', 1)", u("op"), u("hash"))

	// Canary-scoped live_controls (singleton): kill switch off, tiny caps, ack required,
	// generous freshness windows so the seeded 'now' timestamps pass.
	f.exec("DELETE FROM live_controls")
	f.exec(`INSERT INTO live_controls
		(id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty,
		 max_consecutive_failures, max_unresolved_reconcile, require_canary_ack, canary_exchange_id, canary_market_id,
		 credential_validation_max_age_minutes, market_data_max_age_seconds, balance_max_age_minutes, dry_run_success_max_age_minutes, health_required)
		VALUES (1, 0, 1, 100, '1000000', '50', '0.01', 10, 1000000, 1, ?, ?, 60, 300, 60, 1440, 0)`, f.exID, f.mkID)
	// Pollution-proofing for the shared gated DB: release any expired locks left by other
	// packages so the global no_stale_lock check reflects this test's clean baseline.
	f.exec("UPDATE symbol_locks SET state='RELEASED', released_at=NOW(6) WHERE state='ACTIVE' AND expires_at < NOW(6)")
}

func (f *pffix) run() Report {
	r, err := f.chk.Run(f.ctx, f.exID, f.mkID)
	if err != nil {
		f.t.Fatalf("run: %v", err)
	}
	return r
}

func failed(r Report, name string) bool {
	for _, c := range r.Checks {
		if c.Name == name {
			return c.Status == StatusFail
		}
	}
	return false
}

// ---- tests ----

func TestPreflightPassesWhenReady(t *testing.T) {
	f := setupPF(t)
	r := f.run()
	if !r.Ready {
		t.Fatalf("expected ready; failures=%v", r.Failures)
	}
	if r.ConfigHash == "" {
		t.Error("expected a config hash")
	}
}

func TestPreflightFailsCredentialMissing(t *testing.T) {
	f := setupPF(t)
	f.exec("DELETE FROM exchange_credentials WHERE exchange_id=?", f.exID)
	r := f.run()
	if r.Ready || !failed(r, "credential_active") {
		t.Errorf("expected credential_active to fail; ready=%v", r.Ready)
	}
}

func TestPreflightFailsCredentialValidationStale(t *testing.T) {
	f := setupPF(t)
	f.exec("UPDATE exchange_credentials SET last_checked_at = NOW(6) - INTERVAL 200 MINUTE WHERE exchange_id=?", f.exID)
	r := f.run()
	if r.Ready || !failed(r, "credential_validation_fresh") {
		t.Errorf("expected credential_validation_fresh to fail; ready=%v", r.Ready)
	}
}

func TestPreflightFailsKillSwitchEngaged(t *testing.T) {
	f := setupPF(t)
	f.exec("UPDATE live_controls SET kill_switch=1 WHERE id=1")
	r := f.run()
	if r.Ready || !failed(r, "kill_switch_disengaged") {
		t.Errorf("expected kill_switch_disengaged to fail; ready=%v", r.Ready)
	}
}

func TestPreflightFailsCapsMissing(t *testing.T) {
	f := setupPF(t)
	f.exec("UPDATE live_controls SET max_order_notional=NULL WHERE id=1")
	r := f.run()
	if r.Ready || !failed(r, "caps_configured") {
		t.Errorf("expected caps_configured to fail; ready=%v", r.Ready)
	}
}

func TestPreflightFailsMarketDataStale(t *testing.T) {
	f := setupPF(t)
	f.exec("UPDATE comparison_events SET created_at = NOW(6) - INTERVAL 1 HOUR WHERE canonical_symbol=?", f.symbol)
	r := f.run()
	if r.Ready || !failed(r, "market_data_fresh") {
		t.Errorf("expected market_data_fresh to fail; ready=%v", r.Ready)
	}
}

func TestPreflightFailsBalanceStale(t *testing.T) {
	f := setupPF(t)
	f.exec("UPDATE wallet_balances_current SET last_seen_at = NOW(6) - INTERVAL 1 DAY WHERE exchange_id=?", f.exID)
	r := f.run()
	if r.Ready || !failed(r, "balance_recent") {
		t.Errorf("expected balance_recent to fail; ready=%v", r.Ready)
	}
}

func TestPreflightFailsUnresolvedReconcileExceedsCap(t *testing.T) {
	f := setupPF(t)
	f.exec("UPDATE live_controls SET max_unresolved_reconcile=0 WHERE id=1")
	// One real NEEDS_RECONCILE cycle on this market.
	f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'NEEDS_RECONCILE', 0)", f.mkID, f.exID, f.symbol)
	r := f.run()
	if r.Ready || !failed(r, "reconcile_within_cap") {
		t.Errorf("expected reconcile_within_cap to fail; ready=%v", r.Ready)
	}
}

func TestPreflightFailsStuckInflight(t *testing.T) {
	f := setupPF(t)
	cyc := f.lastID(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_SUBMITTED', 0)", f.mkID, f.exID, f.symbol))
	ord := f.lastID(f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'SUBMITTED', '1')", cyc, f.exID, f.mkID, fmt.Sprintf("o%d", pfseq)))
	// A PLACE_ORDER stuck IN_FLIGHT past its timeout.
	f.exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, idempotency_key, payload, timeout_ms, inflight_at)
		VALUES (?, ?, ?, 'PLACE_ORDER', 'IN_FLIGHT', ?, '{}', 1000, NOW(6) - INTERVAL 1 HOUR)`, f.exID, cyc, ord, fmt.Sprintf("idem%d", pfseq))
	r := f.run()
	if r.Ready || !failed(r, "no_stuck_inflight") {
		t.Errorf("expected no_stuck_inflight to fail; ready=%v", r.Ready)
	}
}

func TestPreflightRequiresRecentDryRun(t *testing.T) {
	f := setupPF(t)
	f.exec("UPDATE cycles SET closed_at = NOW(6) - INTERVAL 10 DAY WHERE exchange_market_id=? AND dry_run=1", f.mkID)
	r := f.run()
	if r.Ready || !failed(r, "recent_dry_run_success") {
		t.Errorf("expected recent_dry_run_success to fail; ready=%v", r.Ready)
	}
}

func TestPreflightDoesNotMutate(t *testing.T) {
	f := setupPF(t)
	count := func(tbl string) int {
		var n int
		f.db.QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&n)
		return n
	}
	before := []int{count("cycles"), count("orders"), count("exchange_requests"), count("live_acknowledgements"), count("live_audit")}
	_ = f.run()
	after := []int{count("cycles"), count("orders"), count("exchange_requests"), count("live_acknowledgements"), count("live_audit")}
	for i := range before {
		if before[i] != after[i] {
			t.Errorf("preflight mutated trading state (table idx %d: %d -> %d)", i, before[i], after[i])
		}
	}
}

func TestAcknowledgeRecordsOperatorAndHash(t *testing.T) {
	f := setupPF(t)
	id, report, err := f.chk.Acknowledge(f.ctx, AckInput{ExchangeID: f.exID, MarketID: f.mkID, Operator: "alice", Reason: "first canary"})
	if err != nil {
		t.Fatal(err)
	}
	var op, hash, reason string
	var credID sql.NullInt64
	f.db.QueryRow("SELECT operator, preflight_hash, reason, credential_id FROM live_acknowledgements WHERE id=?", id).Scan(&op, &hash, &reason, &credID)
	if op != "alice" || hash != report.ConfigHash || reason == "" {
		t.Errorf("ack = op:%s hash:%s reason:%q (want alice / %s)", op, hash, reason, report.ConfigHash)
	}
	if !credID.Valid || credID.Int64 != f.credID {
		t.Errorf("ack credential_id = %v, want %d", credID, f.credID)
	}
}

func TestAcknowledgeRefusedWhenNotReady(t *testing.T) {
	f := setupPF(t)
	f.exec("UPDATE live_controls SET kill_switch=1 WHERE id=1")
	if _, _, err := f.chk.Acknowledge(f.ctx, AckInput{ExchangeID: f.exID, MarketID: f.mkID, Operator: "alice", Reason: "x"}); err != ErrNotReady {
		t.Errorf("ack with failing preflight err = %v, want ErrNotReady", err)
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM live_acknowledgements WHERE exchange_id=?", f.exID).Scan(&n)
	if n != 0 {
		t.Error("a not-ready acknowledgement must not be recorded")
	}
}

func TestConfigChangeInvalidatesAcknowledgement(t *testing.T) {
	f := setupPF(t)
	_, report, err := f.chk.Acknowledge(f.ctx, AckInput{ExchangeID: f.exID, MarketID: f.mkID, Operator: "alice", Reason: "ack"})
	if err != nil {
		t.Fatal(err)
	}
	// Change a config-relevant value (a cap) -> the config hash must change, so the ack is stale.
	f.exec("UPDATE live_controls SET max_order_notional='999' WHERE id=1")
	newHash, err := ConfigHash(f.ctx, f.db, f.exID, f.mkID, "live")
	if err != nil {
		t.Fatal(err)
	}
	if newHash == report.ConfigHash {
		t.Error("changing a cap must change the config hash (ack should become stale)")
	}
	// The stored active ack still has the OLD hash.
	stored, ok := ActiveAckHash(f.ctx, f.db, f.exID, f.mkID)
	if !ok || stored == newHash {
		t.Error("active ack should still hold the old hash (now != current config)")
	}
}
