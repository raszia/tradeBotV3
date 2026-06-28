package executor

import (
	"testing"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/live"
	"v3TradeBot/internal/simexec"
)

// Live-mode executor with a FAKE (no-network) simexec client + the live Guard as the
// FINAL gate. No real exchange is contacted. Gated on V3_TEST_MYSQL_DSN (via setup()).

func (it *intg) liveExec(t *testing.T, sc simexec.Scenario) {
	t.Helper()
	guard := live.NewGuard(it.store.DB(), clock.NewSystem(), nil)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: simexec.New(it.code, sc)}, nil,
		Config{Name: "live", AllowLiveExecution: true, ExecutionMode: "live", Guard: guard, FinalStatusDelay: 10 * time.Millisecond})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
}

// liveControls writes a permissive configured controls row (kill switch as given) and
// enables live on the exchange + its markets + an active credential.
func (it *intg) liveControls(killSwitch int, withCreds bool) {
	it.db.Exec("UPDATE exchanges SET live_enabled=1 WHERE id=?", it.exID)
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1 WHERE exchange_id=?", it.exID)
	it.db.Exec("DELETE FROM exchange_credentials WHERE exchange_id=?", it.exID)
	if withCreds {
		it.db.Exec("INSERT INTO exchange_credentials (exchange_id, label, enabled, status) VALUES (?, 'd', 1, 'active')", it.exID)
	}
	it.db.Exec("DELETE FROM live_controls")
	// Count-based caps are HUGE so shared-DB pollution (other packages' non-dry-run
	// cycles/orders) never trips the gate; these tests exercise kill-switch/creds/flags.
	it.db.Exec(`INSERT INTO live_controls (id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty, max_consecutive_failures, max_unresolved_reconcile)
		VALUES (1, ?, 1000000000, 1000000000, '1000000000000000', '100000', '1000', 1000000000, 1000000000)`, killSwitch)
}

func (it *intg) liveAuditCount(decision string) int {
	var n int
	it.db.QueryRow("SELECT COUNT(*) FROM live_audit WHERE decision=?", decision).Scan(&n)
	return n
}

func TestLiveGateAllowsAndSends(t *testing.T) {
	it := setup(t)
	it.liveControls(0, true) // kill switch off, creds present
	it.liveExec(t, simexec.FullFill)
	cyc, _, _ := it.seedBuyCycle(t, "0.5")
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1 WHERE exchange_id=?", it.exID) // enable the cycle's market
	it.drive(6)
	if it.cycleState(cyc) != "BUY_FILLED" {
		t.Fatalf("allowed live buy should fill: cycle=%s", it.cycleState(cyc))
	}
	if it.liveAuditCount("allow") == 0 {
		t.Error("an allowed live send must be audited")
	}
}

func TestLiveGateKillSwitchBlocksBuy(t *testing.T) {
	it := setup(t)
	it.liveControls(1, true) // kill switch ENGAGED
	it.liveExec(t, simexec.FullFill)
	cyc, _, placeReq := it.seedBuyCycle(t, "0.5")
	it.drive(3)
	// The buy was NOT sent: the request is FAILED by the gate, the cycle never fills.
	if s := reqStatus(t, it.db, placeReq); s != "FAILED" {
		t.Errorf("kill-switch place = %s, want FAILED (blocked, not sent)", s)
	}
	if it.cycleState(cyc) == "BUY_FILLED" {
		t.Error("kill switch must prevent the buy from filling")
	}
	if it.liveAuditCount("deny") == 0 {
		t.Error("a denied live send must be audited")
	}
}

func TestLiveGateNoCredentialsRefuses(t *testing.T) {
	it := setup(t)
	it.liveControls(0, false) // no credentials
	it.liveExec(t, simexec.FullFill)
	_, _, placeReq := it.seedBuyCycle(t, "0.5")
	it.drive(3)
	if s := reqStatus(t, it.db, placeReq); s != "FAILED" {
		t.Errorf("no-credentials place = %s, want FAILED (refused)", s)
	}
}

func TestLiveAmbiguousPlaceNeedsReconcileNoBlindResend(t *testing.T) {
	it := setup(t)
	it.liveControls(0, true)
	it.liveExec(t, simexec.PlaceTimeout) // PlaceOrder returns an ambiguous ack timeout
	cyc, ord, placeReq := it.seedBuyCycle(t, "0.5")
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1 WHERE exchange_id=?", it.exID)
	it.drive(3)
	// Ambiguous live place -> order+cycle NEEDS_RECONCILE, request DEAD, never re-sent.
	if it.cycleState(cyc) != "NEEDS_RECONCILE" || ordState(t, it.db, ord) != "NEEDS_RECONCILE" {
		t.Errorf("ambiguous live place = cyc:%s ord:%s, want NEEDS_RECONCILE", it.cycleState(cyc), ordState(t, it.db, ord))
	}
	if s := reqStatus(t, it.db, placeReq); s != "DEAD" {
		t.Errorf("ambiguous request = %s, want DEAD (not blindly retried)", s)
	}
}
