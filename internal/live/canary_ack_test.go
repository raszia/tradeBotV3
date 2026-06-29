package live

import (
	"fmt"
	"testing"
	"time"

	"v3TradeBot/internal/preflight"
)

// canaryControls writes a controls row that REQUIRES a canary acknowledgement, scoped to
// this fixture's exchange/market, with a 60-minute ack window. Count caps are huge so
// shared-DB pollution never trips them — these tests isolate the PR23 ack + dynamic gate.
func (f *lfix) canaryControls() {
	f.exec("DELETE FROM live_controls")
	f.exec(`INSERT INTO live_controls
		(id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty,
		 max_consecutive_failures, max_unresolved_reconcile, require_canary_ack, canary_exchange_id, canary_market_id, canary_ack_max_age_minutes)
		VALUES (1, 0, 1000000000, 1000000000, '1000000000000000', '1000', '10', 1000000000, 1000000000, 1, ?, ?, 60)`, f.ex, f.em)
}

func (f *lfix) symbolOf() string {
	var s string
	f.db.QueryRow("SELECT canonical_symbol FROM exchange_markets WHERE id=?", f.em).Scan(&s)
	return s
}

func (f *lfix) lastID(r interface{ LastInsertId() (int64, error) }) int64 {
	id, _ := r.LastInsertId()
	return id
}

// seedDynamicReady makes every TIME-SENSITIVE condition fresh so the guard's dynamic
// re-check passes: a recently-validated credential, a fresh balance, fresh market data, and
// a recent successful dry-run for the canary symbol (required before the first live buy).
func (f *lfix) seedDynamicReady() {
	f.exec("UPDATE exchange_credentials SET last_checked_at=NOW(6) WHERE exchange_id=?", f.ex)
	f.exec("INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total, last_seen_at) VALUES (?, 'IRT', '1', '0', '1', NOW(6))", f.ex)
	f.exec("INSERT INTO comparison_events (exchange_id, canonical_symbol, binance_price, created_at) VALUES (?, ?, '100', NOW(6))", f.ex, f.symbolOf())
	f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run, closed_at) VALUES (?, ?, ?, 'CLOSED', 1, NOW(6))", f.em, f.ex, f.symbolOf())
}

func (f *lfix) insertAck(hash string) { f.insertAckAt(hash, 0) }

// insertAckAt inserts an active acknowledgement aged `minutesAgo`.
func (f *lfix) insertAckAt(hash string, minutesAgo int) {
	f.exec("INSERT INTO live_acknowledgements (operator, exchange_id, exchange_market_id, canonical_symbol, preflight_hash, acknowledged_at) VALUES ('op', ?, ?, 'S', ?, NOW(6) - INTERVAL ? MINUTE)", f.ex, f.em, hash, minutesAgo)
}

func (f *lfix) hash() string {
	h, err := preflight.ConfigHash(f.ctx, f.db, f.ex, f.em, "live")
	if err != nil {
		f.t.Fatal(err)
	}
	return h
}

func (f *lfix) sell() PlaceCheck {
	return PlaceCheck{ExchangeID: f.ex, ExchangeMarketID: f.em, Side: "sell", Notional: dec("50"), BaseQty: dec("0.5")}
}

// insertSession inserts an ACTIVE live run session for the fixture's canary scope.
func (f *lfix) insertSession() {
	f.exec("INSERT INTO live_run_sessions (operator, exchange_id, exchange_market_id, canonical_symbol, preflight_hash, status) VALUES ('op', ?, ?, 'S', 'h', 'ACTIVE')", f.ex, f.em)
}

// readyAck sets up canary controls + fresh dynamics + a matching active ack + an active run
// session, so a live buy would be ALLOWED. Returns once that baseline holds.
func (f *lfix) readyAck() {
	f.canaryControls()
	f.seedDynamicReady()
	f.insertAck(f.hash())
	f.insertSession()
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		f.t.Fatalf("baseline canary buy should be allowed: %s", d.Reason)
	}
}

// ---- ack identity / scope / config-hash (PR23) ----

func TestCanaryAckGateRequiresValidAck(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady()
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("buy must be denied without a live acknowledgement")
	}
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Error("buy place must be denied without a live acknowledgement")
	}
	f.insertAck(f.hash())
	f.insertSession()
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		t.Errorf("buy must be allowed with a valid ack: %s", d.Reason)
	}
	f.set("max_order_notional", "999") // config change -> hash mismatch
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("buy must be denied after a config change (ack stale)")
	}
}

func TestNoActiveSessionDeniesBuy(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady()
	f.insertAck(f.hash())
	// A valid ack but NO active session -> buy denied (PR24 session gate).
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("a valid ack without an active session must deny the buy")
	}
	f.insertSession()
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		t.Errorf("with an active session the buy should be allowed: %s", d.Reason)
	}
}

func TestCanaryScopeRestrictsToOneMarket(t *testing.T) {
	f := setupL(t)
	f.readyAck()
	otherEx, otherEm := f.seedMarket(1, 1)
	if d := f.g.AllowNewBuyCycle(f.ctx, otherEx, otherEm); d.Allow {
		t.Error("a buy outside the canary exchange/symbol must be denied")
	}
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		t.Errorf("canary market should be allowed: %s", d.Reason)
	}
}

func TestCanaryAckRequiredButScopeUnset(t *testing.T) {
	f := setupL(t)
	f.exec("DELETE FROM live_controls")
	f.exec(`INSERT INTO live_controls
		(id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty,
		 max_consecutive_failures, max_unresolved_reconcile, require_canary_ack)
		VALUES (1, 0, 1000000000, 1000000000, '1000000000000000', '1000', '10', 1000000000, 1000000000, 1)`)
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("buy must be denied when canary ack is required but scope is unset")
	}
}

// ---- PR23 CORRECTION: expiry + dynamic re-check ----

func TestExpiredAckDeniesBuy(t *testing.T) {
	f := setupL(t)
	f.canaryControls() // 60-minute window
	f.seedDynamicReady()
	f.insertAckAt(f.hash(), 120) // acknowledged 2h ago -> expired
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("an expired acknowledgement must deny a live buy")
	}
}

func TestStaleCredentialAfterAckDenies(t *testing.T) {
	f := setupL(t)
	f.readyAck()
	f.exec("UPDATE exchange_credentials SET last_checked_at = NOW(6) - INTERVAL 200 MINUTE WHERE exchange_id=?", f.ex)
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("stale credential validation after ack must deny the buy")
	}
}

func TestStaleMarketAfterAckDenies(t *testing.T) {
	f := setupL(t)
	f.readyAck()
	f.exec("UPDATE comparison_events SET created_at = NOW(6) - INTERVAL 1 HOUR WHERE canonical_symbol=?", f.symbolOf())
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("stale market data after ack must deny the buy")
	}
}

func TestStaleBalanceAfterAckDenies(t *testing.T) {
	f := setupL(t)
	f.readyAck()
	f.exec("UPDATE wallet_balances_current SET last_seen_at = NOW(6) - INTERVAL 1 DAY WHERE exchange_id=?", f.ex)
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("stale balance after ack must deny the buy")
	}
}

func TestNewReconcileAfterAckDenies(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady()
	// Cap = current global unresolved count, so the baseline passes; +1 then exceeds it.
	var cur int
	f.db.QueryRow("SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state='NEEDS_RECONCILE'").Scan(&cur)
	f.set("max_unresolved_reconcile", cur)
	f.insertAck(f.hash())
	f.insertSession()
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		t.Fatalf("baseline should be allowed: %s", d.Reason)
	}
	f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'NEEDS_RECONCILE', 0)", f.em, f.ex, f.symbolOf())
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("a new unresolved NEEDS_RECONCILE over cap must deny the buy")
	}
}

func TestNewStuckInflightAfterAckDenies(t *testing.T) {
	f := setupL(t)
	f.readyAck()
	uniq := fmt.Sprintf("%d_%d", time.Now().UnixNano(), lseq)
	cyc := f.lastID(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_SUBMITTED', 0)", f.em, f.ex, f.symbolOf()))
	ord := f.lastID(f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'SUBMITTED', '1')", cyc, f.ex, f.em, "so"+uniq))
	f.exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, idempotency_key, payload, timeout_ms, inflight_at)
		VALUES (?, ?, ?, 'PLACE_ORDER', 'IN_FLIGHT', ?, '{}', 1000, NOW(6) - INTERVAL 1 HOUR)`, f.ex, cyc, ord, "idem"+uniq)
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("a stuck IN_FLIGHT mutating request after ack must deny the buy")
	}
}

func TestKillSwitchReengagedAfterAckDenies(t *testing.T) {
	f := setupL(t)
	f.readyAck()
	f.set("kill_switch", 1)
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("re-engaging the kill switch after ack must deny the buy")
	}
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Error("re-engaging the kill switch must deny a live buy place")
	}
}

func TestRiskReducingPathsUnaffectedByAckGate(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady()
	// NO acknowledgement exists. A SELL (exiting inventory) and a CANCEL must still be
	// allowed — the canary ack gate only blocks new buys.
	if d := f.g.CheckPlace(f.ctx, f.sell()); !d.Allow {
		t.Errorf("sell must be allowed without a canary ack: %s", d.Reason)
	}
	if d := f.g.CheckCancel(f.ctx, PlaceCheck{ExchangeID: f.ex}); !d.Allow {
		t.Errorf("cancel must be allowed without a canary ack: %s", d.Reason)
	}
}
