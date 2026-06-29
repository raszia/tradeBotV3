package dashboard

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// setupDLive is like setupD but builds the server in LIVE execution mode (so the preflight
// execution_mode_live check can pass).
func setupDLive(t *testing.T) *dfix {
	f := setupD(t)
	srv := New(f.db, nil, Config{DefaultLimit: 50, MaxLimit: 100, StaleBalanceAge: time.Hour, MasterKey: "dashboard-test-master-key", ExecutionMode: "live"})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close() })
	f.ts = ts
	return f
}

// seedReadyCanary seeds a fully-ready live scenario scoped to a fresh exchange/market and
// points live_controls' canary scope at it. Returns (exchangeID, marketID).
func (f *dfix) seedReadyCanary() (int64, int64) {
	f.t.Helper()
	dseq++
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), dseq) }
	symbol := u("BAS") + "/IRT"
	last := func(q string, a ...any) int64 { r := f.exec(q, a...); id, _ := r.LastInsertId(); return id }
	exID := last("INSERT INTO exchanges (code, name, enabled, live_enabled) VALUES (?, 'PFD', 1, 1)", u("pfdx"))
	b := last("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B"))
	qa := last("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q"))
	m := last("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", symbol, b, qa)
	mkID := last("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, live_enabled) VALUES (?, ?, ?, ?, 1)", exID, m, u("ES"), symbol)
	f.exec("INSERT INTO exchange_credentials (exchange_id, label, enabled, status, key_version, last_checked_at) VALUES (?, 'default', 1, 'active', 1, NOW(6))", exID)
	f.exec("INSERT INTO exchange_health_current (exchange_id, api_key_status, rest_status, last_success_at) VALUES (?, 'ok', 'up', NOW(6))", exID)
	f.exec("INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total, last_seen_at) VALUES (?, 'IRT', '1', '0', '1', NOW(6))", exID)
	f.exec("INSERT INTO comparison_events (exchange_id, canonical_symbol, binance_price, created_at) VALUES (?, ?, '100', NOW(6))", exID, symbol)
	f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run, closed_at) VALUES (?, ?, ?, 'CLOSED', 1, NOW(6))", mkID, exID, symbol)
	f.exec("DELETE FROM live_controls")
	f.exec(`INSERT INTO live_controls
		(id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty,
		 max_consecutive_failures, max_unresolved_reconcile, require_canary_ack, canary_exchange_id, canary_market_id,
		 credential_validation_max_age_minutes, market_data_max_age_seconds, balance_max_age_minutes, dry_run_success_max_age_minutes, health_required)
		VALUES (1, 0, 1, 100, '1000000', '50', '0.01', 10, 1000000, 1, ?, ?, 60, 300, 60, 1440, 0)`, exID, mkID)
	// Pollution-proofing for the shared gated DB (other packages' leftovers).
	f.exec("UPDATE symbol_locks SET state='RELEASED', released_at=NOW(6) WHERE state='ACTIVE' AND expires_at < NOW(6)")
	return exID, mkID
}

func TestPreflightEndpointReadOnly(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary()
	var before int
	f.db.QueryRow("SELECT COUNT(*) FROM live_acknowledgements").Scan(&before)

	code, obj := f.getObj(fmt.Sprintf("/api/live/preflight?exchange_id=%d&market_id=%d", exID, mkID))
	if code != http.StatusOK {
		t.Fatalf("preflight = %d (%v)", code, obj)
	}
	if obj["ready"] != true {
		t.Errorf("expected ready=true; failures=%v", obj["failures"])
	}
	if _, ok := obj["config_hash"].(string); !ok {
		t.Error("expected a config_hash")
	}
	// Read-only: no acknowledgement was created.
	var after int
	f.db.QueryRow("SELECT COUNT(*) FROM live_acknowledgements").Scan(&after)
	if after != before {
		t.Error("GET preflight must not create acknowledgements")
	}
}

func TestAcknowledgeRequiresAdmin(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary()
	body := fmt.Sprintf(`{"exchange_id":%d,"market_id":%d,"reason":"first canary"}`, exID, mkID)

	if c, _ := f.post("/api/live/acknowledge", "", body); c != http.StatusUnauthorized {
		t.Errorf("no token = %d, want 401", c)
	}
	for _, role := range []string{RoleViewer, RoleConfigOperator, RoleCredentialOperator, RoleReconcileOperator} {
		if c, _ := f.post("/api/live/acknowledge", f.token(role), body); c != http.StatusForbidden {
			t.Errorf("%s = %d, want 403 (admin required)", role, c)
		}
	}
	// Admin acknowledges -> 200 and the ack is recorded with the operator + config hash.
	c, obj := f.post("/api/live/acknowledge", f.token(RoleAdmin), body)
	if c != http.StatusOK {
		t.Fatalf("admin acknowledge = %d (%v)", c, obj)
	}
	hash, _ := obj["config_hash"].(string)
	var op, storedHash string
	f.db.QueryRow("SELECT operator, preflight_hash FROM live_acknowledgements WHERE exchange_id=? AND active=1", exID).Scan(&op, &storedHash)
	if op != "op_"+RoleAdmin || storedHash == "" || storedHash != hash {
		t.Errorf("ack = op:%s hash:%s (response hash %s)", op, storedHash, hash)
	}
}

func TestAcknowledgeNotReadyReturns409(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary()
	f.exec("UPDATE live_controls SET kill_switch=1 WHERE id=1") // now not ready
	body := fmt.Sprintf(`{"exchange_id":%d,"market_id":%d,"reason":"x"}`, exID, mkID)
	c, obj := f.post("/api/live/acknowledge", f.token(RoleAdmin), body)
	if c != http.StatusConflict {
		t.Fatalf("not-ready acknowledge = %d (%v), want 409", c, obj)
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM live_acknowledgements WHERE exchange_id=?", exID).Scan(&n)
	if n != 0 {
		t.Error("a not-ready acknowledgement must not be recorded")
	}
}
