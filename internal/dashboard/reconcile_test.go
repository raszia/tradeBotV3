package dashboard

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// seedReconcileCycle inserts a NEEDS_RECONCILE cycle with an entry_buy order (zero fill),
// an ACTIVE symbol lock, and a NEEDS_RECONCILE state event (the reason). Returns ids.
func (f *dfix) seedReconcileCycle(buyFilled string) (cycID, buyID int64, base string) {
	f.t.Helper()
	dseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), dseq) }
	base = u("BAS")
	symbol := base + "/IRT"
	exID := last(f.exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'RC', 1)", u("rcx")))
	b := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", base))
	qa := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", symbol, b, qa))
	em := last(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", exID, m, u("ES"), symbol))
	cycID = last(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, version) VALUES (?, ?, ?, 'NEEDS_RECONCILE', 3)", em, exID, symbol))
	buyID = last(f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity, limit_price) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'NEEDS_RECONCILE', 2, '1', ?, '10')", cycID, exID, em, u("bo"), buyFilled))
	f.exec("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, state, expires_at) VALUES (?, ?, ?, 'ACTIVE', NOW(6) + INTERVAL 600 SECOND)", u("scope"), symbol, cycID)
	f.exec("INSERT INTO cycle_state_events (cycle_id, event_type, from_state, to_state, version, message) VALUES (?, 'needs_reconcile', 'BUY_SUBMITTED', 'NEEDS_RECONCILE', 3, 'ambiguous place ack')", cycID)
	return cycID, buyID, base
}

func (f *dfix) cycleStateD(id int64) string {
	var s string
	f.db.QueryRow("SELECT state FROM cycles WHERE id=?", id).Scan(&s)
	return s
}

func TestReconcileResolutionAuthRequired(t *testing.T) {
	f := setupD(t)
	cyc, _, _ := f.seedReconcileCycle("0")
	for _, ep := range []string{"preview", "apply"} {
		path := fmt.Sprintf("/api/reconcile/%d/%s", cyc, ep)
		body := `{"action":"cancel_zero_exposure","reason":"x"}`
		if code, _ := f.post(path, "", body); code != http.StatusUnauthorized {
			t.Errorf("%s no token = %d, want 401", ep, code)
		}
		if code, _ := f.post(path, "bad-token", body); code != http.StatusUnauthorized {
			t.Errorf("%s bad token = %d, want 401", ep, code)
		}
		if code, _ := f.post(path, f.token(RoleViewer), body); code != http.StatusForbidden {
			t.Errorf("%s viewer = %d, want 403", ep, code)
		}
		if code, _ := f.post(path, f.token(RoleConfigOperator), body); code != http.StatusForbidden {
			t.Errorf("%s config_operator = %d, want 403 (wrong role)", ep, code)
		}
	}
	// The unauthorized attempts must not have changed anything.
	if f.cycleStateD(cyc) != "NEEDS_RECONCILE" {
		t.Error("unauthorized resolution must not mutate the cycle")
	}
}

func TestReconcileDetailShowsContext(t *testing.T) {
	f := setupD(t)
	cyc, buyID, _ := f.seedReconcileCycle("0")
	code, obj := f.getObj(fmt.Sprintf("/api/reconcile/%d", cyc))
	if code != http.StatusOK {
		t.Fatalf("detail = %d", code)
	}
	for _, key := range []string{"cycle", "exchange", "orders", "fills", "requests", "locks", "logs", "reconcile_reason", "balances", "resolutions", "available_actions"} {
		if _, ok := obj[key]; !ok {
			t.Errorf("detail missing context key %q", key)
		}
	}
	orders, _ := obj["orders"].([]any)
	if len(orders) == 0 {
		t.Fatal("detail should include the buy order")
	}
	o0, _ := orders[0].(map[string]any)
	if int64(o0["id"].(float64)) != buyID {
		t.Error("detail order id mismatch")
	}
	// The reason (last NEEDS_RECONCILE event) is surfaced.
	reason, _ := obj["reconcile_reason"].(map[string]any)
	if reason == nil || asString(reason["message"]) == "" {
		t.Error("detail should surface the NEEDS_RECONCILE reason")
	}
	// No secret/credential material anywhere in the detail.
	full := fmt.Sprintf("%v", obj)
	for _, leak := range []string{"encrypted_api", "api_secret", "passphrase", "master_key"} {
		if strings.Contains(full, leak) {
			t.Errorf("reconcile detail leaked %q", leak)
		}
	}
}

func TestReconcilePreviewDoesNotMutateViaHTTP(t *testing.T) {
	f := setupD(t)
	cyc, _, _ := f.seedReconcileCycle("0")
	tok := f.token(RoleReconcileOperator)
	code, obj := f.post(fmt.Sprintf("/api/reconcile/%d/preview", cyc), tok, `{"action":"cancel_zero_exposure","reason":"checking"}`)
	if code != http.StatusOK {
		t.Fatalf("preview = %d (%v)", code, obj)
	}
	plan, _ := obj["preview"].(map[string]any)
	if plan == nil || plan["new_cycle_state"] != "CANCELLED" {
		t.Errorf("preview plan = %v, want new_cycle_state CANCELLED", obj)
	}
	if f.cycleStateD(cyc) != "NEEDS_RECONCILE" {
		t.Error("preview must not mutate the cycle")
	}
}

func TestReconcileApplyResolvesAndAudits(t *testing.T) {
	f := setupD(t)
	cyc, _, _ := f.seedReconcileCycle("0") // zero exposure
	tok := f.token(RoleReconcileOperator)
	code, obj := f.post(fmt.Sprintf("/api/reconcile/%d/apply", cyc), tok, `{"action":"cancel_zero_exposure","reason":"no exposure confirmed"}`)
	if code != http.StatusOK {
		t.Fatalf("apply = %d (%v)", code, obj)
	}
	if f.cycleStateD(cyc) != "CANCELLED" {
		t.Errorf("cycle = %s, want CANCELLED", f.cycleStateD(cyc))
	}
	// Audited with the authenticated operator name (op_reconcile_operator).
	var op, reason string
	f.db.QueryRow("SELECT operator, reason FROM reconcile_resolutions WHERE cycle_id=? ORDER BY id DESC LIMIT 1", cyc).Scan(&op, &reason)
	if op != "op_"+RoleReconcileOperator || reason == "" {
		t.Errorf("audit operator/reason = %q/%q", op, reason)
	}
}

func TestReconcileApplyInvalidReturns400(t *testing.T) {
	f := setupD(t)
	cyc, _, _ := f.seedReconcileCycle("1") // buy filled -> open exposure; cancel_zero_exposure invalid
	tok := f.token(RoleAdmin)
	code, obj := f.post(fmt.Sprintf("/api/reconcile/%d/apply", cyc), tok, `{"action":"cancel_zero_exposure","reason":"x"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("invalid apply = %d (%v), want 400", code, obj)
	}
	if f.cycleStateD(cyc) != "NEEDS_RECONCILE" {
		t.Error("a rejected apply must not mutate the cycle")
	}
}

func TestReconcileListIncludesCycle(t *testing.T) {
	f := setupD(t)
	cyc, _, _ := f.seedReconcileCycle("0")
	code, arr := f.get("/api/reconcile?limit=500")
	if code != http.StatusOK {
		t.Fatalf("list = %d", code)
	}
	found := false
	for _, m := range arr {
		if id, ok := m["id"].(float64); ok && int64(id) == cyc {
			found = true
		}
	}
	if !found {
		t.Error("the NEEDS_RECONCILE cycle should appear in /api/reconcile")
	}
}
