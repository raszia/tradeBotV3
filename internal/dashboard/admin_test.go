package dashboard

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// token seeds a dashboard_tokens row for a role and returns the plaintext bearer token
// (only its SHA-256 hash is stored).
func (f *dfix) token(role string) string {
	f.t.Helper()
	tok := fmt.Sprintf("tok_%d_%s", time.Now().UnixNano(), role)
	sum := sha256.Sum256([]byte(tok))
	f.exec("INSERT INTO dashboard_tokens (name, token_hash, role, enabled) VALUES (?, ?, ?, 1)", "op_"+role, hex.EncodeToString(sum[:]), role)
	return tok
}

func (f *dfix) post(path, token, body string) (int, map[string]any) {
	f.t.Helper()
	req, _ := http.NewRequest("POST", f.ts.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var obj map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&obj)
	return resp.StatusCode, obj
}

// seedEditable inserts an exchange + market (collection+signal on, trading off,
// sell_manage on) + a symbol_config + an exchange_config. Returns (emID, exID).
func (f *dfix) seedEditable(signal, trading int) (int64, int64) {
	f.t.Helper()
	dseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), dseq) }
	exID := last(f.exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'D', 1)", u("dx")))
	b := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M")+"/IRT", b, qa))
	em := last(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage) VALUES (?, ?, ?, ?, 1, ?, ?, 1)", exID, m, u("ES"), u("M")+"/IRT", signal, trading))
	f.exec("INSERT INTO symbol_configs (exchange_market_id, min_spread_bps, buy_size, buy_size_unit, sell_offset_bps, reprice_interval_seconds, order_timeout_ms, max_retries, retry_backoff_ms) VALUES (?, 40, '0.5', 'base', 20, 5, 3000, 3, 500)", em)
	f.exec("INSERT INTO exchange_configs (exchange_id, max_concurrent_requests, request_timeout_ms) VALUES (?, 2, 5000)", exID)
	return em, exID
}

func TestUnauthenticatedAndUnauthorizedMutationRejected(t *testing.T) {
	f := setupD(t)
	em, _ := f.seedEditable(1, 0)
	path := fmt.Sprintf("/api/config/symbol/%d", em)

	if code, _ := f.post(path, "", `{"min_spread_bps":60,"reason":"x"}`); code != http.StatusUnauthorized {
		t.Errorf("no token = %d, want 401", code)
	}
	if code, _ := f.post(path, "garbage-token", `{"min_spread_bps":60,"reason":"x"}`); code != http.StatusUnauthorized {
		t.Errorf("bad token = %d, want 401", code)
	}
	if code, _ := f.post(path, f.token(RoleViewer), `{"min_spread_bps":60,"reason":"x"}`); code != http.StatusForbidden {
		t.Errorf("viewer = %d, want 403", code)
	}
}

func TestConfigOperatorUpdateIsVersionedAndAudited(t *testing.T) {
	f := setupD(t)
	em, _ := f.seedEditable(1, 0)
	tok := f.token(RoleConfigOperator)

	code, obj := f.post(fmt.Sprintf("/api/config/symbol/%d", em), tok, `{"min_spread_bps":60,"buy_size":"0.75","reason":"widen spread"}`)
	if code != http.StatusOK {
		t.Fatalf("update = %d (%v), want 200", code, obj)
	}
	version := int64(obj["config_version"].(float64))
	if version <= 0 {
		t.Fatalf("no config version returned: %v", obj)
	}
	// The symbol_config row is updated and stamped with the new version.
	var ms int
	var bs string
	var cv int64
	f.db.QueryRow("SELECT min_spread_bps, buy_size, config_version FROM symbol_configs WHERE exchange_market_id=?", em).Scan(&ms, &bs, &cv)
	if ms != 60 || cv != version {
		t.Errorf("symbol_config = min_spread %d cv %d, want 60 / %d", ms, cv, version)
	}
	// A new active config_versions row exists.
	var active int64
	f.db.QueryRow("SELECT id FROM config_versions WHERE status='active' ORDER BY id DESC LIMIT 1").Scan(&active)
	if active != version {
		t.Errorf("active version = %d, want %d", active, version)
	}
	// Audit rows carry old/new/operator/reason.
	var oldV, newV, by, reason string
	err := f.db.QueryRow("SELECT old_value, new_value, changed_by, reason FROM config_change_audit WHERE config_version=? AND field='min_spread_bps'", version).
		Scan(&oldV, &newV, &by, &reason)
	if err != nil {
		t.Fatalf("audit row missing: %v", err)
	}
	if oldV != "40" || newV != "60" || by != "op_"+RoleConfigOperator || reason != "widen spread" {
		t.Errorf("audit = old %s new %s by %s reason %s", oldV, newV, by, reason)
	}
}

func TestInvalidValueRejected400(t *testing.T) {
	f := setupD(t)
	em, _ := f.seedEditable(1, 0)
	tok := f.token(RoleConfigOperator)
	if code, obj := f.post(fmt.Sprintf("/api/config/symbol/%d", em), tok, `{"min_spread_bps":-5,"reason":"x"}`); code != http.StatusBadRequest {
		t.Errorf("negative min_spread = %d (%v), want 400", code, obj)
	}
	if code, _ := f.post(fmt.Sprintf("/api/config/symbol/%d", em), tok, `{"maker_signal_window_seconds":0,"reason":"x"}`); code != http.StatusBadRequest {
		t.Errorf("zero maker window = %d, want 400", code)
	}
}

func TestEnableFlagHierarchyEnforced(t *testing.T) {
	f := setupD(t)
	tok := f.token(RoleConfigOperator)
	// Market with signal OFF -> enabling trading must be rejected.
	emOff, _ := f.seedEditable(0, 0)
	if code, obj := f.post(fmt.Sprintf("/api/config/market/%d/flags", emOff), tok, `{"enabled_for_trading":true,"reason":"x"}`); code != http.StatusBadRequest {
		t.Errorf("trading without signal = %d (%v), want 400", code, obj)
	}
	// Market with signal ON -> enabling trading is allowed.
	emOn, _ := f.seedEditable(1, 0)
	if code, obj := f.post(fmt.Sprintf("/api/config/market/%d/flags", emOn), tok, `{"enabled_for_trading":true,"reason":"go live"}`); code != http.StatusOK {
		t.Errorf("trading with signal = %d (%v), want 200", code, obj)
	}
}

func TestExchangeAndFeeEditsVersionedAudited(t *testing.T) {
	f := setupD(t)
	_, exID := f.seedEditable(1, 0)
	tok := f.token(RoleConfigOperator)

	if code, obj := f.post(fmt.Sprintf("/api/config/exchange/%d", exID), tok, `{"max_concurrent_requests":4,"reason":"more throughput"}`); code != http.StatusOK {
		t.Errorf("exchange config edit = %d (%v), want 200", code, obj)
	}
	if code, obj := f.post("/api/config/fee", tok, fmt.Sprintf(`{"exchange_id":%d,"maker_fee":"0.001","taker_fee":"0.002","reason":"fee update"}`, exID)); code != http.StatusOK {
		t.Errorf("fee edit = %d (%v), want 200", code, obj)
	}
	// Non-negative fee validation.
	if code, _ := f.post("/api/config/fee", tok, fmt.Sprintf(`{"exchange_id":%d,"maker_fee":"-0.001","taker_fee":"0.002","reason":"x"}`, exID)); code != http.StatusBadRequest {
		t.Errorf("negative fee = %d, want 400", code)
	}
}

func TestRegimeBasketEditVersionedAudited(t *testing.T) {
	f := setupD(t)
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	bid := last(f.exec("INSERT INTO market_regime_baskets (name, enabled, update_interval_seconds, neutral_band_bps, moderate_threshold_bps, strong_threshold_bps) VALUES (?, 1, 60, 5, 30, 100)", fmt.Sprintf("b_%d", time.Now().UnixNano())))
	tok := f.token(RoleConfigOperator)

	if code, obj := f.post(fmt.Sprintf("/api/config/regime/basket/%d", bid), tok, `{"strong_threshold_bps":150,"reason":"tune"}`); code != http.StatusOK {
		t.Errorf("basket edit = %d (%v), want 200", code, obj)
	}
	// Threshold ordering enforced (moderate must be <= strong).
	if code, _ := f.post(fmt.Sprintf("/api/config/regime/basket/%d", bid), tok, `{"moderate_threshold_bps":999,"reason":"x"}`); code != http.StatusBadRequest {
		t.Errorf("disordered thresholds = %d, want 400", code)
	}
}

func TestAuditEndpointShowsChanges(t *testing.T) {
	f := setupD(t)
	em, _ := f.seedEditable(1, 0)
	tok := f.token(RoleConfigOperator)
	f.post(fmt.Sprintf("/api/config/symbol/%d", em), tok, `{"sell_offset_bps":35,"reason":"audit me"}`)
	code, entries := f.get("/api/audit")
	if code != 200 {
		t.Fatalf("audit endpoint = %d", code)
	}
	var found bool
	for _, e := range entries {
		if e["field"] == "sell_offset_bps" && e["reason"] == "audit me" {
			found = true
		}
	}
	if !found {
		t.Error("audit endpoint did not surface the recent change")
	}
}

func TestNoTradingOrCredentialMutationRoutes(t *testing.T) {
	f := setupD(t)
	tok := f.token(RoleAdmin)
	// No credential-editing route exists in PR17.
	if code, _ := f.post("/api/config/credential", tok, `{}`); code != http.StatusNotFound && code != http.StatusMethodNotAllowed {
		t.Errorf("credential route = %d, want 404/405 (no such route)", code)
	}
	// No order/cycle/queue mutation route exists.
	for _, p := range []string{"/api/orders", "/api/cycles/open", "/api/requests"} {
		if code, _ := f.post(p, tok, `{}`); code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s = %d, want 405 (read-only)", p, code)
		}
	}
}

func TestLiveStatusEndpoint(t *testing.T) {
	f := setupD(t)
	_, exID := f.seedEditable(1, 0)
	// Configure live controls + enable live + a credential, then read the status.
	f.exec("UPDATE exchanges SET live_enabled=1 WHERE id=?", exID)
	f.exec("DELETE FROM live_controls")
	f.exec("INSERT INTO live_controls (id, kill_switch, max_open_cycles, max_daily_orders, max_order_notional) VALUES (1, 1, 1, 10, '100')")
	f.exec("INSERT INTO exchange_credentials (exchange_id, label, enabled, status) VALUES (?, 'd', 1, 'active')", exID)
	f.exec("INSERT INTO live_audit (exchange_id, action, decision, reason, execution_mode) VALUES (?, 'place_buy', 'deny', 'kill switch engaged', 'live')", exID)

	// The dashboard server here was built with no ExecutionMode (defaults empty); the
	// status still reflects the DB kill switch + controls + credentials.
	code, obj := f.getObj("/api/live")
	if code != 200 {
		t.Fatalf("/api/live = %d", code)
	}
	if obj["kill_switch"] != true {
		t.Errorf("kill_switch = %v, want true", obj["kill_switch"])
	}
	if obj["controls"] == nil {
		t.Error("controls should be shown")
	}
	if obj["last_live_error"] == nil {
		t.Error("last live error (deny audit) should be shown")
	}
	// Credentials are shown as status only — never any key material.
	creds, _ := obj["credentials"].([]any)
	if len(creds) == 0 {
		t.Error("credential status should be listed")
	}
	for _, c := range creds {
		m, _ := c.(map[string]any)
		for _, secret := range []string{"encrypted_api_key", "encrypted_api_secret", "api_key", "api_secret"} {
			if _, leaked := m[secret]; leaked {
				t.Errorf("/api/live leaked credential field %q", secret)
			}
		}
	}
}
