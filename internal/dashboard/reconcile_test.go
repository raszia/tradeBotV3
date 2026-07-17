package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// seedReconcile creates a NEEDS_RECONCILE cycle with a never-sent (PROVEN_ZERO) entry_buy order and
// an ACTIVE symbol lock, and returns the cycle id. dryRun controls the execution mode.
func (f *isoFix) seedReconcile(dryRun bool) int64 {
	f.t.Helper()
	u := func(p string) string { return fmt.Sprintf("%s%d", p, time.Now().UnixNano()) }
	base := u("BAS")
	sym := base + "/IRT"
	exID := lastID(f.exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'R', 1)", u("rx")))
	b := lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", base))
	q := lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := lastID(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", sym, b, q))
	emID := lastID(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", exID, m, u("ES"), sym))
	dry := 0
	if dryRun {
		dry = 1
	}
	cyc := lastID(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run, version) VALUES (?, ?, ?, 'NEEDS_RECONCILE', ?, 1)", emID, exID, sym, dry))
	f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'QUEUED', 1, '1', '0')", cyc, exID, emID, u("bo"))
	f.exec("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, state, expires_at) VALUES (?, ?, ?, 'ACTIVE', NOW(6)+INTERVAL 600 SECOND)", u("scope"), sym, cyc)
	return cyc
}

func (f *isoFix) cycState(id int64) string {
	var s string
	f.db.QueryRow("SELECT state FROM cycles WHERE id=?", id).Scan(&s)
	return s
}

// reconcileEndpoints lists (method, path builder) for every reconcile endpoint.
func reconcilePaths(cyc int64) []struct {
	method, path, body string
} {
	return []struct{ method, path, body string }{
		{"GET", "/api/reconcile", ""},
		{"GET", "/api/reconcile/audit", ""},
		{"GET", fmt.Sprintf("/api/reconcile/%d", cyc), ""},
		{"POST", fmt.Sprintf("/api/reconcile/%d/preview", cyc), `{"action":"keep_needs_reconcile","reason":"x"}`},
		{"POST", fmt.Sprintf("/api/reconcile/%d/apply", cyc), `{"action":"keep_needs_reconcile","reason":"x"}`},
	}
}

func (f *isoFix) reqWith(c *http.Client, method, path, body string) int {
	f.t.Helper()
	var req *http.Request
	var err error
	if body != "" {
		req, err = http.NewRequest(method, f.ts.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, err = http.NewRequest(method, f.ts.URL+path, nil)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TestReconcileEndpointsRequireSession: no cookie -> 401 on every endpoint (details never leak).
func TestReconcileEndpointsRequireSession(t *testing.T) {
	f := setupIso(t)
	cyc := f.seedReconcile(false)
	anon := &http.Client{}
	for _, e := range reconcilePaths(cyc) {
		if code := f.reqWith(anon, e.method, e.path, e.body); code != http.StatusUnauthorized {
			t.Errorf("%s %s without session = %d, want 401", e.method, e.path, code)
		}
	}
}

// TestReconcileEndpointsRejectUnauthorizedRoles: viewer + config_operator -> 403 on every endpoint.
func TestReconcileEndpointsRejectUnauthorizedRoles(t *testing.T) {
	f := setupIso(t)
	cyc := f.seedReconcile(false)
	for _, role := range []string{RoleViewer, RoleConfigOperator} {
		c := f.loginRole(role)
		for _, e := range reconcilePaths(cyc) {
			if code := f.reqWith(c, e.method, e.path, e.body); code != http.StatusForbidden {
				t.Errorf("role %s: %s %s = %d, want 403", role, e.method, e.path, code)
			}
		}
	}
}

// TestReconcileOperatorCannotEditConfig: the reconcile capability must NOT leak config-edit rights.
func TestReconcileOperatorCannotEditConfig(t *testing.T) {
	f := setupIso(t)
	c := f.loginRole(RoleReconcileOperator)
	// A config edit route must 403 for a reconcile_operator (it is not on the config ladder).
	code := f.reqWith(c, "POST", "/api/config/symbol/1", `{"expected_config_version":1,"reason":"x","min_spread_bps":10}`)
	if code != http.StatusForbidden {
		t.Errorf("reconcile_operator config edit = %d, want 403 (no config permission)", code)
	}
	// But a reconcile read must succeed (200).
	cyc := f.seedReconcile(false)
	if code := f.reqWith(c, "GET", fmt.Sprintf("/api/reconcile/%d", cyc), ""); code != http.StatusOK {
		t.Errorf("reconcile_operator detail = %d, want 200", code)
	}
}

// TestReconcilePreviewApplyHappyPath: a reconcile_operator previews then applies a proven-zero
// cancel; the cycle closes and the lock releases; audit records the operator.
func TestReconcilePreviewApplyHappyPath(t *testing.T) {
	f := setupIso(t)
	c := f.loginRole(RoleReconcileOperator)
	cyc := f.seedReconcile(false)
	body := `{"action":"cancel_zero_exposure","reason":"never sent"}`
	code, prev := f.postJSON(c, fmt.Sprintf("/api/reconcile/%d/preview", cyc), body)
	if code != http.StatusOK {
		t.Fatalf("preview = %d %v", code, prev)
	}
	tok, _ := prev["preview_token"].(string)
	if tok == "" {
		t.Fatal("preview must return a preview_token")
	}
	applyBody := fmt.Sprintf(`{"action":"cancel_zero_exposure","reason":"never sent","preview_token":%q}`, tok)
	code, res := f.postJSON(c, fmt.Sprintf("/api/reconcile/%d/apply", cyc), applyBody)
	if code != http.StatusOK {
		t.Fatalf("apply = %d %v", code, res)
	}
	if f.cycState(cyc) != "CANCELLED" {
		t.Errorf("cycle = %s, want CANCELLED", f.cycState(cyc))
	}
	var op string
	f.db.QueryRow("SELECT operator FROM reconcile_resolutions WHERE cycle_id=? ORDER BY id DESC LIMIT 1", cyc).Scan(&op)
	if op == "" || op == "isoadmin" {
		t.Errorf("audit operator = %q, want the reconcile_operator session user", op)
	}
}

// TestReconcileApplyWithoutPreviewRejected: apply with no token -> 400.
func TestReconcileApplyWithoutPreviewRejected(t *testing.T) {
	f := setupIso(t)
	c := f.loginRole(RoleReconcileOperator)
	cyc := f.seedReconcile(false)
	code, _ := f.postJSON(c, fmt.Sprintf("/api/reconcile/%d/apply", cyc), `{"action":"cancel_zero_exposure","reason":"x"}`)
	if code != http.StatusBadRequest {
		t.Errorf("apply without preview_token = %d, want 400", code)
	}
}

// TestReconcileApplyWrongOperatorToken: operator B cannot use operator A's token.
func TestReconcileApplyWrongOperatorToken(t *testing.T) {
	f := setupIso(t)
	a := f.loginRole(RoleReconcileOperator)
	bAdmin := f.loginRole(RoleAdmin)
	cyc := f.seedReconcile(false)
	_, prev := f.postJSON(a, fmt.Sprintf("/api/reconcile/%d/preview", cyc), `{"action":"cancel_zero_exposure","reason":"x"}`)
	tok, _ := prev["preview_token"].(string)
	code, _ := f.postJSON(bAdmin, fmt.Sprintf("/api/reconcile/%d/apply", cyc), fmt.Sprintf(`{"action":"cancel_zero_exposure","reason":"x","preview_token":%q}`, tok))
	if code != http.StatusForbidden {
		t.Errorf("apply with another operator's token = %d, want 403", code)
	}
}

// TestReconcileApplyStateChangedConflicts: state drift after preview -> 409.
func TestReconcileApplyStateChangedConflicts(t *testing.T) {
	f := setupIso(t)
	c := f.loginRole(RoleReconcileOperator)
	cyc := f.seedReconcile(false)
	_, prev := f.postJSON(c, fmt.Sprintf("/api/reconcile/%d/preview", cyc), `{"action":"cancel_zero_exposure","reason":"x"}`)
	tok, _ := prev["preview_token"].(string)
	f.exec("UPDATE cycles SET version=version+1 WHERE id=?", cyc) // drift
	code, _ := f.postJSON(c, fmt.Sprintf("/api/reconcile/%d/apply", cyc), fmt.Sprintf(`{"action":"cancel_zero_exposure","reason":"x","preview_token":%q}`, tok))
	if code != http.StatusConflict {
		t.Errorf("apply after state drift = %d, want 409", code)
	}
}

// TestReconcileDetailSubQueryFailureIs500: a failed secondary query must 500, never a partial 200.
func TestReconcileDetailSubQueryFailureIs500(t *testing.T) {
	f := setupIso(t)
	c := f.loginRole(RoleReconcileOperator)
	cyc := f.seedReconcile(false)
	// Break a secondary query the detail composes (fills).
	f.exec("DROP TABLE fills")
	code := f.reqWith(c, "GET", fmt.Sprintf("/api/reconcile/%d", cyc), "")
	if code != http.StatusInternalServerError {
		t.Errorf("detail with a failing sub-query = %d, want 500 (never a partial 200)", code)
	}
}

// TestReconcileListShowsExecutionMode: live and dry-run cycles are both listed with their mode.
func TestReconcileListShowsExecutionMode(t *testing.T) {
	f := setupIso(t)
	c := f.loginRole(RoleReconcileOperator)
	live := f.seedReconcile(false)
	dry := f.seedReconcile(true)
	resp, err := c.Get(f.ts.URL + "/api/reconcile?limit=100")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list = %d", resp.StatusCode)
	}
	var rows []any
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatalf("decode list array: %v", err)
	}
	seenLive, seenDry := false, false
	for _, r := range rows {
		m, _ := r.(map[string]any)
		idf, _ := m["id"].(float64)
		id := int64(idf)
		if id == live && fmt.Sprint(m["dry_run"]) == "0" {
			seenLive = true
		}
		if id == dry && fmt.Sprint(m["dry_run"]) == "1" {
			seenDry = true
		}
	}
	if !seenLive || !seenDry {
		t.Errorf("list must show both live (dry_run=0) and dry-run (dry_run=1) cycles with their mode; got live=%v dry=%v", seenLive, seenDry)
	}
}
