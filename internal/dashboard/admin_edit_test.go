package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"
	"time"
)

// ---- isoFix config-editing helpers (isolated DB → clean config_versions) ----

func (f *isoFix) exec(q string, a ...any) sql.Result {
	f.t.Helper()
	r, err := f.db.Exec(q, a...)
	if err != nil {
		f.t.Fatalf("exec %q: %v", q, err)
	}
	return r
}

func lastID(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }

func (f *isoFix) activeVersion() int64 {
	var v sql.NullInt64
	f.db.QueryRow("SELECT id FROM config_versions WHERE status='active' ORDER BY id DESC LIMIT 1").Scan(&v)
	return v.Int64
}

func (f *isoFix) activeCount() int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM config_versions WHERE status='active'").Scan(&n)
	return n
}

// seedConfigTarget creates an exchange + market + exchange_market (all flags on) +
// symbol_config (min_spread_bps=40) + exchange_config, plus ONE active config_version.
func (f *isoFix) seedConfigTarget() (exID, emID int64) {
	f.t.Helper()
	u := func(p string) string { return fmt.Sprintf("%s%d", p, time.Now().UnixNano()) }
	exID = lastID(f.exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'X', 1)", u("ex")))
	b := lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	q := lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := lastID(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M")+"/IRT", b, q))
	emID = lastID(f.exec(`INSERT INTO exchange_markets
		(exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage)
		VALUES (?, ?, ?, ?, 1, 1, 1, 1)`, exID, m, u("ES"), u("M")+"/IRT"))
	f.exec("INSERT INTO config_versions (status, created_by, activated_at) VALUES ('active','seed',NOW(6))")
	v := f.activeVersion()
	f.exec("INSERT INTO symbol_configs (exchange_market_id, min_spread_bps, buy_size, buy_size_unit, config_version) VALUES (?, 40, '1', 'quote', ?)", emID, v)
	f.exec("INSERT INTO exchange_configs (exchange_id, max_concurrent_requests, config_version) VALUES (?, 2, ?)", exID, v)
	return
}

// seedMarketUnder creates a second exchange_market under an existing exchange (no new
// config_version), returning its id. Used to test per-market fee auditing.
func (f *isoFix) seedMarketUnder(exID int64) int64 {
	f.t.Helper()
	u := func(p string) string { return fmt.Sprintf("%s%d", p, time.Now().UnixNano()) }
	b := lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B2")))
	q := lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q2")))
	m := lastID(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M2")+"/IRT", b, q))
	return lastID(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", exID, m, u("ES2"), u("M2")+"/IRT"))
}

// seedCycleInState inserts a cycle for the market in the given state (to create exposure).
func (f *isoFix) seedCycleInState(emID int64, st string) int64 {
	f.t.Helper()
	var exID int64
	var sym string
	f.db.QueryRow("SELECT exchange_id, canonical_symbol FROM exchange_markets WHERE id=?", emID).Scan(&exID, &sym)
	return lastID(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, ?)", emID, exID, sym, st))
}

// loginRole creates a fresh user of the role and returns a client logged in as them.
func (f *isoFix) loginRole(role string) *http.Client {
	f.t.Helper()
	user := fmt.Sprintf("%s_%d", role, time.Now().UnixNano())
	if _, err := f.srv.CreateUser(context.Background(), user, "password123", role); err != nil {
		f.t.Fatalf("create %s: %v", role, err)
	}
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar}
	resp, err := c.Post(f.ts.URL+"/login", "application/json", strings.NewReader(fmt.Sprintf(`{"username":%q,"password":"password123"}`, user)))
	if err != nil {
		f.t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("login %s = %d", role, resp.StatusCode)
	}
	return c
}

// loginExisting logs in an already-created user and returns the authenticated client.
func (f *isoFix) loginExisting(user, pass string) *http.Client {
	f.t.Helper()
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar}
	resp, err := c.Post(f.ts.URL+"/login", "application/json", strings.NewReader(fmt.Sprintf(`{"username":%q,"password":%q}`, user, pass)))
	if err != nil {
		f.t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("login %s = %d", user, resp.StatusCode)
	}
	return c
}

func (f *isoFix) postJSON(c *http.Client, path, body string) (int, map[string]any) {
	f.t.Helper()
	resp, err := c.Post(f.ts.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var obj map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&obj)
	return resp.StatusCode, obj
}

func (f *isoFix) getObjWith(c *http.Client, path string) (int, map[string]any) {
	f.t.Helper()
	resp, err := c.Get(f.ts.URL + path)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var obj map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&obj)
	return resp.StatusCode, obj
}

func symbolPath(emID int64) string { return fmt.Sprintf("/api/config/symbol/%d", emID) }
func flagsPath(emID int64) string  { return fmt.Sprintf("/api/config/market/%d/flags", emID) }

func (f *isoFix) minSpread(emID int64) int {
	var v int
	f.db.QueryRow("SELECT min_spread_bps FROM symbol_configs WHERE exchange_market_id=?", emID).Scan(&v)
	return v
}
func (f *isoFix) sellManage(emID int64) bool {
	var v bool
	f.db.QueryRow("SELECT enabled_for_sell_manage FROM exchange_markets WHERE id=?", emID).Scan(&v)
	return v
}

// ---- tests ----

// TestConfigEditVersionedAndAudited: a config_operator edit succeeds, changes the value,
// mints exactly one new active version, and audits the REAL previous value + the session
// user (never a client-supplied changed_by).
func TestConfigEditVersionedAndAudited(t *testing.T) {
	f := setupIso(t)
	op := f.loginRole(RoleConfigOperator)
	_, emID := f.seedConfigTarget()
	v := f.activeVersion()

	code, body := f.postJSON(op, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"widen spread","min_spread_bps":55}`, v))
	if code != http.StatusOK {
		t.Fatalf("edit = %d %v, want 200", code, body)
	}
	if f.minSpread(emID) != 55 {
		t.Errorf("min_spread_bps = %d, want 55", f.minSpread(emID))
	}
	if nv := f.activeVersion(); nv == v {
		t.Error("active version must advance after an edit")
	}
	if f.activeCount() != 1 {
		t.Errorf("active config versions = %d, want exactly 1", f.activeCount())
	}
	var changedBy, reason, oldV, newV string
	f.db.QueryRow("SELECT changed_by, reason, old_value, new_value FROM config_change_audit WHERE field='min_spread_bps' ORDER BY id DESC LIMIT 1").
		Scan(&changedBy, &reason, &oldV, &newV)
	if !strings.HasPrefix(changedBy, RoleConfigOperator) {
		t.Errorf("audit changed_by = %q, want the authenticated session user", changedBy)
	}
	if reason != "widen spread" {
		t.Errorf("audit reason = %q", reason)
	}
	if oldV != "40" || newV != "55" {
		t.Errorf("audit old/new = %s/%s, want 40/55 (real previous value)", oldV, newV)
	}
}

// TestStaleConfigVersionReturns409: an edit whose expected_config_version != the active
// version is rejected with 409 and changes nothing (rollback leaves the previous active).
func TestStaleConfigVersionReturns409(t *testing.T) {
	f := setupIso(t)
	op := f.loginRole(RoleConfigOperator)
	_, emID := f.seedConfigTarget()
	v := f.activeVersion()

	code, _ := f.postJSON(op, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"stale","min_spread_bps":50}`, v+999))
	if code != http.StatusConflict {
		t.Errorf("stale expected_config_version = %d, want 409", code)
	}
	if f.minSpread(emID) != 40 {
		t.Error("a stale edit must not change the value")
	}
	if f.activeVersion() != v {
		t.Error("a stale (rolled-back) edit must leave the previous version active")
	}
	// A missing/zero version is a 400 (mandatory).
	if code, _ := f.postJSON(op, symbolPath(emID), `{"reason":"x","min_spread_bps":50}`); code != http.StatusBadRequest {
		t.Errorf("missing expected_config_version = %d, want 400", code)
	}
}

// TestConcurrentEditsNoLostUpdate: two edits loaded at the same version race; exactly one
// commits (200) and the other gets 409 — no lost update, version advances by exactly one.
func TestConcurrentEditsNoLostUpdate(t *testing.T) {
	f := setupIso(t)
	op := f.loginRole(RoleConfigOperator)
	_, emID := f.seedConfigTarget()
	v := f.activeVersion()

	codes := make(chan int, 2)
	edit := func(val int) {
		code, _ := f.postJSON(op, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"concurrent","min_spread_bps":%d}`, v, val))
		codes <- code
	}
	go edit(50)
	go edit(60)
	c1, c2 := <-codes, <-codes

	ok, conflict := 0, 0
	for _, c := range []int{c1, c2} {
		switch c {
		case http.StatusOK:
			ok++
		case http.StatusConflict:
			conflict++
		}
	}
	if ok != 1 || conflict != 1 {
		t.Errorf("concurrent edits: got %d ok + %d conflict (codes %d,%d), want exactly 1 ok + 1 conflict", ok, conflict, c1, c2)
	}
	if f.activeVersion() != v+1 {
		t.Errorf("active version = %d, want %d (advanced by exactly one)", f.activeVersion(), v+1)
	}
	if f.activeCount() != 1 {
		t.Errorf("active config versions = %d, want 1", f.activeCount())
	}
	if ms := f.minSpread(emID); ms != 50 && ms != 60 {
		t.Errorf("min_spread_bps = %d, want the single winner's value (50 or 60)", ms)
	}
}

// TestSellManageDisableBlockedByExposure: disabling sell management for a market with an
// open/unresolved cycle is 409, and neither the flag nor the version changes.
func TestSellManageDisableBlockedByExposure(t *testing.T) {
	for _, st := range []string{"BUY_REQUEST_QUEUED", "BUY_SUBMITTED", "BUY_PARTIALLY_FILLED", "BUY_FILLED", "SELL_SUBMITTED", "SELL_PARTIALLY_FILLED", "CANCEL_PENDING", "NEEDS_RECONCILE"} {
		t.Run(st, func(t *testing.T) {
			f := setupIso(t)
			_, emID := f.seedConfigTarget()
			f.seedCycleInState(emID, st)
			v := f.activeVersion()
			// Turn trading off in the same edit (the invariant forbids trading on + sell-manage
			// off); the exposure guard must then reject on the still-open exposure.
			code, body := f.postJSON(f.client, flagsPath(emID),
				fmt.Sprintf(`{"expected_config_version":%d,"reason":"disable sell mgmt","enabled_for_trading":false,"enabled_for_sell_manage":false}`, v))
			if code != http.StatusConflict {
				t.Errorf("disable sell-manage with %s exposure = %d %v, want 409", st, code, body)
			}
			if !f.sellManage(emID) {
				t.Error("sell management must stay ENABLED when the change is blocked")
			}
			if f.activeVersion() != v {
				t.Error("a blocked edit must not advance the config version")
			}
		})
	}
}

// TestSellManageDisableAllowedWithNoExposure: with only a terminal cycle (no exposure), an
// admin may disable sell management.
func TestSellManageDisableAllowedWithNoExposure(t *testing.T) {
	f := setupIso(t)
	_, emID := f.seedConfigTarget()
	f.seedCycleInState(emID, "CLOSED") // terminal — not exposure
	v := f.activeVersion()
	// Also turn trading off (invariant: no trading-on + sell-manage-off).
	code, body := f.postJSON(f.client, flagsPath(emID),
		fmt.Sprintf(`{"expected_config_version":%d,"reason":"no open exposure","enabled_for_trading":false,"enabled_for_sell_manage":false}`, v))
	if code != http.StatusOK {
		t.Fatalf("disable with no exposure = %d %v, want 200", code, body)
	}
	if f.sellManage(emID) {
		t.Error("sell management should now be disabled")
	}
}

// TestHighRiskFlagsRequireAdmin: enabling trading or disabling sell management requires the
// admin role; a config_operator gets 403, while normal flag changes are allowed.
func TestHighRiskFlagsRequireAdmin(t *testing.T) {
	f := setupIso(t)
	op := f.loginRole(RoleConfigOperator)
	_, emID := f.seedConfigTarget()
	v := f.activeVersion()

	// config_operator: enabling trading -> 403.
	if code, _ := f.postJSON(op, flagsPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","enabled_for_trading":true}`, v)); code != http.StatusForbidden {
		t.Errorf("config_operator enable trading = %d, want 403", code)
	}
	// config_operator: disabling sell management -> 403.
	if code, _ := f.postJSON(op, flagsPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","enabled_for_sell_manage":false}`, v)); code != http.StatusForbidden {
		t.Errorf("config_operator disable sell-manage = %d, want 403", code)
	}
	// config_operator: a NORMAL change (disable trading) is allowed.
	if code, body := f.postJSON(op, flagsPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"pause new trades","enabled_for_trading":false}`, v)); code != http.StatusOK {
		t.Errorf("config_operator disable trading = %d %v, want 200 (not high-risk)", code, body)
	}
}

// TestReasonMandatoryAndFromSession: a blank/whitespace reason is rejected; a client cannot
// impersonate another user via a changed_by field (strict decode rejects it), and the audit
// records the session user.
func TestReasonMandatoryAndFromSession(t *testing.T) {
	f := setupIso(t)
	op := f.loginRole(RoleConfigOperator)
	_, emID := f.seedConfigTarget()
	v := f.activeVersion()

	for _, reason := range []string{"", "   ", "\t"} {
		if code, _ := f.postJSON(op, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":%q,"min_spread_bps":50}`, v, reason)); code != http.StatusBadRequest {
			t.Errorf("reason %q = %d, want 400", reason, code)
		}
	}
	// An attempt to supply changed_by in the body is rejected (unknown field, strict decode).
	if code, _ := f.postJSON(op, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","min_spread_bps":50,"changed_by":"someone-else"}`, v)); code != http.StatusBadRequest {
		t.Errorf("changed_by in body = %d, want 400 (cannot impersonate)", code)
	}
	// A valid edit records the SESSION user as changed_by.
	if code, _ := f.postJSON(op, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"real","min_spread_bps":52}`, v)); code != http.StatusOK {
		t.Fatal("valid edit should succeed")
	}
	var changedBy string
	f.db.QueryRow("SELECT changed_by FROM config_change_audit WHERE field='min_spread_bps' ORDER BY id DESC LIMIT 1").Scan(&changedBy)
	if !strings.HasPrefix(changedBy, RoleConfigOperator) {
		t.Errorf("changed_by = %q, want the authenticated session user (not a body value)", changedBy)
	}
}

// TestDisabledUserLosesExistingSession: after a user is disabled (active=0), their EXISTING
// session is rejected (401) without waiting for expiry.
func TestDisabledUserLosesExistingSession(t *testing.T) {
	f := setupIso(t)
	user := fmt.Sprintf("viewer_%d", time.Now().UnixNano())
	if _, err := f.srv.CreateUser(context.Background(), user, "password123", RoleViewer); err != nil {
		t.Fatal(err)
	}
	c := f.loginExisting(user, "password123")
	if code, _ := f.getObjWith(c, "/api/me"); code != http.StatusOK {
		t.Fatalf("active user /api/me = %d, want 200", code)
	}
	f.exec("UPDATE dashboard_users SET active=0 WHERE username=?", user)
	if code, _ := f.getObjWith(c, "/api/me"); code != http.StatusUnauthorized {
		t.Errorf("disabled user's existing session /api/me = %d, want 401", code)
	}
}

// TestRoleChangeAppliesToExistingSession: a demotion (admin→viewer) takes effect on the
// existing session — the user can still read, but a mutation now returns 403; a promotion
// (viewer→config_operator) likewise enables editing without a new login.
func TestRoleChangeAppliesToExistingSession(t *testing.T) {
	f := setupIso(t)
	_, emID := f.seedConfigTarget()

	// Demotion: admin -> viewer.
	admin := fmt.Sprintf("adm_%d", time.Now().UnixNano())
	if _, err := f.srv.CreateUser(context.Background(), admin, "password123", RoleAdmin); err != nil {
		t.Fatal(err)
	}
	ac := f.loginExisting(admin, "password123")
	v := f.activeVersion()
	// As admin, a normal edit works.
	if code, _ := f.postJSON(ac, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"admin edit","min_spread_bps":45}`, v)); code != http.StatusOK {
		t.Fatalf("admin edit = %d, want 200", code)
	}
	f.exec("UPDATE dashboard_users SET role='viewer' WHERE username=?", admin)
	// Same session can still READ.
	if code, _ := f.getObjWith(ac, "/api/config"); code != http.StatusOK {
		t.Errorf("demoted user read = %d, want 200 (still a viewer)", code)
	}
	// But a mutation now 403 (live role).
	v2 := f.activeVersion()
	if code, _ := f.postJSON(ac, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"should fail","min_spread_bps":47}`, v2)); code != http.StatusForbidden {
		t.Errorf("demoted user edit = %d, want 403 (role change applied to existing session)", code)
	}

	// Promotion: viewer -> config_operator enables editing on the same session.
	vu := fmt.Sprintf("promo_%d", time.Now().UnixNano())
	if _, err := f.srv.CreateUser(context.Background(), vu, "password123", RoleViewer); err != nil {
		t.Fatal(err)
	}
	vc := f.loginExisting(vu, "password123")
	if code, _ := f.postJSON(vc, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"nope","min_spread_bps":48}`, f.activeVersion())); code != http.StatusForbidden {
		t.Fatalf("viewer edit = %d, want 403", code)
	}
	f.exec("UPDATE dashboard_users SET role='config_operator' WHERE username=?", vu)
	if code, body := f.postJSON(vc, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"now allowed","min_spread_bps":49}`, f.activeVersion())); code != http.StatusOK {
		t.Errorf("promoted user edit = %d %v, want 200 (no re-login needed)", code, body)
	}
}

// TestMultipleActiveVersionsRejected: with 2+ active config versions the edit is refused
// (409) and creates no version / config change / audit row.
func TestMultipleActiveVersionsRejected(t *testing.T) {
	f := setupIso(t)
	op := f.loginRole(RoleConfigOperator)
	_, emID := f.seedConfigTarget()
	v := f.activeVersion()
	// Force a SECOND active version (a corrupt/anomalous state).
	f.exec("INSERT INTO config_versions (status, created_by, activated_at) VALUES ('active','anomaly',NOW(6))")

	var versionsBefore, auditBefore int
	f.db.QueryRow("SELECT COUNT(*) FROM config_versions").Scan(&versionsBefore)
	f.db.QueryRow("SELECT COUNT(*) FROM config_change_audit").Scan(&auditBefore)

	code, _ := f.postJSON(op, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","min_spread_bps":55}`, v))
	if code != http.StatusConflict {
		t.Errorf("edit with multiple active versions = %d, want 409", code)
	}
	if f.minSpread(emID) != 40 {
		t.Error("config value must not change when multiple active versions exist")
	}
	var versionsAfter, auditAfter int
	f.db.QueryRow("SELECT COUNT(*) FROM config_versions").Scan(&versionsAfter)
	f.db.QueryRow("SELECT COUNT(*) FROM config_change_audit").Scan(&auditAfter)
	if versionsAfter != versionsBefore {
		t.Errorf("config_versions changed (%d→%d) — no new version must be created", versionsBefore, versionsAfter)
	}
	if auditAfter != auditBefore {
		t.Errorf("audit rows changed (%d→%d) — no misleading audit must be written", auditBefore, auditAfter)
	}
}

// TestUpsertFeeReturnsRealReadError: a real error reading the previous fee (not ErrNoRows)
// aborts the edit — no version, config, or audit mutation.
func TestUpsertFeeReturnsRealReadError(t *testing.T) {
	f := setupIso(t)
	exID, _ := f.seedConfigTarget()
	v := f.activeVersion()
	var versionsBefore, auditBefore int
	f.db.QueryRow("SELECT COUNT(*) FROM config_versions").Scan(&versionsBefore)
	f.db.QueryRow("SELECT COUNT(*) FROM config_change_audit").Scan(&auditBefore)

	// Break the previous-fee SELECT by renaming exchange_fees away (isolated DB).
	f.exec("RENAME TABLE exchange_fees TO exchange_fees_broken")
	t.Cleanup(func() { f.db.Exec("RENAME TABLE exchange_fees_broken TO exchange_fees") })

	code, _ := f.postJSON(f.client, "/api/config/fee",
		fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","exchange_id":%d,"maker_fee":"0.001","taker_fee":"0.002"}`, v, exID))
	if code != http.StatusInternalServerError {
		t.Errorf("fee edit with a broken previous-fee read = %d, want 500 (error not swallowed)", code)
	}
	var versionsAfter, auditAfter int
	f.db.QueryRow("SELECT COUNT(*) FROM config_versions").Scan(&versionsAfter)
	f.db.QueryRow("SELECT COUNT(*) FROM config_change_audit").Scan(&auditAfter)
	if versionsAfter != versionsBefore || auditAfter != auditBefore {
		t.Errorf("a failed fee read must not mutate version/audit (versions %d→%d, audit %d→%d)", versionsBefore, versionsAfter, auditBefore, auditAfter)
	}
}

// TestUpsertFeeValidatesMarketOwnership: a market-specific fee whose exchange_market_id
// belongs to a DIFFERENT exchange is rejected (400) with no version/audit; the matching
// pair succeeds.
func TestUpsertFeeValidatesMarketOwnership(t *testing.T) {
	f := setupIso(t)
	exA, emA := f.seedConfigTarget()
	// A DIFFERENT exchange (bare — no second active config_version).
	exB := lastID(f.exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'B', 1)", fmt.Sprintf("exB%d", time.Now().UnixNano())))

	// exB + a market that belongs to exA -> 400, no mutation.
	v := f.activeVersion()
	var versionsBefore int
	f.db.QueryRow("SELECT COUNT(*) FROM config_versions").Scan(&versionsBefore)
	code, _ := f.postJSON(f.client, "/api/config/fee",
		fmt.Sprintf(`{"expected_config_version":%d,"reason":"wrong exchange","exchange_id":%d,"exchange_market_id":%d,"maker_fee":"0.001","taker_fee":"0.002"}`, v, exB, emA))
	if code != http.StatusBadRequest {
		t.Errorf("fee for a market of another exchange = %d, want 400", code)
	}
	var versionsAfter int
	f.db.QueryRow("SELECT COUNT(*) FROM config_versions").Scan(&versionsAfter)
	if versionsAfter != versionsBefore {
		t.Error("a rejected cross-exchange fee must not create a config version")
	}
	_ = exA
	// Matching exchange + its own market -> 200.
	if code, body := f.postJSON(f.client, "/api/config/fee",
		fmt.Sprintf(`{"expected_config_version":%d,"reason":"ok","exchange_id":%d,"exchange_market_id":%d,"maker_fee":"0.001","taker_fee":"0.002"}`, f.activeVersion(), exA, emA)); code != http.StatusOK {
		t.Errorf("fee for the market's own exchange = %d %v, want 200", code, body)
	}
}

// TestTradingRequiresSellManage: enabled_for_trading=true while enabled_for_sell_manage is
// false is rejected (400); the safe combinations succeed.
func TestTradingRequiresSellManage(t *testing.T) {
	f := setupIso(t)
	_, emID := f.seedConfigTarget() // seeds all flags = 1

	// trading=true + sell_manage=false -> 400.
	if code, _ := f.postJSON(f.client, flagsPath(emID),
		fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","enabled_for_trading":true,"enabled_for_sell_manage":false}`, f.activeVersion())); code != http.StatusBadRequest {
		t.Errorf("trading=true+sell_manage=false = %d, want 400", code)
	}
	// disabling sell_manage while trading stays enabled -> 400 (effective trading=true, sell=false).
	if code, _ := f.postJSON(f.client, flagsPath(emID),
		fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","enabled_for_sell_manage":false}`, f.activeVersion())); code != http.StatusBadRequest {
		t.Errorf("disable sell_manage while trading on = %d, want 400", code)
	}
	// trading=false + sell_manage=true -> 200 (stop new buys, keep managing inventory).
	if code, body := f.postJSON(f.client, flagsPath(emID),
		fmt.Sprintf(`{"expected_config_version":%d,"reason":"pause buys","enabled_for_trading":false,"enabled_for_sell_manage":true}`, f.activeVersion())); code != http.StatusOK {
		t.Errorf("trading=false+sell_manage=true = %d %v, want 200", code, body)
	}
	// trading=false + sell_manage=false with NO exposure -> 200.
	if code, body := f.postJSON(f.client, flagsPath(emID),
		fmt.Sprintf(`{"expected_config_version":%d,"reason":"fully off","enabled_for_trading":false,"enabled_for_sell_manage":false}`, f.activeVersion())); code != http.StatusOK {
		t.Errorf("trading=false+sell_manage=false (no exposure) = %d %v, want 200", code, body)
	}
}

// TestFeeAuditIdentifiesMarket: a market-specific fee audits by exchange_market_id (distinct
// per market of the same exchange); an exchange-wide fee audits by exchange_id.
func TestFeeAuditIdentifiesMarket(t *testing.T) {
	f := setupIso(t)
	exID, emA := f.seedConfigTarget()
	emB := f.seedMarketUnder(exID)

	feePath := "/api/config/fee"
	// Two market-specific fees on the SAME exchange, different markets.
	if code, _ := f.postJSON(f.client, feePath, fmt.Sprintf(`{"expected_config_version":%d,"reason":"mkt A","exchange_id":%d,"exchange_market_id":%d,"maker_fee":"0.001","taker_fee":"0.002"}`, f.activeVersion(), exID, emA)); code != http.StatusOK {
		t.Fatalf("fee A = %d", code)
	}
	if code, _ := f.postJSON(f.client, feePath, fmt.Sprintf(`{"expected_config_version":%d,"reason":"mkt B","exchange_id":%d,"exchange_market_id":%d,"maker_fee":"0.003","taker_fee":"0.004"}`, f.activeVersion(), exID, emB)); code != http.StatusOK {
		t.Fatalf("fee B = %d", code)
	}
	entType := func(reason string) (string, int64) {
		var et string
		var eid int64
		f.db.QueryRow("SELECT entity_type, entity_id FROM config_change_audit WHERE reason LIKE ? AND field='maker_fee' ORDER BY id DESC LIMIT 1", reason+"%").Scan(&et, &eid)
		return et, eid
	}
	etA, idA := entType("mkt A")
	etB, idB := entType("mkt B")
	if etA != "exchange_market_fee" || idA != emA {
		t.Errorf("market A fee audit = %s/%d, want exchange_market_fee/%d", etA, idA, emA)
	}
	if etB != "exchange_market_fee" || idB != emB {
		t.Errorf("market B fee audit = %s/%d, want exchange_market_fee/%d", etB, idB, emB)
	}
	if idA == idB {
		t.Error("two markets of the same exchange must have DISTINCT audit entity_id")
	}
	// An exchange-wide default fee audits by exchange_id.
	if code, _ := f.postJSON(f.client, feePath, fmt.Sprintf(`{"expected_config_version":%d,"reason":"default","exchange_id":%d,"maker_fee":"0.0005","taker_fee":"0.0006"}`, f.activeVersion(), exID)); code != http.StatusOK {
		t.Fatalf("default fee = %d", code)
	}
	etD, idD := entType("default")
	if etD != "exchange_default_fee" || idD != exID {
		t.Errorf("exchange-wide fee audit = %s/%d, want exchange_default_fee/%d", etD, idD, exID)
	}
}

// feeAuditCount returns how many audit rows exist for a given reason + field.
func (f *isoFix) feeAuditCount(reason, field string) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM config_change_audit WHERE reason LIKE ? AND field=?", reason+"%", field).Scan(&n)
	return n
}

// TestFeeNoOpAndPerFieldAudit: resubmitting identical fees is a no-op (400/ErrNoChanges, no
// version/audit); decimal-equal values are equal; a single-field change audits only that
// field.
func TestFeeNoOpAndPerFieldAudit(t *testing.T) {
	f := setupIso(t)
	exID, _ := f.seedConfigTarget()
	feePath := "/api/config/fee"
	post := func(reason, maker, taker string) (int, map[string]any) {
		return f.postJSON(f.client, feePath, fmt.Sprintf(`{"expected_config_version":%d,"reason":%q,"exchange_id":%d,"maker_fee":%q,"taker_fee":%q}`, f.activeVersion(), reason, exID, maker, taker))
	}

	// Initial insert.
	if code, _ := post("init", "0.001", "0.002"); code != http.StatusOK {
		t.Fatalf("initial fee = %d", code)
	}
	verBefore, auditBefore := f.activeVersion(), func() int { var n int; f.db.QueryRow("SELECT COUNT(*) FROM config_change_audit").Scan(&n); return n }()

	// Resubmit identical -> 400 ErrNoChanges, no version/audit.
	if code, _ := post("noop", "0.001", "0.002"); code != http.StatusBadRequest {
		t.Errorf("resubmit identical fee = %d, want 400", code)
	}
	// Decimal-equal (0.001 == 0.00100000) -> also a no-op.
	if code, _ := post("noop2", "0.00100000", "0.00200000"); code != http.StatusBadRequest {
		t.Errorf("decimal-equal fee = %d, want 400 (no change)", code)
	}
	var verAfter, auditAfter int64
	f.db.QueryRow("SELECT id FROM config_versions WHERE status='active' ORDER BY id DESC LIMIT 1").Scan(&verAfter)
	f.db.QueryRow("SELECT COUNT(*) FROM config_change_audit").Scan(&auditAfter)
	if verAfter != verBefore {
		t.Errorf("no-op fee advanced the config version (%d→%d)", verBefore, verAfter)
	}
	if int(auditAfter) != auditBefore {
		t.Errorf("no-op fee wrote audit rows (%d→%d)", auditBefore, auditAfter)
	}

	// Change ONLY maker_fee -> exactly one maker_fee audit, no taker_fee audit.
	if code, _ := post("maker-only", "0.005", "0.002"); code != http.StatusOK {
		t.Fatalf("maker-only change = %d, want 200", code)
	}
	if c := f.feeAuditCount("maker-only", "maker_fee"); c != 1 {
		t.Errorf("maker-only maker_fee audits = %d, want 1", c)
	}
	if c := f.feeAuditCount("maker-only", "taker_fee"); c != 0 {
		t.Errorf("maker-only taker_fee audits = %d, want 0 (taker unchanged)", c)
	}

	// Change ONLY taker_fee -> exactly one taker_fee audit, no maker_fee audit.
	if code, _ := post("taker-only", "0.005", "0.009"); code != http.StatusOK {
		t.Fatalf("taker-only change = %d, want 200", code)
	}
	if c := f.feeAuditCount("taker-only", "taker_fee"); c != 1 {
		t.Errorf("taker-only taker_fee audits = %d, want 1", c)
	}
	if c := f.feeAuditCount("taker-only", "maker_fee"); c != 0 {
		t.Errorf("taker-only maker_fee audits = %d, want 0 (maker unchanged)", c)
	}
}

// TestStrictJSONDecoding: exactly one JSON object is accepted; trailing content, trailing
// garbage, and unknown fields are all 400.
func TestStrictJSONDecoding(t *testing.T) {
	f := setupIso(t)
	op := f.loginRole(RoleConfigOperator)
	_, emID := f.seedConfigTarget()
	v := f.activeVersion()
	good := fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","min_spread_bps":50}`, v)

	if code, _ := f.postJSON(op, symbolPath(emID), good+` {"min_spread_bps":10}`); code != http.StatusBadRequest {
		t.Error("trailing JSON object must be 400")
	}
	if code, _ := f.postJSON(op, symbolPath(emID), good+` some garbage`); code != http.StatusBadRequest {
		t.Error("trailing garbage must be 400")
	}
	if code, _ := f.postJSON(op, symbolPath(emID), fmt.Sprintf(`{"expected_config_version":%d,"reason":"x","bogus_field":1}`, v)); code != http.StatusBadRequest {
		t.Error("unknown field must be 400")
	}
	if code, body := f.postJSON(op, symbolPath(emID), good); code != http.StatusOK {
		t.Errorf("one valid JSON body = %d %v, want 200", code, body)
	}
}
