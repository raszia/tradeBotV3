package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"

	"v3TradeBot/internal/migrate"
)

// ---- offline (no DB needed) ----

// TestServerHoldsNoOrderClient: the dashboard can't trade by construction — it holds
// only a DB handle (+ config/log), no exchange client, no queue.
func TestServerHoldsNoOrderClient(t *testing.T) {
	type placer interface{ PlaceOrder(any) any }
	type canceller interface{ CancelOrder(any) any }
	type enqueuer interface{ Enqueue(any) any }
	st := reflect.TypeOf(Server{})
	for _, badT := range []reflect.Type{
		reflect.TypeOf((*placer)(nil)).Elem(),
		reflect.TypeOf((*canceller)(nil)).Elem(),
		reflect.TypeOf((*enqueuer)(nil)).Elem(),
	} {
		for i := 0; i < st.NumField(); i++ {
			if st.Field(i).Type.Implements(badT) {
				t.Errorf("Server.%s implements %s — dashboard must be read-only", st.Field(i).Name, badT)
			}
		}
	}
}

// TestMutatingMethodsRejected: every route is GET-only, so ANY mutating method (POST/
// PUT/PATCH/DELETE — e.g. an attempt to edit config, cancel an order, retry a request)
// is 405 on EVERY operational path. There is NO mutating route at all. The method
// mismatch is resolved by the mux before any DB access, so a nil DB is fine here.
func TestMutatingMethodsRejected(t *testing.T) {
	h := New(nil, nil, Config{}).Handler()
	paths := []string{
		"/api/cycles/open", "/api/cycles/closed", "/api/cycles/123", "/api/orders",
		"/api/fills", "/api/requests", "/api/signals", "/api/comparisons", "/api/balances",
		"/api/health", "/api/regime", "/api/logs", "/api/api-logs", "/api/config",
	}
	for _, method := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		for _, p := range paths {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(method, p, nil))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d, want 405 (read-only; no mutating route exists)", method, p, rec.Code)
			}
		}
	}
}

func TestStepKind(t *testing.T) {
	if stepKind("RETRY_SCHEDULED", 0) != "scheduled_next_step" {
		t.Error("retry_count 0 should be a scheduled next step")
	}
	if stepKind("RETRY_SCHEDULED", 2) != "retry" {
		t.Error("retry_count >0 should be a retry")
	}
	if stepKind("QUEUED", 0) != "" {
		t.Error("non-RETRY_SCHEDULED has no step_kind")
	}
}

func TestMaskSecrets(t *testing.T) {
	in := `{"Authorization":"Bearer sk-abc123","X-API-KEY":"keyVALUE","note":"ok","signature":"deadbeef"}`
	out := maskSecrets(in)
	for _, leak := range []string{"sk-abc123", "keyVALUE", "deadbeef"} {
		if strings.Contains(out, leak) {
			t.Errorf("masked output still leaks %q: %s", leak, out)
		}
	}
	if !strings.Contains(out, `"note":"ok"`) {
		t.Errorf("masking clobbered a non-secret field: %s", out)
	}
}

// ---- gated ----

type dfix struct {
	t      *testing.T
	db     *sql.DB
	srv    *Server
	ts     *httptest.Server
	ctx    context.Context
	client *http.Client // authenticated as a fresh admin (so read tests just work)
}

func setupD(t *testing.T) *dfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the dashboard integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	srv := New(db, nil, Config{DefaultLimit: 50, MaxLimit: 100, StaleBalanceAge: time.Hour, SessionTTL: time.Hour})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); db.Close() })
	f := &dfix{t: t, db: db, srv: srv, ts: ts, ctx: ctx}
	// A default admin client so the (PR16) read-endpoint tests keep working under PR17 auth.
	f.client = f.loginNewUser("admin", RoleAdmin)
	return f
}

var userSeq int

// uniqueUser returns a per-test-unique username so tests sharing the DB don't collide.
func (f *dfix) uniqueUser(prefix string) string {
	userSeq++
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), userSeq)
}

// newClient is an http.Client with its own cookie jar (unauthenticated).
func (f *dfix) newClient() *http.Client {
	jar, _ := cookiejar.New(nil)
	return &http.Client{Jar: jar}
}

// createUser makes a fresh user with the given role and returns (username, password).
func (f *dfix) createUser(role string) (string, string) {
	f.t.Helper()
	user, pass := f.uniqueUser(role), "password123"
	if _, err := f.srv.CreateUser(f.ctx, user, pass, role); err != nil {
		f.t.Fatalf("create user: %v", err)
	}
	return user, pass
}

// loginNewUser creates a user with the role and returns a client logged in as them.
func (f *dfix) loginNewUser(prefix, role string) *http.Client {
	f.t.Helper()
	user, pass := f.uniqueUser(prefix), "password123"
	if _, err := f.srv.CreateUser(f.ctx, user, pass, role); err != nil {
		f.t.Fatalf("create user: %v", err)
	}
	c := f.newClient()
	if code, _ := f.login(c, user, pass); code != http.StatusOK {
		f.t.Fatalf("login as %s = %d, want 200", user, code)
	}
	return c
}

// login POSTs /login on client c; returns (status, body).
func (f *dfix) login(c *http.Client, user, pass string) (int, map[string]any) {
	f.t.Helper()
	body := fmt.Sprintf(`{"username":%q,"password":%q}`, user, pass)
	resp, err := c.Post(f.ts.URL+"/login", "application/json", strings.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var obj map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&obj)
	return resp.StatusCode, obj
}

// wsHeader returns the WebSocket dial header carrying c's session cookie (if any).
func (f *dfix) wsHeader(c *http.Client) http.Header {
	h := http.Header{}
	u, _ := url.Parse(f.ts.URL)
	for _, ck := range c.Jar.Cookies(u) {
		if ck.Name == sessionCookie {
			h.Set("Cookie", sessionCookie+"="+ck.Value)
		}
	}
	return h
}

func (f *dfix) get(path string) (int, []map[string]any) { return f.getWith(f.client, path) }

func (f *dfix) getWith(c *http.Client, path string) (int, []map[string]any) {
	f.t.Helper()
	resp, err := c.Get(f.ts.URL + path)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var arr []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&arr)
	return resp.StatusCode, arr
}

func (f *dfix) getObj(path string) (int, map[string]any) { return f.getObjWith(f.client, path) }

func (f *dfix) getObjWith(c *http.Client, path string) (int, map[string]any) {
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

// postJSON POSTs a raw JSON body on client c and returns (status, body).
func (f *dfix) postJSON(c *http.Client, path, body string) (int, map[string]any) {
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

// activeVersion returns the current active config_version id (0 if none).
func (f *dfix) activeVersion() int64 {
	var v sql.NullInt64
	f.db.QueryRow("SELECT id FROM config_versions WHERE status='active' ORDER BY id DESC LIMIT 1").Scan(&v)
	return v.Int64
}

func (f *dfix) exec(q string, a ...any) sql.Result {
	f.t.Helper()
	r, err := f.db.Exec(q, a...)
	if err != nil {
		f.t.Fatalf("seed %q: %v", q, err)
	}
	return r
}

var dseq int

// seedCycle inserts an exchange + market + cycle + buy order + fill + a QUEUED request
// and returns (cycleID, exchangeID).
func (f *dfix) seedCycle() (int64, int64) { return seedCycleInto(f.t, f.db) }

// seedCycleInto seeds a full cycle graph into an arbitrary db (shared or isolated) so
// both the shared-DB and isolated-DB (error-handling) fixtures can reuse it.
func seedCycleInto(t *testing.T, db *sql.DB) (int64, int64) {
	t.Helper()
	dseq++
	execOrFail := func(q string, a ...any) sql.Result {
		r, err := db.Exec(q, a...)
		if err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), dseq) }
	exID := last(execOrFail("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'D', 1)", u("dx")))
	b := last(execOrFail("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(execOrFail("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(execOrFail("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M")+"/IRT", b, qa))
	em := last(execOrFail("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", exID, m, u("ES"), u("M")+"/IRT"))
	cyc := last(execOrFail("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, 'BUY_SUBMITTED')", em, exID, u("M")+"/IRT"))
	ord := last(execOrFail("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity, intended_execution_mode) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'ACKED', '1', 'MAKER_FIRST')", cyc, exID, em, u("loc")))
	execOrFail("INSERT INTO fills (order_id, cycle_id, exchange_fill_id, quantity, price) VALUES (?, ?, ?, '1', '100')", ord, cyc, u("fill"))
	execOrFail("INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, payload, idempotency_key, retry_count) VALUES (?, ?, ?, 'CANCEL_ORDER', 'RETRY_SCHEDULED', '{}', ?, 0)", exID, cyc, ord, u("idem"))
	execOrFail("INSERT INTO cycle_state_events (cycle_id, from_state, to_state, version, event_type) VALUES (?, 'NEW', 'BUY_SUBMITTED', 1, 'x')", cyc)
	return cyc, exID
}

func TestEndpointsMissingDataNoPanic(t *testing.T) {
	f := setupD(t)
	for _, p := range []string{"/api/cycles/open", "/api/cycles/closed", "/api/orders", "/api/fills",
		"/api/requests", "/api/signals", "/api/comparisons", "/api/balances", "/api/health", "/api/regime", "/api/logs", "/api/api-logs"} {
		if code, _ := f.get(p); code != http.StatusOK {
			t.Errorf("%s on empty data = %d, want 200", p, code)
		}
	}
	if code, _ := f.getObj("/api/config"); code != http.StatusOK {
		t.Errorf("/api/config = %d, want 200", code)
	}
}

func TestSeededCycleEndpoints(t *testing.T) {
	f := setupD(t)
	cyc, _ := f.seedCycle()

	if code, arr := f.get("/api/cycles/open"); code != 200 || len(arr) == 0 {
		t.Fatalf("open cycles = %d/%d rows", code, len(arr))
	}
	// requests carry the scheduled-vs-retry label.
	code, reqs := f.get("/api/requests")
	if code != 200 || len(reqs) == 0 {
		t.Fatalf("requests = %d/%d", code, len(reqs))
	}
	if reqs[0]["step_kind"] != "scheduled_next_step" {
		t.Errorf("step_kind = %v, want scheduled_next_step (RETRY_SCHEDULED + retry_count 0)", reqs[0]["step_kind"])
	}
	// cycle detail composes the related rows + maker/taker + fee note.
	code, detail := f.getObj(fmt.Sprintf("/api/cycles/%d", cyc))
	if code != 200 {
		t.Fatalf("cycle detail = %d", code)
	}
	for _, k := range []string{"cycle", "orders", "fills", "requests", "cycle_events", "locks", "logs", "fee_note"} {
		if _, ok := detail[k]; !ok {
			t.Errorf("cycle detail missing %q", k)
		}
	}
	orders, _ := detail["orders"].([]any)
	if len(orders) == 0 {
		t.Fatal("detail has no orders")
	}
	if o0, _ := orders[0].(map[string]any); o0["intended_execution_mode"] != "MAKER_FIRST" {
		t.Errorf("order intended_execution_mode = %v, want MAKER_FIRST", o0["intended_execution_mode"])
	}
	// unknown cycle -> 404.
	if code, _ := f.getObj("/api/cycles/999999999"); code != http.StatusNotFound {
		t.Errorf("missing cycle = %d, want 404", code)
	}
}

func TestRetryStepKind(t *testing.T) {
	f := setupD(t)
	_, exID := f.seedCycle()
	f.exec("INSERT INTO exchange_requests (exchange_id, request_type, status, payload, idempotency_key, retry_count) VALUES (?, 'GET_BALANCE', 'RETRY_SCHEDULED', '{}', ?, 3)", exID, fmt.Sprintf("r_%d", time.Now().UnixNano()))
	_, reqs := f.get("/api/requests")
	var sawRetry bool
	for _, m := range reqs {
		if m["step_kind"] == "retry" {
			sawRetry = true
		}
	}
	if !sawRetry {
		t.Error("a RETRY_SCHEDULED with retry_count>0 should be labelled 'retry'")
	}
}

func TestBalancesStaleAndMasking(t *testing.T) {
	f := setupD(t)
	_, exID := f.seedCycle()
	// A balance not seen for a while -> flagged stale, value preserved (not zeroed).
	// Unique asset so it can't collide with other packages' seeded balances (shared DB).
	staleAsset := fmt.Sprintf("STALE%d", time.Now().UnixNano())
	f.exec("INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total, last_seen_at) VALUES (?, ?, '1.5', '0', '1.5', NOW(6) - INTERVAL 2 HOUR)", exID, staleAsset)
	code, bals := f.get("/api/balances")
	if code != 200 || len(bals) == 0 {
		t.Fatalf("balances = %d/%d", code, len(bals))
	}
	var found bool
	for _, m := range bals {
		if m["asset"] == staleAsset {
			found = true
			if m["stale"] != true {
				t.Errorf("BTC balance should be flagged stale, got %v", m["stale"])
			}
			if av, _ := m["available"].(string); av == "" || av == "0" {
				t.Errorf("stale balance must keep its value (not zeroed), got %v", m["available"])
			}
		}
	}
	if !found {
		t.Error("seeded balance not returned")
	}

	// api-call-log with a secret in headers is masked on display.
	f.exec(`INSERT INTO api_call_logs (exchange_id, method, url, request_headers, response_status, created_at)
		VALUES (?, 'GET', 'https://x/api', ?, 200, NOW(6))`, exID, `{"Authorization":"Bearer sk-LEAKME","X-API-KEY":"keyLEAK"}`)
	_, logs := f.get("/api/api-logs")
	for _, m := range logs {
		if h, _ := m["request_headers"].(string); strings.Contains(h, "LEAK") {
			t.Errorf("api-log leaked a secret: %s", h)
		}
	}
}

func TestPaginationLimit(t *testing.T) {
	f := setupD(t)
	_, exID := f.seedCycle()
	for i := 0; i < 5; i++ {
		f.exec("INSERT INTO signals (exchange_id, canonical_symbol, binance_price, iranian_price, spread_bps, fee_adjusted_spread_bps, buy_size, accepted, signal_time) VALUES (?, 'X/IRT', '1', '1', 1, 1, '1', 1, NOW(6))", exID)
	}
	if _, arr := f.get("/api/signals?limit=2"); len(arr) != 2 {
		t.Errorf("limit=2 returned %d rows, want 2", len(arr))
	}
	// over-max clamps (MaxLimit=100); just assert it returns without error.
	if code, _ := f.get("/api/signals?limit=99999"); code != 200 {
		t.Errorf("limit clamp = %d, want 200", code)
	}
}

func TestWebSocketLiveSnapshot(t *testing.T) {
	f := setupD(t)
	f.seedCycle()
	url := "ws" + strings.TrimPrefix(f.ts.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, f.wsHeader(f.client))
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var msg map[string]any
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatalf("ws read: %v", err)
	}
	if msg["type"] != "snapshot" {
		t.Errorf("ws message type = %v, want snapshot", msg["type"])
	}
	for _, k := range []string{"open_cycles", "health", "regime", "balances"} {
		if _, ok := msg[k]; !ok {
			t.Errorf("ws snapshot missing %q", k)
		}
	}
}
