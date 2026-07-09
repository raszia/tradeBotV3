package dashboard

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"

	"v3TradeBot/internal/migrate"
)

// ---- isolated-DB fixture (for DB-error handling) ----
//
// These tests must make ONE specific sub-query fail. To do that safely without touching
// the shared v3tb schema other packages use, each test runs against its own throwaway
// database that it can freely break (rename a table away) and restore.

type isoFix struct {
	t      *testing.T
	db     *sql.DB
	srv    *Server
	ts     *httptest.Server
	client *http.Client // authenticated as an admin on this isolated DB
}

func setupIso(t *testing.T) *isoFix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the dashboard error-handling test")
	}
	cfg, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("v3tb_dasherr_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatalf("create throwaway db: %v", err)
	}
	t.Cleanup(func() {
		a, err := sql.Open("mysql", dsn)
		if err == nil {
			a.Exec("DROP DATABASE IF EXISTS " + name)
			a.Close()
		}
	})
	cfg.DBName = name
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.Run(context.Background(), db, migrate.FS); err != nil {
		t.Fatalf("migrate throwaway db: %v", err)
	}
	srv := New(db, nil, Config{DefaultLimit: 50, MaxLimit: 100, StaleBalanceAge: time.Hour, SessionTTL: time.Hour})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); db.Close() })
	f := &isoFix{t: t, db: db, srv: srv, ts: ts}
	// Authenticate an admin (all routes require a session under PR17).
	if _, err := srv.CreateUser(context.Background(), "isoadmin", "password123", RoleAdmin); err != nil {
		t.Fatalf("create iso admin: %v", err)
	}
	jar, _ := cookiejar.New(nil)
	f.client = &http.Client{Jar: jar}
	resp, err := f.client.Post(ts.URL+"/login", "application/json", strings.NewReader(`{"username":"isoadmin","password":"password123"}`))
	if err != nil {
		t.Fatalf("iso login: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("iso login = %d, want 200", resp.StatusCode)
	}
	return f
}

func (f *isoFix) statusOf(path string) int {
	f.t.Helper()
	resp, err := f.client.Get(f.ts.URL + path)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// wsHeader returns the WebSocket dial header carrying the authenticated session cookie.
func (f *isoFix) wsHeader() http.Header {
	h := http.Header{}
	u, _ := url.Parse(f.ts.URL)
	for _, ck := range f.client.Jar.Cookies(u) {
		if ck.Name == sessionCookie {
			h.Set("Cookie", sessionCookie+"="+ck.Value)
		}
	}
	return h
}

// breakTable renames a table away (so any query against it errors) for the duration of fn,
// then restores it. Runs on the isolated DB, so it never affects the shared schema.
func (f *isoFix) breakTable(name string, fn func()) {
	f.t.Helper()
	if _, err := f.db.Exec("RENAME TABLE " + name + " TO " + name + "_broken"); err != nil {
		f.t.Fatalf("rename %s away: %v", name, err)
	}
	defer func() {
		if _, err := f.db.Exec("RENAME TABLE " + name + "_broken TO " + name); err != nil {
			f.t.Fatalf("restore %s: %v", name, err)
		}
	}()
	fn()
}

// TestCycleDetailDBErrorsReturn500 proves reviewer #6: if ANY related sub-query fails, the
// cycle-detail endpoint returns 500 — never a 200 with partial data (an operator must not
// misread "no orders" / "no logs" as a fact when the query actually failed).
func TestCycleDetailDBErrorsReturn500(t *testing.T) {
	f := setupIso(t)
	cyc, _ := seedCycleInto(t, f.db)
	path := fmt.Sprintf("/api/cycles/%d", cyc)

	// Sanity: with everything intact the detail is 200.
	if code := f.statusOf(path); code != http.StatusOK {
		t.Fatalf("intact cycle detail = %d, want 200", code)
	}
	for _, tbl := range []string{"orders", "fills", "exchange_requests", "app_logs",
		"cycle_state_events", "symbol_locks"} {
		f.breakTable(tbl, func() {
			if code := f.statusOf(path); code != http.StatusInternalServerError {
				t.Errorf("cycle detail with %s query failing = %d, want 500 (no partial 200)", tbl, code)
			}
		})
	}
}

// TestConfigDBErrorsReturn500 proves reviewer #7: /api/config returns 500 if ANY config
// table cannot be loaded — never an incomplete config with 200.
func TestConfigDBErrorsReturn500(t *testing.T) {
	f := setupIso(t)

	if code := f.statusOf("/api/config"); code != http.StatusOK {
		t.Fatalf("intact /api/config = %d, want 200", code)
	}
	// table -> the config query it breaks: exchange / market / fee / regime.
	for _, tbl := range []string{"exchange_configs", "symbol_configs", "exchange_fees", "market_regime_baskets", "config_versions"} {
		f.breakTable(tbl, func() {
			if code := f.statusOf("/api/config"); code != http.StatusInternalServerError {
				t.Errorf("/api/config with %s failing = %d, want 500 (no partial 200)", tbl, code)
			}
		})
	}
}

// ---- app_logs masking (#8) ----

// TestAppLogsMaskedBeforeDisplay proves reviewer #8: secrets in app_logs message/fields are
// redacted before display (defence in depth — the dashboard must not trust that upstream
// never logged a secret).
func TestAppLogsMaskedBeforeDisplay(t *testing.T) {
	f := setupD(t)
	f.exec(`INSERT INTO app_logs (level, source_binary, message, fields) VALUES ('INFO','probe', ?, ?)`,
		"connecting with api_key=SECRETKEY123 password=hunter2 signature=deadbeef",
		`{"authorization":"Bearer sk-LEAKTOKEN","access_token":"AT-LEAK","refresh_token":"RT-LEAK","client_secret":"CS-LEAK","secret":"S-LEAK","token":"T-LEAK","note":"ok"}`)

	_, logs := f.get("/api/logs")
	leaks := []string{"SECRETKEY123", "hunter2", "deadbeef", "sk-LEAKTOKEN", "AT-LEAK", "RT-LEAK", "CS-LEAK", "S-LEAK", "T-LEAK"}
	var found bool
	for _, m := range logs {
		msg, _ := m["message"].(string)
		flds, _ := m["fields"].(string)
		if strings.Contains(msg, "api_key") { // our seeded row
			found = true
		}
		for _, leak := range leaks {
			if strings.Contains(msg+"\n"+flds, leak) {
				t.Errorf("app_logs display leaked %q (msg=%q fields=%q)", leak, msg, flds)
			}
		}
	}
	if !found {
		t.Fatal("seeded app_log row not returned by /api/logs")
	}
}

// TestCycleDetailAppLogsMasked proves the cycle-detail logs are masked too.
func TestCycleDetailAppLogsMasked(t *testing.T) {
	f := setupD(t)
	cyc, _ := f.seedCycle()
	f.exec(`INSERT INTO app_logs (level, source_binary, message, cycle_id) VALUES ('INFO','probe', ?, ?)`,
		"authorization=Bearer sk-CYCLELEAK token=T-CYCLELEAK", cyc)

	_, detail := f.getObj(fmt.Sprintf("/api/cycles/%d", cyc))
	logs, _ := detail["logs"].([]any)
	if len(logs) == 0 {
		t.Fatal("cycle detail has no logs")
	}
	for _, l := range logs {
		m, _ := l.(map[string]any)
		msg, _ := m["message"].(string)
		for _, leak := range []string{"sk-CYCLELEAK", "T-CYCLELEAK"} {
			if strings.Contains(msg, leak) {
				t.Errorf("cycle detail app log leaked %q: %s", leak, msg)
			}
		}
	}
}

// ---- WebSocket same-origin enforcement (#2) ----

// TestSameOriginOnly is the offline origin-policy matrix: same-origin allowed, foreign
// rejected, missing Origin allowed (non-browser), opaque/malformed rejected.
func TestSameOriginOnly(t *testing.T) {
	mk := func(host, origin string) *http.Request {
		r := httptest.NewRequest("GET", "http://"+host+"/ws", nil)
		r.Host = host
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		return r
	}
	cases := []struct {
		name         string
		host, origin string
		want         bool
	}{
		{"same-origin", "dash.local:8080", "http://dash.local:8080", true},
		{"same-origin-case-insensitive", "dash.local:8080", "HTTP://DASH.LOCAL:8080", true},
		{"foreign-host", "dash.local:8080", "http://evil.example.com", false},
		{"different-port", "dash.local:8080", "http://dash.local:9999", false},
		{"missing-origin-allowed", "dash.local:8080", "", true},
		{"opaque-null-origin", "dash.local:8080", "null", false},
	}
	for _, c := range cases {
		if got := sameOriginOnly(mk(c.host, c.origin)); got != c.want {
			t.Errorf("%s: sameOriginOnly(host=%q origin=%q) = %v, want %v", c.name, c.host, c.origin, got, c.want)
		}
	}
}

// TestWebSocketOriginEnforced proves the live upgrade rejects a foreign Origin (403),
// allows the same origin, and allows a missing Origin (non-browser client).
func TestWebSocketOriginEnforced(t *testing.T) {
	f := setupD(t)
	wsURL := "ws" + strings.TrimPrefix(f.ts.URL, "http") + "/ws"
	host := strings.TrimPrefix(f.ts.URL, "http://") // 127.0.0.1:port

	// All dials carry a valid session (origin is checked AFTER the session gate).
	// Foreign origin -> rejected with 403.
	fh := f.wsHeader(f.client)
	fh.Set("Origin", "http://evil.example.com")
	if c, resp, err := websocket.DefaultDialer.Dial(wsURL, fh); err == nil {
		c.Close()
		t.Error("foreign Origin should be rejected, but the upgrade succeeded")
	} else if resp != nil && resp.StatusCode != http.StatusForbidden {
		t.Errorf("foreign Origin rejected with status %d, want 403", resp.StatusCode)
	}

	// Same origin -> allowed.
	sh := f.wsHeader(f.client)
	sh.Set("Origin", "http://"+host)
	if c, _, err := websocket.DefaultDialer.Dial(wsURL, sh); err != nil {
		t.Errorf("same-origin should be allowed, got %v", err)
	} else {
		c.Close()
	}

	// Missing Origin -> allowed (non-browser client; documented behaviour).
	if c, _, err := websocket.DefaultDialer.Dial(wsURL, f.wsHeader(f.client)); err != nil {
		t.Errorf("missing Origin should be allowed (non-browser), got %v", err)
	} else {
		c.Close()
	}
}

// ---- WebSocket snapshot must not silently ignore DB errors (#3) ----

// TestWebSocketSnapshotDBErrors proves that if ANY snapshot sub-query fails, the socket
// sends a generic snapshot_error event — NOT a normal snapshot with silently-missing
// sections (an operator must not read an empty "open cycles" as fact on a failed query).
func TestWebSocketSnapshotDBErrors(t *testing.T) {
	for _, tbl := range []string{"cycles", "exchange_health_current", "market_regime_current", "wallet_balances_current"} {
		t.Run(tbl, func(t *testing.T) {
			f := setupIso(t)
			f.breakTable(tbl, func() {
				wsURL := "ws" + strings.TrimPrefix(f.ts.URL, "http") + "/ws"
				conn, _, err := websocket.DefaultDialer.Dial(wsURL, f.wsHeader())
				if err != nil {
					t.Fatalf("ws dial: %v", err)
				}
				defer conn.Close()
				conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				var msg map[string]any
				if err := conn.ReadJSON(&msg); err != nil {
					t.Fatalf("ws read: %v", err)
				}
				if msg["type"] != "snapshot_error" {
					t.Errorf("with %s broken, ws first message type = %v, want snapshot_error (no partial normal snapshot)", tbl, msg["type"])
				}
				// The error event must not leak raw DB details.
				if e, _ := msg["error"].(string); strings.Contains(strings.ToLower(e), "sql") || strings.Contains(e, tbl) {
					t.Errorf("snapshot_error leaked internal detail: %q", e)
				}
			})
		})
	}
}

// TestWebSocketSnapshotStaleIsBool proves the snapshot balances use the same clean bool
// `stale` representation as HTTP /api/balances.
func TestWebSocketSnapshotStaleIsBool(t *testing.T) {
	f := setupD(t)
	_, exID := f.seedCycle()
	asset := fmt.Sprintf("WSSTALE%d", time.Now().UnixNano())
	f.exec("INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total, last_seen_at) VALUES (?, ?, '2', '0', '2', NOW(6) - INTERVAL 2 HOUR)", exID, asset)

	wsURL := "ws" + strings.TrimPrefix(f.ts.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, f.wsHeader(f.client))
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var msg map[string]any
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatalf("ws read: %v", err)
	}
	bals, _ := msg["balances"].([]any)
	var seen bool
	for _, b := range bals {
		m, _ := b.(map[string]any)
		if m["asset"] == asset {
			seen = true
			if m["stale"] != true { // JSON bool decodes to Go bool
				t.Errorf("ws snapshot stale = %v (%T), want boolean true (consistent with HTTP)", m["stale"], m["stale"])
			}
		}
	}
	if !seen {
		t.Fatal("seeded stale balance not present in ws snapshot")
	}
}

// ---- WebSocket is snapshot-only (#5) ----

// TestWebSocketIgnoresCommands proves reviewer #5: the WebSocket takes NO commands. Sending
// cancel/retry/config-edit messages neither errors the connection nor mutates any state;
// the server keeps pushing read-only snapshots. There is no command handler at all.
func TestWebSocketIgnoresCommands(t *testing.T) {
	f := setupD(t)
	cyc, _ := f.seedCycle()

	var beforeState string
	f.db.QueryRow("SELECT state FROM cycles WHERE id=?", cyc).Scan(&beforeState)

	url := "ws" + strings.TrimPrefix(f.ts.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, f.wsHeader(f.client))
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	// Read the initial snapshot.
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var first map[string]any
	if err := conn.ReadJSON(&first); err != nil {
		t.Fatalf("ws initial read: %v", err)
	}

	// Fire "commands" a malicious/confused browser might send. None must be honoured.
	for _, cmd := range []string{
		fmt.Sprintf(`{"action":"cancel_order","cycle_id":%d}`, cyc),
		fmt.Sprintf(`{"action":"retry_request","cycle_id":%d}`, cyc),
		`{"action":"update_config","min_spread_bps":0}`,
		fmt.Sprintf(`{"type":"mutate","cycle_id":%d,"state":"CLOSED"}`, cyc),
	} {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(cmd)); err != nil {
			t.Fatalf("ws write: %v", err)
		}
	}

	// The socket must still deliver a snapshot (connection alive, commands ignored).
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var next map[string]any
	if err := conn.ReadJSON(&next); err != nil {
		t.Fatalf("ws read after commands: %v", err)
	}
	if next["type"] != "snapshot" {
		t.Errorf("ws message after commands = %v, want snapshot (socket is push-only)", next["type"])
	}

	// And nothing was mutated: the cycle state is unchanged.
	var afterState string
	f.db.QueryRow("SELECT state FROM cycles WHERE id=?", cyc).Scan(&afterState)
	if afterState != beforeState {
		t.Errorf("a WebSocket command mutated cycle state: %q -> %q", beforeState, afterState)
	}
}
