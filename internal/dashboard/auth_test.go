package dashboard

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

// ---- offline password/role unit tests ----

func TestPasswordHashRoundTrip(t *testing.T) {
	h, err := hashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(h, "$pbkdf2-sha256$") {
		t.Errorf("hash not self-describing pbkdf2: %q", h)
	}
	if strings.Contains(h, "correct horse battery") {
		t.Error("hash must not contain the plaintext password")
	}
	if !verifyPassword(h, "correct horse battery") {
		t.Error("correct password should verify")
	}
	if verifyPassword(h, "wrong password") {
		t.Error("wrong password must not verify")
	}
	if _, err := hashPassword("short"); err == nil {
		t.Error("too-short password should be rejected")
	}
}

func TestRoleRanking(t *testing.T) {
	if !roleAtLeast(RoleAdmin, RoleConfigOperator) || !roleAtLeast(RoleConfigOperator, RoleViewer) {
		t.Error("higher roles must satisfy lower minimums")
	}
	if roleAtLeast(RoleViewer, RoleConfigOperator) || roleAtLeast(RoleConfigOperator, RoleAdmin) {
		t.Error("lower roles must NOT satisfy higher minimums")
	}
	if roleAtLeast("nonsense", RoleViewer) {
		t.Error("unknown role must satisfy nothing")
	}
}

// ---- gated login/session tests ----

func TestLoginFlow(t *testing.T) {
	f := setupD(t)
	user, pass := f.createUser(RoleViewer)

	// Valid login succeeds.
	c := f.newClient()
	if code, body := f.login(c, user, pass); code != http.StatusOK || body["role"] != RoleViewer {
		t.Fatalf("valid login = %d %v, want 200 viewer", code, body)
	}
	// Wrong password -> 401.
	if code, _ := f.login(f.newClient(), user, "not-the-password"); code != http.StatusUnauthorized {
		t.Errorf("wrong password = %d, want 401", code)
	}
	// Unknown user -> 401.
	if code, _ := f.login(f.newClient(), "no-such-user-xyz", pass); code != http.StatusUnauthorized {
		t.Errorf("unknown user = %d, want 401", code)
	}
}

func TestDisabledUserCannotLogin(t *testing.T) {
	f := setupD(t)
	user, pass := f.createUser(RoleConfigOperator)
	f.exec("UPDATE dashboard_users SET active=0 WHERE username=?", user)
	if code, _ := f.login(f.newClient(), user, pass); code != http.StatusUnauthorized {
		t.Errorf("disabled user login = %d, want 401", code)
	}
}

func TestLogoutInvalidatesSession(t *testing.T) {
	f := setupD(t)
	// f.client is a logged-in admin. It can read.
	if code, _ := f.getObjWith(f.client, "/api/me"); code != http.StatusOK {
		t.Fatalf("authenticated /api/me = %d, want 200", code)
	}
	// Log out, then the same client (cookie now revoked server-side) is rejected.
	resp, err := f.client.Post(f.ts.URL+"/logout", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if code, _ := f.getObjWith(f.client, "/api/me"); code != http.StatusUnauthorized {
		t.Errorf("/api/me after logout = %d, want 401 (session invalidated)", code)
	}
}

func TestProtectedRoutesRequireLogin(t *testing.T) {
	f := setupD(t)
	anon := f.newClient() // no session
	for _, p := range []string{"/", "/api/cycles/open", "/api/config", "/api/balances", "/api/audit", "/api/me"} {
		if code, _ := f.getObjWith(anon, p); code != http.StatusUnauthorized {
			t.Errorf("anonymous GET %s = %d, want 401", p, code)
		}
	}
	// A mutation route without login is also 401 (before any role check).
	if code, _ := f.postJSON(anon, "/api/config/symbol/1", `{"expected_config_version":1,"reason":"x","min_spread_bps":10}`); code != http.StatusUnauthorized {
		t.Errorf("anonymous POST edit = %d, want 401", code)
	}
}

func TestWebSocketRequiresLogin(t *testing.T) {
	f := setupD(t)
	wsURL := "ws" + strings.TrimPrefix(f.ts.URL, "http") + "/ws"
	// No session cookie -> upgrade rejected with 401.
	if c, resp, err := websocket.DefaultDialer.Dial(wsURL, nil); err == nil {
		c.Close()
		t.Error("WebSocket without login should be rejected")
	} else if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("WebSocket without login = %d, want 401", resp.StatusCode)
	}
	// With a valid session it connects.
	if c, _, err := websocket.DefaultDialer.Dial(wsURL, f.wsHeader(f.client)); err != nil {
		t.Errorf("authenticated WebSocket dial = %v, want success", err)
	} else {
		c.Close()
	}
}

func TestViewerCanReadButNotEdit(t *testing.T) {
	f := setupD(t)
	viewer := f.loginNewUser("viewer", RoleViewer)
	// Viewer can read.
	if code, _ := f.getObjWith(viewer, "/api/config"); code != http.StatusOK {
		t.Errorf("viewer GET /api/config = %d, want 200", code)
	}
	// Viewer cannot edit (403 from the role gate, before any handler work).
	if code, _ := f.postJSON(viewer, "/api/config/symbol/1", `{"expected_config_version":1,"reason":"x","min_spread_bps":10}`); code != http.StatusForbidden {
		t.Errorf("viewer POST edit = %d, want 403", code)
	}
}

// TestSecretsNotStoredPlaintext proves passwords and session tokens are never stored raw.
func TestSecretsNotStoredPlaintext(t *testing.T) {
	f := setupD(t)
	user, pass := f.createUser(RoleViewer)

	var ph string
	f.db.QueryRow("SELECT password_hash FROM dashboard_users WHERE username=?", user).Scan(&ph)
	if strings.Contains(ph, pass) || !strings.HasPrefix(ph, "$pbkdf2-sha256$") {
		t.Errorf("password stored insecurely: %q", ph)
	}

	// Log in, grab the raw cookie, and prove the DB stores only its sha256 hash.
	c := f.newClient()
	if code, _ := f.login(c, user, pass); code != http.StatusOK {
		t.Fatal("login failed")
	}
	u, _ := url.Parse(f.ts.URL)
	var raw string
	for _, ck := range c.Jar.Cookies(u) {
		if ck.Name == sessionCookie {
			raw = ck.Value
		}
	}
	if raw == "" {
		t.Fatal("no session cookie set")
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM dashboard_sessions WHERE token_hash=?", raw).Scan(&n)
	if n != 0 {
		t.Error("dashboard_sessions stores the RAW token — must store only its hash")
	}
	f.db.QueryRow("SELECT COUNT(*) FROM dashboard_sessions WHERE token_hash=?", tokenHash(raw)).Scan(&n)
	if n != 1 {
		t.Errorf("session not found by token hash (got %d)", n)
	}
}
