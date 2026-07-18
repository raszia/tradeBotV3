package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/migrate"
)

const credTestMasterKey = "dashboard-credential-test-master-key"

// setupCredDash builds a dashboard on a throwaway DB WITH a master key (so credential provisioning
// is enabled) and returns the fixture (admin logged in). masterKey="" disables credential writes.
func setupCredDash(t *testing.T, masterKey string) *isoFix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the dashboard credential test")
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
	name := fmt.Sprintf("v3tb_creddash_%d", time.Now().UnixNano())
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
	srv := New(db, nil, Config{DefaultLimit: 50, MaxLimit: 100, StaleBalanceAge: time.Hour, SessionTTL: time.Hour, MasterKey: masterKey})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); db.Close() })
	f := &isoFix{t: t, db: db, srv: srv, ts: ts}
	if _, err := srv.CreateUser(context.Background(), "credadmin", "password123", RoleAdmin); err != nil {
		t.Fatalf("create admin: %v", err)
	}
	jar, _ := cookiejar.New(nil)
	f.client = &http.Client{Jar: jar}
	resp, err := f.client.Post(ts.URL+"/login", "application/json", strings.NewReader(`{"username":"credadmin","password":"password123"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return f
}

func credEndpoints() []struct{ method, path, body string } {
	return []struct{ method, path, body string }{
		{"GET", "/api/credentials", ""},
		{"GET", "/api/credentials/audit", ""},
		{"POST", "/api/credentials", `{"exchange_code":"x","api_key":"k","api_secret":"s","reason":"r"}`},
		{"POST", "/api/credentials/1/validate", `{"reason":"r"}`},
		{"POST", "/api/credentials/1/activate-or-rotate", `{"reason":"r"}`},
		{"POST", "/api/credentials/1/disable", `{"reason":"r"}`},
	}
}

// Req 10: every credential endpoint uses the session-cookie model (401 without a session) and the
// exact-set capability (403 for viewer/config_operator).
func TestCredentialEndpointsRequireSessionAndRole(t *testing.T) {
	f := setupIso(t) // no master key needed to test auth (checked before the write-readiness 503)
	anon := &http.Client{}
	for _, e := range credEndpoints() {
		if code := f.reqWith(anon, e.method, e.path, e.body); code != http.StatusUnauthorized {
			t.Errorf("%s %s without session = %d, want 401", e.method, e.path, code)
		}
	}
	for _, role := range []string{RoleViewer, RoleConfigOperator} {
		c := f.loginRole(role)
		for _, e := range credEndpoints() {
			if code := f.reqWith(c, e.method, e.path, e.body); code != http.StatusForbidden {
				t.Errorf("role %s: %s %s = %d, want 403", role, e.method, e.path, code)
			}
		}
	}
}

// Req: with no valid master key, credential WRITES are disabled (503); reads still work.
func TestCredentialWritesDisabledWithoutMasterKey(t *testing.T) {
	f := setupCredDash(t, "") // no master key
	writes := []struct{ path, body string }{
		{"/api/credentials", `{"exchange_code":"x","api_key":"k","api_secret":"s","reason":"r"}`},
		{"/api/credentials/1/validate", `{"reason":"r"}`},
		{"/api/credentials/1/activate-or-rotate", `{"reason":"r"}`},
		{"/api/credentials/1/disable", `{"reason":"r"}`},
	}
	for _, wr := range writes {
		if code := f.reqWith(f.client, "POST", wr.path, wr.body); code != http.StatusServiceUnavailable {
			t.Errorf("POST %s without a master key = %d, want 503", wr.path, code)
		}
	}
	// A read still works.
	if code := f.reqWith(f.client, "GET", "/api/credentials", ""); code != http.StatusOK {
		t.Errorf("GET /api/credentials = %d, want 200 (reads work without a master key)", code)
	}
}

// Req 1 + 9: a created credential is inactive/unknown and NO secret is echoed or stored in plaintext.
func TestCredentialCreateInactiveNoSecret(t *testing.T) {
	f := setupCredDash(t, credTestMasterKey)
	ex := lastID(f.exec("INSERT INTO exchanges (code, name, enabled) VALUES ('cx', 'X', 1)"))
	body := `{"exchange_code":"cx","label":"default","api_key":"PLAINTEXT-KEY","api_secret":"PLAINTEXT-SEC","reason":"provision"}`
	resp, err := f.client.Post(f.ts.URL+"/api/credentials", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create = %d", resp.StatusCode)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	// Response has no secret and reports inactive/unknown.
	raw := fmt.Sprint(out)
	if strings.Contains(raw, "PLAINTEXT") {
		t.Errorf("create response leaked a secret: %v", out)
	}
	if fmt.Sprint(out["status"]) != "unknown" || fmt.Sprint(out["enabled"]) != "false" {
		t.Errorf("create response = %v, want status unknown + enabled false", out)
	}
	id := int64(out["credential_id"].(float64))
	var en int
	var st string
	var encKey []byte
	f.db.QueryRow("SELECT enabled, status, encrypted_api_key FROM exchange_credentials WHERE id=? AND exchange_id=?", id, ex).Scan(&en, &st, &encKey)
	if en != 0 || st != "unknown" {
		t.Errorf("stored cred = enabled:%d status:%s, want 0/unknown", en, st)
	}
	if strings.Contains(string(encKey), "PLAINTEXT") {
		t.Error("stored encrypted_api_key contains the plaintext")
	}
	// The list endpoint never returns encrypted blobs or plaintext.
	lresp, lerr := f.client.Get(f.ts.URL + "/api/credentials")
	if lerr != nil {
		t.Fatal(lerr)
	}
	defer lresp.Body.Close()
	var list []map[string]any
	json.NewDecoder(lresp.Body).Decode(&list)
	for _, row := range list {
		for k := range row {
			if strings.Contains(strings.ToLower(k), "encrypted") || strings.Contains(strings.ToLower(k), "api_key") || strings.Contains(strings.ToLower(k), "secret") {
				t.Errorf("list exposed a secret column %q", k)
			}
		}
	}
}
