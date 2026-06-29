package dashboard

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func (f *dfix) seedExchangeC() (int64, string) {
	f.t.Helper()
	dseq++
	code := fmt.Sprintf("cax%d_%d", time.Now().UnixNano(), dseq)
	r := f.exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'CA', 1)", code)
	id, _ := r.LastInsertId()
	return id, code
}

func TestCredentialCreateAuthRequired(t *testing.T) {
	f := setupD(t)
	_, code := f.seedExchangeC()
	body := fmt.Sprintf(`{"exchange_code":%q,"label":"default","api_key":"k","api_secret":"s","reason":"x"}`, code)
	if c, _ := f.post("/api/credentials", "", body); c != http.StatusUnauthorized {
		t.Errorf("no token = %d, want 401", c)
	}
	if c, _ := f.post("/api/credentials", "bad", body); c != http.StatusUnauthorized {
		t.Errorf("bad token = %d, want 401", c)
	}
	// Wrong roles (incl. config_operator and reconcile_operator) must be 403.
	for _, role := range []string{RoleViewer, RoleConfigOperator, RoleReconcileOperator} {
		if c, _ := f.post("/api/credentials", f.token(role), body); c != http.StatusForbidden {
			t.Errorf("%s = %d, want 403", role, c)
		}
	}
}

func TestCredentialCreateViaHTTPNoSecretsReturned(t *testing.T) {
	f := setupD(t)
	exID, code := f.seedExchangeC()
	tok := f.token(RoleCredentialOperator)
	const plainKey, plainSec = "HTTP-PLAINKEY-111", "HTTP-PLAINSEC-222"
	body := fmt.Sprintf(`{"exchange_code":%q,"label":"default","api_key":%q,"api_secret":%q,"reason":"provision via http"}`, code, plainKey, plainSec)
	c, obj := f.post("/api/credentials", tok, body)
	if c != http.StatusOK {
		t.Fatalf("create = %d (%v)", c, obj)
	}
	if _, ok := obj["credential_id"]; !ok {
		t.Error("response should return the new credential_id")
	}
	// The response must NOT echo any plaintext or blob.
	full := fmt.Sprintf("%v", obj)
	for _, leak := range []string{plainKey, plainSec, "encrypted", "api_secret", "api_key"} {
		if strings.Contains(full, leak) {
			t.Errorf("create response leaked %q", leak)
		}
	}
	// The DB stored ciphertext (never the plaintext).
	var blob []byte
	f.db.QueryRow("SELECT encrypted_api_key FROM exchange_credentials WHERE exchange_id=? ORDER BY id DESC LIMIT 1", exID).Scan(&blob)
	if len(blob) == 0 || strings.Contains(string(blob), plainKey) {
		t.Error("api key not stored as ciphertext")
	}
}

func TestCredentialCreateInvalidReturns400(t *testing.T) {
	f := setupD(t)
	_, code := f.seedExchangeC()
	tok := f.token(RoleAdmin)
	// missing api_secret -> validation 400
	body := fmt.Sprintf(`{"exchange_code":%q,"label":"l","api_key":"k","reason":"x"}`, code)
	if c, _ := f.post("/api/credentials", tok, body); c != http.StatusBadRequest {
		t.Errorf("invalid create = %d, want 400", c)
	}
}

func TestCredentialDisableViaHTTP(t *testing.T) {
	f := setupD(t)
	_, code := f.seedExchangeC()
	tok := f.token(RoleCredentialOperator)
	cc, obj := f.post("/api/credentials", tok, fmt.Sprintf(`{"exchange_code":%q,"label":"default","api_key":"k","api_secret":"s","reason":"create"}`, code))
	if cc != http.StatusOK {
		t.Fatalf("create = %d (%v)", cc, obj)
	}
	id := int64(obj["credential_id"].(float64))
	c, _ := f.post(fmt.Sprintf("/api/credentials/%d/disable", id), tok, `{"reason":"compromised"}`)
	if c != http.StatusOK {
		t.Fatalf("disable = %d", c)
	}
	var enabled int
	var status string
	f.db.QueryRow("SELECT enabled, status FROM exchange_credentials WHERE id=?", id).Scan(&enabled, &status)
	if enabled != 0 || status != "disabled" {
		t.Errorf("after disable = enabled:%d status:%s, want 0/disabled", enabled, status)
	}
}

func TestCredentialAuditEndpointNoSecrets(t *testing.T) {
	f := setupD(t)
	_, code := f.seedExchangeC()
	tok := f.token(RoleCredentialOperator)
	f.post("/api/credentials", tok, fmt.Sprintf(`{"exchange_code":%q,"label":"default","api_key":"SEEKRET","api_secret":"SEEKRET2","reason":"x"}`, code))
	c, arr := f.get("/api/credentials/audit?limit=500")
	if c != http.StatusOK {
		t.Fatalf("audit = %d", c)
	}
	full := fmt.Sprintf("%v", arr)
	for _, leak := range []string{"SEEKRET", "encrypted", "api_secret", "api_key", "passphrase"} {
		if strings.Contains(full, leak) {
			t.Errorf("credential audit leaked %q", leak)
		}
	}
}
