package exchanges

import (
	"net/http"
	"strings"
	"testing"
)

// secrets that must never survive masking, reused across tests.
var leakValues = []string{
	"AKIAEXAMPLEKEY123", "supersecretvalue", "passphrase-xyz",
	"deadbeefsignature", "Bearer-token-abc", "sessioncookieval",
	// Bitpin auth-token values + error-text secrets (PR4 correction).
	"ACCESS-TOKEN-1", "REFRESH-TOKEN-1",
	"SECRET-TOKEN", "SECRET-KEY", "SECRET-SIGNATURE",
}

func assertNoLeak(t *testing.T, where, out string) {
	t.Helper()
	for _, s := range leakValues {
		if strings.Contains(out, s) {
			t.Errorf("%s leaked secret %q in: %s", where, s, out)
		}
	}
}

func TestMaskHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer-token-abc")
	h.Set("X-API-Key", "AKIAEXAMPLEKEY123")
	h.Set("X-MBX-APIKEY", "AKIAEXAMPLEKEY123")
	h.Set("Cookie", "sessioncookieval")
	h.Set("X-Signature", "deadbeefsignature")
	h.Set("Content-Type", "application/json")

	masked := MaskHeaders(h)
	assertNoLeak(t, "MaskHeaders", strings.Join(values(masked), " "))
	if masked["Content-Type"] != "application/json" {
		t.Errorf("benign header altered: %q", masked["Content-Type"])
	}
	for _, k := range []string{"Authorization", "X-Api-Key", "X-Mbx-Apikey", "Cookie", "X-Signature"} {
		if v, ok := masked[k]; ok && v != "***" {
			t.Errorf("header %q = %q, want ***", k, v)
		}
	}
}

func TestMaskBodyJSON(t *testing.T) {
	body := `{"api_key":"AKIAEXAMPLEKEY123","apiSecret":"supersecretvalue",` +
		`"passphrase":"passphrase-xyz","nested":{"signature":"deadbeefsignature","amount":"1.5"},` +
		`"side":"buy","symbol":"BTC/USDT"}`
	out := MaskBody(body)
	assertNoLeak(t, "MaskBody(json)", out)
	// Non-secret fields are preserved.
	for _, keep := range []string{"buy", "BTC/USDT", "1.5", "amount", "side"} {
		if !strings.Contains(out, keep) {
			t.Errorf("MaskBody(json) dropped non-secret %q: %s", keep, out)
		}
	}
}

func TestMaskBodyForm(t *testing.T) {
	body := "apiKey=AKIAEXAMPLEKEY123&signature=deadbeefsignature&symbol=BTCUSDT&amount=1.5&token=Bearer-token-abc"
	out := MaskBody(body)
	assertNoLeak(t, "MaskBody(form)", out)
	if !strings.Contains(out, "symbol=BTCUSDT") || !strings.Contains(out, "amount=1.5") {
		t.Errorf("MaskBody(form) dropped non-secret pairs: %s", out)
	}
}

func TestMaskBodyMalformedJSONFallback(t *testing.T) {
	// Not valid JSON (trailing junk) — must fall back to regex masking, not leak.
	body := `{"secret":"supersecretvalue","token":"Bearer-token-abc" TRAILING`
	out := MaskBody(body)
	assertNoLeak(t, "MaskBody(malformed)", out)
}

func TestMaskURL(t *testing.T) {
	raw := "https://api.binance.com/api/v3/order?symbol=BTCUSDT&timestamp=123&signature=deadbeefsignature&apiKey=AKIAEXAMPLEKEY123"
	out := MaskURL(raw)
	assertNoLeak(t, "MaskURL", out)
	if !strings.Contains(out, "symbol=BTCUSDT") || !strings.Contains(out, "timestamp=123") {
		t.Errorf("MaskURL dropped non-secret params: %s", out)
	}
}

func TestIsSensitiveKey(t *testing.T) {
	sensitive := []string{"api_key", "apiKey", "API-KEY", "secret", "passphrase", "signature",
		"Authorization", "token", "access_token", "refresh_token", "cookie", "x-api-key", "totp", "key", "sign"}
	for _, s := range sensitive {
		if !IsSensitiveKey(s) {
			t.Errorf("IsSensitiveKey(%q) = false, want true", s)
		}
	}
	benign := []string{"symbol", "amount", "price", "side", "quantity", "timestamp", "nonce", "type"}
	for _, b := range benign {
		if IsSensitiveKey(b) {
			t.Errorf("IsSensitiveKey(%q) = true, want false", b)
		}
	}
}

func values(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// TestMaskCoversAdapterSecretFieldNames (PR26 audit) asserts every secret field name an
// adapter actually places in a request body/header/query is recognized as sensitive — a
// gap would leak the secret into api_call_logs. Includes bitpin's "secret_key" body field.
func TestMaskCoversAdapterSecretFieldNames(t *testing.T) {
	for _, name := range []string{
		"api_key", "apikey", "apiKey", "API-KEY", "api-key",
		"secret", "secret_key", "api_secret", "apiSecret", "apisecret",
		"passphrase", "password", "token", "signature", "sign",
		"authorization", "x-api-key", "x-mbx-apikey",
	} {
		if !IsSensitiveKey(name) {
			t.Errorf("IsSensitiveKey(%q) = false — this adapter secret field would leak", name)
		}
	}
	// The bitpin auth body uses BOTH "api_key" AND "secret_key"; both must be masked.
	body := MaskBody(`{"api_key":"AK-LEAK-123","secret_key":"SK-LEAK-456"}`)
	if strings.Contains(body, "AK-LEAK-123") || strings.Contains(body, "SK-LEAK-456") {
		t.Errorf("MaskBody leaked a bitpin auth secret: %s", body)
	}
}

// TestMaskBitpinAuthTokens (PR4 correction) — Bitpin's auth endpoint returns
// {"access":...,"refresh":...}; both are bearer secrets and must be redacted from a logged
// response body. accessToken/refreshToken/jwt/bearer variants must be too.
func TestMaskBitpinAuthTokens(t *testing.T) {
	out := MaskBody(`{"access":"ACCESS-TOKEN-1","refresh":"REFRESH-TOKEN-1"}`)
	assertNoLeak(t, "MaskBody(bitpin access/refresh)", out)

	out2 := MaskBody(`{"accessToken":"ACCESS-TOKEN-1","refreshToken":"REFRESH-TOKEN-1","jwt":"ACCESS-TOKEN-1","bearer":"REFRESH-TOKEN-1"}`)
	assertNoLeak(t, "MaskBody(token variants)", out2)

	for _, k := range []string{"access", "refresh", "accessToken", "refreshToken", "jwt", "bearer"} {
		if !IsSensitiveKey(k) {
			t.Errorf("IsSensitiveKey(%q) = false — Bitpin token field would leak", k)
		}
	}
	// Non-JSON fallback masks them too (key=value form).
	assertNoLeak(t, "maskNonJSON(form tokens)", MaskBody(`access=ACCESS-TOKEN-1&refresh=REFRESH-TOKEN-1`))
}

// TestMaskErrorText (PR4 correction) — a transport error string can embed a signed URL and
// raw key=value secrets; MaskErrorText must redact them before they reach api_call_logs.error.
func TestMaskErrorText(t *testing.T) {
	// Plain key=value fragment (the reviewer's required case).
	assertNoLeak(t, "MaskErrorText(form)",
		MaskErrorText("token=SECRET-TOKEN&apiKey=SECRET-KEY&signature=SECRET-SIGNATURE"))

	// A Go transport error embedding a signed URL.
	urlErr := `Get "https://api.exir.io/v1/order?apiKey=SECRET-KEY&signature=SECRET-SIGNATURE&symbol=BTCIRT": dial tcp: i/o timeout`
	out := MaskErrorText(urlErr)
	assertNoLeak(t, "MaskErrorText(signed url)", out)
	// Non-secret context is preserved (so the log is still useful).
	if !strings.Contains(out, "BTCIRT") || !strings.Contains(out, "i/o timeout") {
		t.Errorf("MaskErrorText over-masked useful context: %s", out)
	}
	if MaskErrorText("") != "" {
		t.Error("MaskErrorText(\"\") must stay empty")
	}
}
