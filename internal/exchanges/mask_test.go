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
