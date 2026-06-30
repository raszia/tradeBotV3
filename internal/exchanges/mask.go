package exchanges

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// Secret masking for raw API request/response logging. MANDATORY: API keys,
// secrets, passphrases, Authorization headers, signatures, tokens, and
// cookie/session values must never reach the api_call_logs table or any other
// output. These helpers are applied by the IO logger before anything is stored.
//
// The masking is deliberately conservative (over-masks rather than under-masks):
// a logged value is only useful for debugging shapes, never for replaying auth.

const maskPlaceholder = "***"

// sensitiveExact matches a header/field name exactly (case-insensitive).
var sensitiveExact = map[string]bool{
	"key": true, "apikey": true, "api_key": true, "api-key": true,
	"secret": true, "apisecret": true, "api_secret": true,
	"passphrase": true, "password": true, "pass": true,
	"sign": true, "sig": true, "signature": true,
	"token": true, "auth": true, "authorization": true,
	"cookie": true, "otp": true, "totp": true, "mfa": true,
	// Bearer/JWT auth tokens — Bitpin's auth endpoint returns {"access":...,"refresh":...},
	// and other venues use accessToken/refreshToken/jwt/bearer. All are secrets.
	"access": true, "refresh": true, "accesstoken": true, "refreshtoken": true,
	"jwt": true, "bearer": true,
}

// sensitiveContains matches if the lower-cased name contains any substring.
var sensitiveContains = []string{
	"secret", "passphrase", "signature", "password",
	"api_key", "api-key", "apikey", "authorization", "auth_token", "auth-token",
	"access_token", "accesstoken", "refresh_token", "private_key", "privatekey",
	"session", "cookie", "x-api", "x-mbx-apikey", "otp", "totp",
}

// sensitiveHeaderNames are HTTP header names whose value is always masked.
var sensitiveHeaderNames = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
	"set-cookie":          true,
	"x-api-key":           true,
	"x-api-secret":        true,
	"x-mbx-apikey":        true, // Binance
	"x-signature":         true,
	"x-auth-token":        true,
	"x-csrf-token":        true,
	"x-totp":              true,
	"token":               true,
	"apikey":              true,
}

// IsSensitiveKey reports whether a header/JSON/form key name is sensitive.
func IsSensitiveKey(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if sensitiveExact[lower] {
		return true
	}
	for _, sub := range sensitiveContains {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
}

func isSensitiveHeader(name string) bool {
	if sensitiveHeaderNames[strings.ToLower(name)] {
		return true
	}
	return IsSensitiveKey(name)
}

// MaskHeaders returns a copy of h as a map with sensitive values masked. Safe to
// store as JSON in api_call_logs.request_headers / response_headers.
func MaskHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vals := range h {
		if isSensitiveHeader(k) {
			out[k] = maskPlaceholder
			continue
		}
		out[k] = strings.Join(vals, ", ")
	}
	return out
}

// MaskURL masks sensitive query parameters (e.g. a signed `signature=` param) so
// a logged URL cannot leak auth material.
func MaskURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return maskNonJSON(raw)
	}
	if u.RawQuery == "" {
		return raw
	}
	q := u.Query()
	changed := false
	for k := range q {
		if IsSensitiveKey(k) {
			q.Set(k, maskPlaceholder)
			changed = true
		}
	}
	if changed {
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// MaskBody masks secrets in a request/response body. JSON bodies are parsed and
// masked precisely (recursively); non-JSON bodies fall back to regex masking of
// "key":"value" and key=value pairs.
func MaskBody(body string) string {
	if body == "" {
		return ""
	}
	trimmed := strings.TrimSpace(body)
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		var v any
		if err := json.Unmarshal([]byte(trimmed), &v); err == nil {
			if b, err := json.Marshal(maskJSON(v)); err == nil {
				return string(b)
			}
		}
	}
	return maskNonJSON(body)
}

// maskJSON recursively replaces the values of sensitive keys with the
// placeholder, preserving structure.
func maskJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if IsSensitiveKey(k) {
				t[k] = maskPlaceholder
			} else {
				t[k] = maskJSON(val)
			}
		}
		return t
	case []any:
		for i := range t {
			t[i] = maskJSON(t[i])
		}
		return t
	default:
		return v
	}
}

// sensitiveTokenAlternation is the regex alternation of sensitive key fragments
// used by both fallback maskers. It includes bare "key"/"sign", so this path
// over-masks (e.g. "keyword") rather than risk leaking — acceptable for raw logs.
const sensitiveTokenAlternation = `secret|signature|sign|password|api[_-]?key|apikey|passphrase|authorization|auth|token|access|refresh|jwt|bearer|otp|totp|cookie|key`

var (
	// jsonPairRe matches "sensitiveKey": "value" (string values) in JSON-ish text
	// (used only when the body is not valid JSON; valid JSON is masked precisely).
	jsonPairRe = regexp.MustCompile(`(?i)("[a-z0-9_\-]*(?:` + sensitiveTokenAlternation + `)[a-z0-9_\-]*"\s*:\s*)"[^"]*"`)
	// formPairRe matches key=value form/query pairs whose key is sensitive.
	formPairRe = regexp.MustCompile(`(?i)([a-z0-9_\-]*(?:` + sensitiveTokenAlternation + `)[a-z0-9_\-]*)=([^&\s"]+)`)
)

func maskNonJSON(s string) string {
	s = jsonPairRe.ReplaceAllString(s, `$1"`+maskPlaceholder+`"`)
	s = formPairRe.ReplaceAllString(s, `$1=`+maskPlaceholder)
	return s
}

// urlInTextRe finds http(s) URLs embedded in free text (e.g. a transport error string).
var urlInTextRe = regexp.MustCompile(`https?://[^\s"'<>]+`)

// MaskErrorText redacts secrets that can appear in transport/error strings before they are
// stored (api_call_logs.error). Go's *url.Error / net errors frequently embed the full
// request URL — including signed query params — and arbitrary "key=value" fragments. We mask
// any embedded URL's sensitive query params (via MaskURL, which itself falls back to regex
// masking on a parse error) and then run the same key=value / "key":"value" fallback maskers
// over the whole string. Never store a raw err.Error() — route it through here first.
func MaskErrorText(s string) string {
	if s == "" {
		return ""
	}
	s = urlInTextRe.ReplaceAllStringFunc(s, MaskURL)
	return maskNonJSON(s)
}
