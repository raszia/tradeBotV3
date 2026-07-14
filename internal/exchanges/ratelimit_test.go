package exchanges

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"v3TradeBot/internal/execution"
)

// PR20 correction #3/#6 — comprehensive rate-limit detection. Offline (pure functions fed
// venue-documented statuses/bodies/headers; the exchange-specific shapes are copied from the
// owner's proven iranArb rules).

// --- shared parsers -------------------------------------------------------------------------

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	if d := ParseRetryAfter("29", now); d != 29*time.Second {
		t.Errorf("delta-seconds = %v, want 29s", d)
	}
	if d := ParseRetryAfter(now.Add(90*time.Second).Format(http.TimeFormat), now); d < 89*time.Second || d > 91*time.Second {
		t.Errorf("http-date = %v, want ~90s", d)
	}
	// Malformed / non-positive / absent → 0 (caller applies configured fallback).
	for _, v := range []string{"", "garbage", "-5", "0", "12x"} {
		if d := ParseRetryAfter(v, now); d != 0 {
			t.Errorf("ParseRetryAfter(%q) = %v, want 0", v, d)
		}
	}
	// Pathological value is capped.
	if d := ParseRetryAfter("999999", now); d != 15*time.Minute {
		t.Errorf("huge retry-after = %v, want capped 15m", d)
	}
}

func TestRetryAfterFromHeaders(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := http.Header{}
	h.Set("Retry-After", "42")
	if d, src, ok := RetryAfterFromHeaders(h, now); !ok || d != 42*time.Second || src != RLSourceHeader {
		t.Errorf("Retry-After header = (%v,%s,%t)", d, src, ok)
	}
	// remaining=0 + delta reset.
	h = http.Header{}
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("X-RateLimit-Reset", "30")
	if d, _, ok := RetryAfterFromHeaders(h, now); !ok || d != 30*time.Second {
		t.Errorf("remaining=0 + delta reset = (%v,%t), want 30s", d, ok)
	}
	// remaining=0 + unix-timestamp reset.
	h.Set("X-RateLimit-Reset", "1800000060")
	if d, _, ok := RetryAfterFromHeaders(h, now); !ok || d != 60*time.Second {
		t.Errorf("remaining=0 + unix reset = (%v,%t), want 60s", d, ok)
	}
	// remaining=0 with malformed reset: still a signal, no duration.
	h.Set("X-RateLimit-Reset", "soon")
	if d, _, ok := RetryAfterFromHeaders(h, now); !ok || d != 0 {
		t.Errorf("remaining=0 malformed reset = (%v,%t), want (0,true)", d, ok)
	}
	// remaining > 0 → NOT a signal.
	h.Set("X-RateLimit-Remaining", "7")
	if _, _, ok := RetryAfterFromHeaders(h, now); ok {
		t.Error("remaining=7 must not be a rate-limit signal")
	}
}

// TestConservativeTextFallbackAvoidsFalsePositives: harmless business text containing "limit"
// must NEVER classify as a rate limit; only unambiguous throttle phrases do.
func TestConservativeTextFallbackAvoidsFalsePositives(t *testing.T) {
	notRateLimit := []string{
		"limit order rejected: price above daily price band",
		"order limit price invalid",
		"minimum order limit is 100000 IRT",
		"insufficient balance",
		"market order size below limit",
	}
	for _, s := range notRateLimit {
		if LooksLikeRateLimitMessage(s) {
			t.Errorf("false positive on harmless text: %q", s)
		}
	}
	isRateLimit := []string{
		"Too many requests",
		"rate limit exceeded",
		"Rate-Limit reached for endpoint",
		"Request was throttled. Expected available in 29 seconds.",
		"TooManyRequests",
	}
	for _, s := range isRateLimit {
		if !LooksLikeRateLimitMessage(s) {
			t.Errorf("missed throttle phrase: %q", s)
		}
	}
}

// --- nobitex (iranArb-proven contract) --------------------------------------------------------

// TestNobitex429WithBackoff: the documented 429 envelope (iranArb wire shape,
// {"status":"failed","code":"TooManyRequests","backOff":698}) yields CatRateLimit with the
// backOff SECONDS as RetryAfter and a DEFINITE pre-execution rejection.
func TestNobitex429WithBackoff(t *testing.T) {
	body := []byte(`{"status":"failed","message":"msg","code":"TooManyRequests","backOff":698,"limit":100}`)
	err := nobitexHTTPError("PlaceOrder", http.StatusTooManyRequests, body)
	rl := RateLimitOf(err)
	if rl == nil {
		t.Fatal("429 not detected as rate limit")
	}
	if !errors.Is(err, execution.ErrRateLimited) {
		t.Error("sentinel not wrapped")
	}
	if rl.RetryAfter != 698*time.Second {
		t.Errorf("RetryAfter = %v, want 698s (backOff seconds — iranArb-verified unit)", rl.RetryAfter)
	}
	if !rl.DefiniteRejection {
		t.Error("documented TooManyRequests envelope must be a DEFINITE pre-execution rejection")
	}
	if rl.Code != "TooManyRequests" {
		t.Errorf("code = %q", rl.Code)
	}
}

// TestNobitexHTTP200RateLimitBody: an HTTP-200 response whose body carries the documented
// throttle envelope is a RATE LIMIT — never CatBadRequest (which would fail the cycle) and
// never a success.
func TestNobitexHTTP200RateLimitBody(t *testing.T) {
	body := []byte(`{"status":"failed","code":"TooManyRequests","backOff":60}`)
	err := nobitexBusinessError("PlaceOrder", "failed", "TooManyRequests", body)
	rl := RateLimitOf(err)
	if rl == nil {
		t.Fatal("HTTP-200 throttle body not detected as rate limit")
	}
	var apiErr *NormalizedAPIError
	if !errors.As(err, &apiErr) || apiErr.Category != CatRateLimit {
		t.Fatalf("category = %v, want CatRateLimit", apiErr.Category)
	}
	if apiErr.StatusCode != http.StatusOK {
		t.Errorf("status recorded = %d, want 200 (the whole point)", apiErr.StatusCode)
	}
	if rl.RetryAfter != 60*time.Second || !rl.DefiniteRejection || rl.Source != RLSourceCode {
		t.Errorf("metadata = %+v, want 60s/definite/code", rl)
	}
}

// TestNobitexUnrelated200BusinessErrorNotMisclassified: an ordinary failed envelope (e.g.
// insufficient balance) stays its own category — never a rate limit.
func TestNobitexUnrelated200BusinessErrorNotMisclassified(t *testing.T) {
	err := nobitexBusinessError("PlaceOrder", "failed", "InsufficientBalance", []byte(`{"status":"failed","code":"InsufficientBalance"}`))
	if RateLimitOf(err) != nil {
		t.Error("insufficient-balance envelope misclassified as rate limit")
	}
	if !errors.Is(err, execution.ErrInsufficientBalance) {
		t.Error("lost the insufficient-balance classification")
	}
}

// TestNobitexBackoffMalformedAndCap: malformed/absent backOff → 0 (configured fallback applies
// at the executor); pathological values are capped (iranArb-proven 15m).
func TestNobitexBackoffMalformedAndCap(t *testing.T) {
	for _, body := range []string{`{}`, `{"backOff":-3}`, `{"backOff":"soon"}`, `not json`, ``} {
		if d := parseNobitexBackoff([]byte(body)); d != 0 {
			t.Errorf("parseNobitexBackoff(%q) = %v, want 0", body, d)
		}
	}
	if d := parseNobitexBackoff([]byte(`{"backOff":86400}`)); d != nobitexMaxBackoff {
		t.Errorf("pathological backOff = %v, want capped %v", d, nobitexMaxBackoff)
	}
	// A bare 429 with no parseable envelope: rate limit, but NOT definite (could be an
	// intermediary), no duration.
	err := nobitexHTTPError("PlaceOrder", http.StatusTooManyRequests, []byte("<html>gateway</html>"))
	rl := RateLimitOf(err)
	if rl == nil || rl.DefiniteRejection || rl.RetryAfter != 0 {
		t.Errorf("bare 429 metadata = %+v, want non-definite with no duration", rl)
	}
}

// --- bitpin (iranArb-proven auth-throttle parser reused for order paths) ----------------------

func TestBitpin429RetryAfterHeaderAndBodyRegex(t *testing.T) {
	// Retry-After header (seconds) wins.
	h := http.Header{}
	h.Set("Retry-After", "29")
	err := bitpinAPIErrorH("PlaceOrder", http.StatusTooManyRequests, h, []byte(`{"detail":"Request was throttled."}`))
	rl := RateLimitOf(err)
	if rl == nil || rl.RetryAfter != 29*time.Second || rl.Source != RLSourceHeader {
		t.Errorf("header case = %+v, want 29s/header", rl)
	}
	if rl.DefiniteRejection {
		t.Error("bitpin order-path 429 has no verified pre-execution contract — must NOT be definite")
	}
	// DRF body regex fallback (iranArb shape: "Expected available in 29 seconds.").
	err = bitpinAPIErrorH("PlaceOrder", http.StatusTooManyRequests, nil, []byte(`{"detail":"Request was throttled. Expected available in 17 seconds."}`))
	rl = RateLimitOf(err)
	if rl == nil || rl.RetryAfter != 17*time.Second || rl.Source != RLSourceBody {
		t.Errorf("body-regex case = %+v, want 17s/body", rl)
	}
	// Malformed both → rate limit with no duration.
	err = bitpinAPIErrorH("PlaceOrder", http.StatusTooManyRequests, nil, []byte(`{"detail":"slow down"}`))
	rl = RateLimitOf(err)
	if rl == nil || rl.RetryAfter != 0 {
		t.Errorf("malformed case = %+v, want detected with no duration", rl)
	}
	if !errors.Is(err, execution.ErrRateLimited) {
		t.Error("sentinel not wrapped")
	}
}

// --- wallex ------------------------------------------------------------------------------------

// TestWallex429AndHeaders: non-body venue — 429 detected from status; Retry-After honored.
func TestWallex429AndHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "11")
	err := wallexHTTPError("PlaceOrder", http.StatusTooManyRequests, h, []byte(`{"message":"Too Many Requests"}`))
	rl := RateLimitOf(err)
	if rl == nil || rl.RetryAfter != 11*time.Second {
		t.Errorf("wallex 429 = %+v, want 11s", rl)
	}
	if rl.DefiniteRejection {
		t.Error("wallex has no verified pre-execution contract — must NOT be definite")
	}
	// HTTP-200 success=false with a throttle phrase → rate limit via the conservative fallback.
	err = wallexBusinessError("PlaceOrder", "rate limit exceeded, try later")
	if rl := RateLimitOf(err); rl == nil || rl.Source != RLSourceBody || rl.DefiniteRejection {
		t.Errorf("wallex 200-body throttle = %+v, want body-source non-definite", rl)
	}
	// HTTP-200 success=false with ordinary business text → NOT a rate limit.
	err = wallexBusinessError("PlaceOrder", "limit order rejected: price out of range")
	if RateLimitOf(err) != nil {
		t.Error("ordinary business error misclassified as rate limit")
	}
}

// --- non-429 statuses & public adapters ---------------------------------------------------------

// TestNon429StatusRateLimit: a venue signaling throttle via another status code with a
// TooManyRequests envelope (nobitex non-429 path) is still detected.
func TestNon429StatusRateLimit(t *testing.T) {
	body := []byte(`{"status":"failed","code":"TooManyRequests","backOff":30}`)
	err := nobitexHTTPError("GetOrder", http.StatusForbidden, body)
	rl := RateLimitOf(err)
	if rl == nil || rl.RetryAfter != 30*time.Second || !rl.DefiniteRejection {
		t.Errorf("non-429 throttle envelope = %+v, want detected/30s/definite", rl)
	}
}

// TestApplyRateLimitSignalsPublicAdapters: the shared public-adapter path wraps the sentinel
// and reads standard headers.
func TestApplyRateLimitSignalsPublicAdapters(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "5")
	e := &NormalizedAPIError{Exchange: "binance", StatusCode: 429, Category: CatRateLimit}
	err := ApplyRateLimitSignals(e, h)
	if !errors.Is(err, execution.ErrRateLimited) {
		t.Error("public adapter 429 must wrap ErrRateLimited")
	}
	if rl := RateLimitOf(err); rl == nil || rl.RetryAfter != 5*time.Second {
		t.Errorf("public adapter metadata = %+v", rl)
	}
	// Non-rate-limit errors pass through untouched.
	other := &NormalizedAPIError{Exchange: "binance", StatusCode: 500, Category: CatServer}
	if out := ApplyRateLimitSignals(other, h); out.Err != nil || out.RateLimit != nil {
		t.Error("non-rate-limit error must pass through unchanged")
	}
}
