// Rate-limit detection helpers (PR20 correction #3/#6). Exchanges signal throttling in
// many shapes — HTTP 429, other non-2xx statuses, HTTP 200 with a business-level error
// code in the body, Retry-After / backOff / reset-time fields, and venue-specific codes.
// This file provides the STRUCTURED metadata model plus shared conservative parsers; each
// adapter feeds it from its venue's DOCUMENTED contract (the rules are ported from the
// owner's proven iranArb system where available — see the per-adapter code). Generic text
// matching exists only as a tightly-scoped fallback that cannot fire on harmless words
// like "limit" (e.g. "limit order").
package exchanges

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"v3TradeBot/internal/execution"
)

// RateLimitInfo is the structured rate-limit metadata carried by a NormalizedAPIError
// whose Category is CatRateLimit.
type RateLimitInfo struct {
	// RetryAfter is the venue-provided wait duration (parsed from a header, body field, or
	// reset time). Zero means the venue provided no usable duration — the caller must use
	// its configured fallback cooldown.
	RetryAfter time.Duration
	// Source records which signal identified the rate limit: "status" (HTTP status code),
	// "header" (Retry-After / X-RateLimit-*), "body" (message text), "code" (structured
	// venue error code).
	Source string
	// DefiniteRejection is true ONLY when the venue's documented contract proves the
	// request was rejected BEFORE execution (e.g. Nobitex's status:"failed" +
	// code:"TooManyRequests" envelope). It is established per exchange and per operation —
	// NEVER inferred globally. A mutating request may be re-queued only when this is true;
	// otherwise the outcome is ambiguous and must go through read-only recovery.
	DefiniteRejection bool
	// Code is the venue's structured error code, when one exists.
	Code string
}

// Rate-limit signal sources.
const (
	RLSourceStatus = "status"
	RLSourceHeader = "header"
	RLSourceBody   = "body"
	RLSourceCode   = "code"
)

// rateLimitMaxServerWait caps a venue-supplied wait so a pathological Retry-After/backOff
// value cannot park an exchange for an unbounded time (iranArb-proven cap).
const rateLimitMaxServerWait = 15 * time.Minute

// RateLimitOf extracts structured rate-limit info from any error chain. It returns non-nil
// when the error is a rate limit: a NormalizedAPIError with CatRateLimit (with its metadata,
// synthesizing an empty info if the adapter attached none) or a bare execution.ErrRateLimited.
func RateLimitOf(err error) *RateLimitInfo {
	if err == nil {
		return nil
	}
	var apiErr *NormalizedAPIError
	if errors.As(err, &apiErr) && apiErr.Category == CatRateLimit {
		if apiErr.RateLimit != nil {
			return apiErr.RateLimit
		}
		return &RateLimitInfo{Source: RLSourceStatus, Code: apiErr.Code}
	}
	if errors.Is(err, execution.ErrRateLimited) {
		return &RateLimitInfo{Source: RLSourceStatus}
	}
	return nil
}

// ParseRetryAfter parses an HTTP Retry-After header value: either integer delta-seconds
// (the only form Iranian venues use — iranArb-proven) or an HTTP-date. Returns 0 when the
// value is absent/malformed/non-positive (the caller then uses its configured fallback) and
// caps the result at rateLimitMaxServerWait.
func ParseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil {
		if n <= 0 {
			return 0
		}
		return capServerWait(time.Duration(n) * time.Second)
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return capServerWait(d)
		}
	}
	return 0
}

// RetryAfterFromHeaders inspects the standard throttle headers of a response:
// Retry-After first, then the X-RateLimit-Remaining/X-RateLimit-Reset pair (a reset is only
// meaningful when the remaining quota is exhausted — remaining "0"). The reset value is
// accepted as either delta-seconds or a unix timestamp. Returns (0, "", false) when no
// usable signal exists.
func RetryAfterFromHeaders(h http.Header, now time.Time) (time.Duration, string, bool) {
	if d := ParseRetryAfter(h.Get("Retry-After"), now); d > 0 {
		return d, RLSourceHeader, true
	}
	if strings.TrimSpace(h.Get("X-RateLimit-Remaining")) == "0" {
		reset := strings.TrimSpace(h.Get("X-RateLimit-Reset"))
		if n, err := strconv.ParseInt(reset, 10, 64); err == nil && n > 0 {
			// Heuristic: values that look like a unix timestamp (past ~2001) are absolute;
			// small values are delta-seconds.
			if n > 1_000_000_000 {
				if d := time.Unix(n, 0).Sub(now); d > 0 {
					return capServerWait(d), RLSourceHeader, true
				}
				return 0, "", false
			}
			return capServerWait(time.Duration(n) * time.Second), RLSourceHeader, true
		}
		// Remaining exhausted but no parseable reset: it IS a rate-limit signal, with no duration.
		return 0, RLSourceHeader, true
	}
	return 0, "", false
}

// LooksLikeRateLimitMessage is the CONSERVATIVE text fallback for venues that expose no
// structured code: it matches only unambiguous throttle phrases and can never fire on
// harmless uses of the word "limit" (limit order, price limit, order limit). Use it only
// when no structured code/field is available.
func LooksLikeRateLimitMessage(s string) bool {
	hay := strings.ToLower(s)
	for _, phrase := range []string{
		"too many request",    // "too many requests"
		"rate limit",          // "rate limit exceeded", "rate-limit" normalized below
		"ratelimit",           //
		"request was throttl", // DRF: "Request was throttled."
		"toomanyrequests",     // structured-code-as-text
	} {
		if strings.Contains(strings.ReplaceAll(hay, "-", " "), phrase) {
			return true
		}
	}
	return false
}

// RateLimitSink receives throttle signals observed on responses whose HTTP status was
// SUCCESSFUL (PR20 correction #6). Such a response means the operation itself completed —
// the result must be preserved and never turned into a failure — but the venue is telling us
// that FUTURE requests must pause. The order-executor implements this by parking the
// exchange's reactive cooldown without touching the completed operation.
type RateLimitSink interface {
	NoteHeaderRateLimit(exchangeCode string, info RateLimitInfo)
}

// RateLimitSinkFunc adapts a function to RateLimitSink.
type RateLimitSinkFunc func(exchangeCode string, info RateLimitInfo)

// NoteHeaderRateLimit implements RateLimitSink.
func (f RateLimitSinkFunc) NoteHeaderRateLimit(code string, info RateLimitInfo) { f(code, info) }

// observeSuccessHeaders reports a throttle signal carried by an otherwise-SUCCESSFUL response
// (e.g. `X-RateLimit-Remaining: 0` + `X-RateLimit-Reset`, or a `Retry-After` alongside 2xx).
// It NEVER inspects or alters the response itself. It lives at the shared transport rather
// than in each adapter on purpose: these headers are HTTP-standard rather than venue-specific,
// so one implementation covers every adapter and every operation (place/cancel/status/
// balance) and cannot be forgotten by a future adapter. Venue-SPECIFIC throttle contracts —
// which are about the BODY, and which decide success vs ambiguity — stay in each adapter.
// DefiniteRejection is meaningless here: nothing was rejected, the call succeeded.
func observeSuccessHeaders(sink RateLimitSink, code string, resp *http.Response) {
	if sink == nil || resp == nil || resp.StatusCode < 200 || resp.StatusCode > 299 {
		return
	}
	if d, src, ok := RetryAfterFromHeaders(resp.Header, time.Now()); ok {
		sink.NoteHeaderRateLimit(code, RateLimitInfo{RetryAfter: d, Source: src})
	}
}

// ApplyRateLimitSignals finalizes a constructed NormalizedAPIError whose Category resolved
// to CatRateLimit: it wraps the execution.ErrRateLimited sentinel (so errors.Is works) and
// attaches header-derived throttle metadata. For adapters without a venue-specific
// documented contract (e.g. the public read-only clients) DefiniteRejection stays false.
func ApplyRateLimitSignals(e *NormalizedAPIError, hdr http.Header) *NormalizedAPIError {
	if e == nil || e.Category != CatRateLimit {
		return e
	}
	if e.Err == nil {
		e.Err = execution.ErrRateLimited
	}
	if e.RateLimit == nil {
		rl := &RateLimitInfo{Source: RLSourceStatus, Code: e.Code}
		if hdr != nil {
			if d, src, ok := RetryAfterFromHeaders(hdr, time.Now()); ok {
				rl.RetryAfter, rl.Source = d, src
			}
		}
		e.RateLimit = rl
	}
	return e
}

func capServerWait(d time.Duration) time.Duration {
	if d > rateLimitMaxServerWait {
		return rateLimitMaxServerWait
	}
	return d
}
