package exchanges

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/execution"
)

// PR20 correction round 9 #4: a Bitpin authentication/refresh rate limit must carry the REAL venue
// throttle deadline in RateLimitInfo.RetryAfter, so the executor schedules the queue retry at that
// deadline rather than a short configured fallback that would burn the retry budget before the
// throttle expires. The mutation is still definitely-not-sent (order endpoint call count = 0).

// TestBitpinAuthFresh429CarriesRetryAfter: a fresh auth 429 with Retry-After:30 → the error carries
// RetryAfter≈30s and the order endpoint is never called.
func TestBitpinAuthFresh429CarriesRetryAfter(t *testing.T) {
	var authHits, orderHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			atomic.AddInt32(&authHits, 1)
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"detail":"Request was throttled."}`))
		case strings.HasPrefix(r.URL.Path, "/api/v1/odr/orders/"):
			atomic.AddInt32(&orderHits, 1)
			_, _ = w.Write([]byte(`{"id":"1"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := bitpinTestPrivate(t, srv)

	_, err := c.PlaceOrder(context.Background(), twoStagePlaceReq())
	if err == nil {
		t.Fatal("an auth 429 must fail the place")
	}
	if !execution.IsNotSent(err) {
		t.Errorf("err = %v, want ErrNotSent", err)
	}
	rl := RateLimitOf(err)
	if rl == nil {
		t.Fatalf("auth 429 must be a rate limit: %v", err)
	}
	if rl.RetryAfter < 29*time.Second || rl.RetryAfter > 31*time.Second {
		t.Errorf("RetryAfter = %v, want ≈30s (the venue Retry-After), not the configured fallback", rl.RetryAfter)
	}
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Errorf("order endpoint called %d times, want 0", n)
	}
}

// TestBitpinActiveThrottleCarriesRemaining: while an auth throttle window is still active, a second
// mutation returns the REMAINING duration and makes NO further auth call (does not re-attempt and
// burn a retry before the window expires).
func TestBitpinActiveThrottleCarriesRemaining(t *testing.T) {
	var authHits, orderHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			atomic.AddInt32(&authHits, 1)
			w.Header().Set("Retry-After", "20")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"detail":"throttled"}`))
		case strings.HasPrefix(r.URL.Path, "/api/v1/odr/orders/"):
			atomic.AddInt32(&orderHits, 1)
			_, _ = w.Write([]byte(`{"id":"1"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := bitpinTestPrivate(t, srv)

	// First mutation arms the throttle window (authThrottledUntil ≈ now+20s).
	if _, err := c.PlaceOrder(context.Background(), twoStagePlaceReq()); err == nil {
		t.Fatal("first place must fail on the auth 429")
	}
	if atomic.LoadInt32(&authHits) != 1 {
		t.Fatalf("expected exactly one auth attempt, got %d", authHits)
	}

	// Second mutation while the window is active: no new auth call, remaining duration carried.
	_, err := c.PlaceOrder(context.Background(), twoStagePlaceReq())
	if err == nil {
		t.Fatal("second place must also fail while the throttle window is active")
	}
	if n := atomic.LoadInt32(&authHits); n != 1 {
		t.Errorf("auth endpoint called %d times, want 1 (no re-auth inside the active window)", n)
	}
	rl := RateLimitOf(err)
	if rl == nil {
		t.Fatalf("active throttle must be a rate limit: %v", err)
	}
	if rl.RetryAfter <= 10*time.Second || rl.RetryAfter > 20*time.Second {
		t.Errorf("RetryAfter = %v, want the remaining window (≈20s), not 0/fallback", rl.RetryAfter)
	}
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Errorf("order endpoint called %d times, want 0", n)
	}
}

// TestBitpinAuth200QuotaResetCarriesDeadline: an auth 200 with X-RateLimit-Remaining:0 and a reset
// → the error carries the reset-derived deadline and the order endpoint is never called.
func TestBitpinAuth200QuotaResetCarriesDeadline(t *testing.T) {
	var orderHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", "25")
			_, _ = w.Write([]byte(`{"access":"A","refresh":"R"}`))
		case strings.HasPrefix(r.URL.Path, "/api/v1/odr/orders/"):
			atomic.AddInt32(&orderHits, 1)
			_, _ = w.Write([]byte(`{"id":"1"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := bitpinTestPrivate(t, srv)

	_, err := c.PlaceOrder(context.Background(), twoStagePlaceReq())
	if err == nil {
		t.Fatal("auth 200 with exhausted quota must fail the place")
	}
	rl := RateLimitOf(err)
	if rl == nil {
		t.Fatalf("quota-exhausted auth must be a rate limit: %v", err)
	}
	if rl.RetryAfter < 24*time.Second || rl.RetryAfter > 26*time.Second {
		t.Errorf("RetryAfter = %v, want ≈25s (X-RateLimit-Reset)", rl.RetryAfter)
	}
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Errorf("order endpoint called %d times, want 0", n)
	}
}
