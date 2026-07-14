package exchanges

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
)

// PR20 round-5 #3: the private adapters classify pre-network failures as execution.ErrNotSent
// (definitely not sent) and make ZERO network calls when they do.

// failingCreds is a credential provider whose decrypt/load fails (e.g. the credential DB is
// briefly unavailable) — a transient pre-network failure.
type failingCreds struct{}

func (failingCreds) Credentials(context.Context, string) (Credentials, error) {
	return Credentials{}, errors.New("credential db temporarily unavailable")
}

// countingServer returns an httptest server that records how many requests reach it.
func countingServer(t *testing.T) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func placeReq(symbol string) execution.OrderRequest {
	return execution.OrderRequest{
		ClientOrderID: "c1", Symbol: symbol, Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("100"),
	}
}

func TestPrivateAdaptersCredentialFailureIsNotSent(t *testing.T) {
	build := map[string]func(cfg ClientConfig) (PrivateClient, error){
		"nobitex": func(cfg ClientConfig) (PrivateClient, error) {
			cfg.Code = nobitexCode
			return newNobitexPrivate(cfg, nil)
		},
		"wallex": func(cfg ClientConfig) (PrivateClient, error) {
			cfg.Code = wallexCode
			return newWallexPrivate(cfg, nil)
		},
		"bitpin": func(cfg ClientConfig) (PrivateClient, error) {
			cfg.Code = bitpinCode
			return newBitpinPrivate(cfg, nil)
		},
	}
	syms := map[string]map[string]string{
		"nobitex": {"BTC/IRT": "BTCIRT"}, "wallex": {"BTC/IRT": "BTCTMN"}, "bitpin": {"BTC/IRT": "BTC_IRT"},
	}
	for name, mk := range build {
		t.Run(name, func(t *testing.T) {
			srv, hits := countingServer(t)
			c, err := mk(ClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Symbols: syms[name], Creds: failingCreds{}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.PlaceOrder(context.Background(), placeReq("BTC/IRT"))
			if err == nil {
				t.Fatal("a credential failure must fail the place")
			}
			if !execution.IsNotSent(err) {
				t.Errorf("credential failure err = %v, want ErrNotSent (definitely not sent)", err)
			}
			if execution.IsNotSentPermanent(err) {
				t.Error("a credential-DB blip must be TEMPORARY, not permanent")
			}
			if n := atomic.LoadInt32(hits); n != 0 {
				t.Errorf("%d network requests reached the venue, want 0 (failed before the network boundary)", n)
			}
		})
	}
}

func TestPrivateAdaptersInvalidSymbolIsNotSentPermanent(t *testing.T) {
	build := map[string]func(cfg ClientConfig) (PrivateClient, error){
		"nobitex": func(cfg ClientConfig) (PrivateClient, error) {
			cfg.Code = nobitexCode
			return newNobitexPrivate(cfg, nil)
		},
		"wallex": func(cfg ClientConfig) (PrivateClient, error) {
			cfg.Code = wallexCode
			return newWallexPrivate(cfg, nil)
		},
		"bitpin": func(cfg ClientConfig) (PrivateClient, error) {
			cfg.Code = bitpinCode
			return newBitpinPrivate(cfg, nil)
		},
	}
	for name, mk := range build {
		t.Run(name, func(t *testing.T) {
			srv, hits := countingServer(t)
			// No Symbols mapping AND a symbol the venue-native mapper can't resolve → empty native.
			c, err := mk(ClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client(),
				Creds: StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.PlaceOrder(context.Background(), placeReq(""))
			if err == nil {
				t.Fatal("an invalid symbol must fail the place")
			}
			if !execution.IsNotSent(err) || !execution.IsNotSentPermanent(err) {
				t.Errorf("invalid-symbol err = %v, want a PERMANENT ErrNotSent", err)
			}
			if n := atomic.LoadInt32(hits); n != 0 {
				t.Errorf("%d network requests reached the venue, want 0 (rejected before building/sending HTTP)", n)
			}
		})
	}
}

// --- PR20 round-6 #3: Bitpin auth/token failures are definitely-not-sent, never order outcomes.

// TestBitpinAuthRateLimitIsNotSentNoOrderCall: the auth endpoint returns 429; the ORDER endpoint
// is never called, the error is a definitely-not-sent rate limit (so the executor arms the
// cooldown and never creates an order probe).
func TestBitpinAuthRateLimitIsNotSentNoOrderCall(t *testing.T) {
	var orderHits, authHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			atomic.AddInt32(&authHits, 1)
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"detail":"Request was throttled."}`))
		case r.URL.Path == "/api/v1/odr/orders/":
			atomic.AddInt32(&orderHits, 1)
			_, _ = w.Write([]byte(`{"id":1}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := bitpinTestPrivate(t, srv)

	_, err := c.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "c1", Symbol: "BTC/IRT", Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("1201000"),
	})
	if err == nil {
		t.Fatal("an auth 429 must fail the place")
	}
	if !execution.IsNotSent(err) {
		t.Errorf("auth 429 err = %v, want ErrNotSent (order endpoint was not called)", err)
	}
	if RateLimitOf(err) == nil {
		t.Errorf("auth 429 must still be detectable as a rate limit (so the cooldown is armed): %v", err)
	}
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Errorf("order endpoint called %d times after an auth 429, want 0", n)
	}
	if atomic.LoadInt32(&authHits) == 0 {
		t.Error("the auth endpoint should have been attempted")
	}
}

// TestBitpinTokenTimeoutIsNotSent: the token endpoint hangs past the request timeout; the order
// endpoint is never called and the failure is definitely-not-sent (not an order ambiguity).
func TestBitpinTokenTimeoutIsNotSent(t *testing.T) {
	var orderHits int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			<-release // hang until the client's request context times out
		case r.URL.Path == "/api/v1/odr/orders/":
			atomic.AddInt32(&orderHits, 1)
			_, _ = w.Write([]byte(`{"id":1}`))
		}
	}))
	defer srv.Close()
	defer close(release)
	cfg := ClientConfig{Code: bitpinCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		RequestTimeout: 100 * time.Millisecond,
		Creds:          StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}}}
	c, err := newBitpinPrivate(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "c1", Symbol: "BTC/IRT", Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("1201000"),
	})
	if err == nil {
		t.Fatal("a token timeout must fail the place")
	}
	if !execution.IsNotSent(err) {
		t.Errorf("token timeout err = %v, want ErrNotSent (order endpoint not reached)", err)
	}
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Errorf("order endpoint called %d times after a token timeout, want 0", n)
	}
}

// --- PR20 round-7 #2/#3: Bitpin auth rate limit blocks the order; token acquisition is
//     the pre-order (Prepare) work so a rate limit never reaches the order endpoint. -----------

// bitpinAuthServer serves auth responses per authResp and counts order-endpoint hits.
func bitpinAuthServer(t *testing.T, authStatus int, authHeaders map[string]string, orderHits *int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			for k, v := range authHeaders {
				w.Header().Set(k, v)
			}
			w.WriteHeader(authStatus)
			if authStatus >= 200 && authStatus < 300 {
				_, _ = w.Write([]byte(`{"access":"ACCESS-1","refresh":"REFRESH-1"}`))
			} else {
				_, _ = w.Write([]byte(`{"detail":"throttled"}`))
			}
		case r.URL.Path == "/api/v1/odr/orders/":
			atomic.AddInt32(orderHits, 1)
			_, _ = w.Write([]byte(`{"id":"1","identifier":"c1","state":"active"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestBitpinAuth429BlocksOrder: the auth endpoint 429s → PreparePlace fails as a
// definitely-not-sent RATE LIMIT and the order endpoint is never called.
func TestBitpinAuth429BlocksOrder(t *testing.T) {
	var orderHits int32
	srv := bitpinAuthServer(t, http.StatusTooManyRequests, map[string]string{"Retry-After": "30"}, &orderHits)
	c := bitpinTestPrivate(t, srv)

	_, err := c.PlaceOrder(context.Background(), placeReq("BTC/IRT"))
	if err == nil {
		t.Fatal("auth 429 must fail the place")
	}
	if !execution.IsNotSent(err) {
		t.Errorf("err = %v, want ErrNotSent", err)
	}
	if RateLimitOf(err) == nil {
		t.Errorf("auth 429 must be a rate limit (so the executor arms the cooldown): %v", err)
	}
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Errorf("order endpoint called %d times after an auth 429, want 0", n)
	}
}

// TestBitpinAuth200QuotaExhaustedBlocksOrder: authentication SUCCEEDS (200) but reports
// X-RateLimit-Remaining: 0 — the order is a subsequent call, so it must not be sent this
// invocation (definitely-not-sent rate limit); the order endpoint is never called.
func TestBitpinAuth200QuotaExhaustedBlocksOrder(t *testing.T) {
	var orderHits int32
	srv := bitpinAuthServer(t, http.StatusOK, map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "30"}, &orderHits)
	c := bitpinTestPrivate(t, srv)

	_, err := c.PlaceOrder(context.Background(), placeReq("BTC/IRT"))
	if err == nil {
		t.Fatal("auth 200 + remaining:0 must not send the order")
	}
	if !execution.IsNotSent(err) {
		t.Errorf("err = %v, want ErrNotSent (order not sent this invocation)", err)
	}
	if RateLimitOf(err) == nil {
		t.Errorf("quota-exhaustion must be a rate limit: %v", err)
	}
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Errorf("order endpoint called %d times after auth quota exhaustion, want 0", n)
	}
}

// TestBitpinAuth429BlocksCancel: the same rule applies to CancelOrder's token flow.
func TestBitpinAuth429BlocksCancel(t *testing.T) {
	var orderHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/" {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"detail":"throttled"}`))
			return
		}
		atomic.AddInt32(&orderHits, 1) // any order/cancel endpoint
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := bitpinTestPrivate(t, srv)

	err := c.CancelOrder(context.Background(), "123")
	if err == nil || !execution.IsNotSent(err) {
		t.Errorf("cancel under auth 429 err = %v, want ErrNotSent", err)
	}
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Errorf("cancel endpoint called %d times after an auth 429, want 0", n)
	}
}
