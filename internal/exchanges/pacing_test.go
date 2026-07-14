package exchanges

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"v3TradeBot/internal/execution"
)

// PR20 correction round 8 #3: the Bitpin adapter reserves a per-exchange pacing slot ONLY when it
// actually performs the authentication/refresh HTTP call. A still-fresh cached token makes no auth
// call and reserves no slot, so pacing reservations equal actual HTTP calls. (The order/cancel send
// is paced separately by the executor; the end-to-end 1-vs-2 total is proven in the executor tests.)

// bitpinPacingServer counts auth-endpoint and order-endpoint hits and serves valid responses.
func bitpinPacingServer(t *testing.T) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var authHits, orderHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			atomic.AddInt32(&authHits, 1)
			_, _ = w.Write([]byte(`{"access":"ACCESS-1","refresh":"REFRESH-1"}`))
		case strings.HasPrefix(r.URL.Path, "/api/v1/odr/orders/"):
			atomic.AddInt32(&orderHits, 1)
			_, _ = w.Write([]byte(`{"id":"991","identifier":"c1","state":"active","type":"limit","side":"buy","symbol":"BTC_IRT","price":"1201000","base_amount":"0.01","remain_amount":"0.01","dealed_base_amount":"0"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &authHits, &orderHits
}

// countingPacer records how many times the adapter reserved a pacing slot for a network call.
func countingPacer(n *int32) func(context.Context) error {
	return func(context.Context) error { atomic.AddInt32(n, 1); return nil }
}

func TestBitpinPlacePacesAuthCallOnlyWhenNetworked(t *testing.T) {
	srv, authHits, orderHits := bitpinPacingServer(t)
	c := bitpinTestPrivate(t, srv)
	var paceCount int32
	ctx := WithNetworkPacer(context.Background(), countingPacer(&paceCount))

	// FRESH/missing token: the adapter authenticates → paces the auth call once, then the order.
	if _, err := c.PlaceOrder(ctx, twoStagePlaceReq()); err != nil {
		t.Fatalf("PlaceOrder (fresh token): %v", err)
	}
	if a := atomic.LoadInt32(authHits); a != 1 {
		t.Errorf("auth endpoint calls = %d, want 1 (no cached token)", a)
	}
	if o := atomic.LoadInt32(orderHits); o != 1 {
		t.Errorf("order endpoint calls = %d, want 1", o)
	}
	if p := atomic.LoadInt32(&paceCount); p != 1 {
		t.Errorf("adapter pacing reservations = %d, want 1 (the auth call)", p)
	}

	// CACHED token: the second place reuses the token → NO auth call, NO auth pacing.
	atomic.StoreInt32(authHits, 0)
	atomic.StoreInt32(orderHits, 0)
	atomic.StoreInt32(&paceCount, 0)
	if _, err := c.PlaceOrder(ctx, twoStagePlaceReq()); err != nil {
		t.Fatalf("PlaceOrder (cached token): %v", err)
	}
	if a := atomic.LoadInt32(authHits); a != 0 {
		t.Errorf("auth endpoint calls = %d with a cached token, want 0", a)
	}
	if o := atomic.LoadInt32(orderHits); o != 1 {
		t.Errorf("order endpoint calls = %d, want 1", o)
	}
	if p := atomic.LoadInt32(&paceCount); p != 0 {
		t.Errorf("adapter pacing reservations = %d with a cached token, want 0", p)
	}
}

func TestBitpinCancelPacesAuthCallOnlyWhenNetworked(t *testing.T) {
	srv, authHits, orderHits := bitpinPacingServer(t)
	c := bitpinTestPrivate(t, srv)
	var paceCount int32
	ctx := WithNetworkPacer(context.Background(), countingPacer(&paceCount))

	if err := c.CancelOrder(ctx, "12345"); err != nil {
		t.Fatalf("CancelOrder (fresh token): %v", err)
	}
	if a := atomic.LoadInt32(authHits); a != 1 {
		t.Errorf("auth endpoint calls = %d, want 1 (no cached token)", a)
	}
	if o := atomic.LoadInt32(orderHits); o != 1 {
		t.Errorf("cancel endpoint calls = %d, want 1", o)
	}
	if p := atomic.LoadInt32(&paceCount); p != 1 {
		t.Errorf("adapter pacing reservations = %d, want 1 (the auth call)", p)
	}

	atomic.StoreInt32(authHits, 0)
	atomic.StoreInt32(orderHits, 0)
	atomic.StoreInt32(&paceCount, 0)
	if err := c.CancelOrder(ctx, "12345"); err != nil {
		t.Fatalf("CancelOrder (cached token): %v", err)
	}
	if a := atomic.LoadInt32(authHits); a != 0 {
		t.Errorf("auth endpoint calls = %d with a cached token, want 0", a)
	}
	if o := atomic.LoadInt32(orderHits); o != 1 {
		t.Errorf("cancel endpoint calls = %d, want 1", o)
	}
	if p := atomic.LoadInt32(&paceCount); p != 0 {
		t.Errorf("adapter pacing reservations = %d with a cached token, want 0", p)
	}
}

// A cancelled pacing wait for the auth call is definitely-not-sent (the auth request is never made).
func TestBitpinAuthPacingCancelledIsNotSent(t *testing.T) {
	srv, authHits, orderHits := bitpinPacingServer(t)
	c := bitpinTestPrivate(t, srv)
	ctx := WithNetworkPacer(context.Background(), func(context.Context) error {
		return context.Canceled
	})
	_, err := c.PlaceOrder(ctx, twoStagePlaceReq())
	if err == nil {
		t.Fatal("a cancelled auth pacing wait must fail the place")
	}
	if !execution.IsNotSent(err) {
		t.Errorf("cancelled auth pacing err = %v, want ErrNotSent", err)
	}
	if a := atomic.LoadInt32(authHits); a != 0 {
		t.Errorf("auth endpoint calls = %d after a cancelled pacing wait, want 0", a)
	}
	if o := atomic.LoadInt32(orderHits); o != 0 {
		t.Errorf("order endpoint calls = %d, want 0", o)
	}
}
