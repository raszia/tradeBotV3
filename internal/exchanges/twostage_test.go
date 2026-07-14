package exchanges

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
)

// PR20 correction round 8 #1/#2: every private adapter splits a mutation into PreparePlace/
// PrepareCancel (ALL fallible pre-network work) and PreparedMutation.Send (the single order/cancel
// HTTP call). Preparation makes NO mutation HTTP call, and a preparation failure is
// definitely-not-sent with zero network traffic. Bitpin additionally builds its FINAL http.Request
// during preparation, so nothing fallible is left for Send.

func staticCreds() StaticCredentialProvider {
	return StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}}
}

// twoStagePlaceReq is a canonical limit buy used by the two-stage tests.
func twoStagePlaceReq() execution.OrderRequest {
	return execution.OrderRequest{
		ClientOrderID: "c1", Symbol: "BTC/IRT", Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("1201000"),
	}
}

// mutationCountingServer serves a generic 200 for any mutation and counts hits.
func mutationCountingServer(t *testing.T) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
		// A shape every adapter's decoder accepts as success.
		_, _ = w.Write([]byte(`{"status":"ok","success":true,"result":{"clientOrderId":"c1","symbol":"BTCTMN","side":"BUY","type":"LIMIT","status":"NEW","active":true},"order":{"id":991,"clientOrderId":"c1","type":"buy","execution":"limit","srcCurrency":"btc","dstCurrency":"rls","status":"Active"},"id":"991","identifier":"c1","state":"active"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// nobitex/wallex: preparation performs NO network call; Send performs exactly one.
func TestNobitexPreparationMakesNoNetworkCall(t *testing.T) {
	srv, hits := mutationCountingServer(t)
	c, err := newNobitexPrivate(ClientConfig{Code: nobitexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCIRT"}, Creds: staticCreds()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pm := c.(MutationPreparer)

	prepared, err := pm.PreparePlace(context.Background(), twoStagePlaceReq())
	if err != nil {
		t.Fatalf("PreparePlace: %v", err)
	}
	if n := atomic.LoadInt32(hits); n != 0 {
		t.Fatalf("preparation made %d HTTP calls, want 0 (all pre-network work is local)", n)
	}
	if _, err := prepared.Send(context.Background()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Fatalf("after Send, HTTP calls = %d, want exactly 1", n)
	}

	atomic.StoreInt32(hits, 0)
	pc, err := pm.PrepareCancel(context.Background(), "12345")
	if err != nil {
		t.Fatalf("PrepareCancel: %v", err)
	}
	if n := atomic.LoadInt32(hits); n != 0 {
		t.Fatalf("cancel preparation made %d HTTP calls, want 0", n)
	}
	if _, err := pc.Send(context.Background()); err != nil {
		t.Fatalf("cancel Send: %v", err)
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Fatalf("after cancel Send, HTTP calls = %d, want exactly 1", n)
	}
}

func TestWallexPreparationMakesNoNetworkCall(t *testing.T) {
	srv, hits := mutationCountingServer(t)
	c, err := newWallexPrivate(ClientConfig{Code: wallexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCTMN"}, Creds: staticCreds()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pm := c.(MutationPreparer)

	prepared, err := pm.PreparePlace(context.Background(), twoStagePlaceReq())
	if err != nil {
		t.Fatalf("PreparePlace: %v", err)
	}
	if n := atomic.LoadInt32(hits); n != 0 {
		t.Fatalf("preparation made %d HTTP calls, want 0", n)
	}
	if _, err := prepared.Send(context.Background()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Fatalf("after Send, HTTP calls = %d, want exactly 1", n)
	}

	atomic.StoreInt32(hits, 0)
	pc, err := pm.PrepareCancel(context.Background(), "c1")
	if err != nil {
		t.Fatalf("PrepareCancel: %v", err)
	}
	if n := atomic.LoadInt32(hits); n != 0 {
		t.Fatalf("cancel preparation made %d HTTP calls, want 0", n)
	}
	if _, err := pc.Send(context.Background()); err != nil {
		t.Fatalf("cancel Send: %v", err)
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Fatalf("after cancel Send, HTTP calls = %d, want exactly 1", n)
	}
}

// A credential-preparation failure is definitely-not-sent with ZERO mutation HTTP calls, for both
// PlaceOrder and CancelOrder, on both adapters. (The executor-level tests then prove such a failure
// never reaches IN_FLIGHT and creates no probe.)
func TestNobitexPrepareCredentialFailureNoNetwork(t *testing.T) {
	assertPrepareCredFailureNoNetwork(t, func(cfg ClientConfig) (PrivateClient, error) {
		cfg.Code = nobitexCode
		cfg.Symbols = map[string]string{"BTC/IRT": "BTCIRT"}
		return newNobitexPrivate(cfg, nil)
	})
}

func TestWallexPrepareCredentialFailureNoNetwork(t *testing.T) {
	assertPrepareCredFailureNoNetwork(t, func(cfg ClientConfig) (PrivateClient, error) {
		cfg.Code = wallexCode
		cfg.Symbols = map[string]string{"BTC/IRT": "BTCTMN"}
		return newWallexPrivate(cfg, nil)
	})
}

func assertPrepareCredFailureNoNetwork(t *testing.T, mk func(ClientConfig) (PrivateClient, error)) {
	t.Helper()
	srv, hits := mutationCountingServer(t)
	c, err := mk(ClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Creds: failingCreds{}})
	if err != nil {
		t.Fatal(err)
	}
	pm := c.(MutationPreparer)

	prepared, perr := pm.PreparePlace(context.Background(), twoStagePlaceReq())
	if perr == nil || prepared != nil {
		t.Fatalf("PreparePlace with failing creds must return (nil, err), got (%v, %v)", prepared, perr)
	}
	if !execution.IsNotSent(perr) {
		t.Errorf("PreparePlace cred failure = %v, want ErrNotSent (definitely not sent)", perr)
	}

	pc, cerr := pm.PrepareCancel(context.Background(), "12345")
	if cerr == nil || pc != nil {
		t.Fatalf("PrepareCancel with failing creds must return (nil, err), got (%v, %v)", pc, cerr)
	}
	if !execution.IsNotSent(cerr) {
		t.Errorf("PrepareCancel cred failure = %v, want ErrNotSent", cerr)
	}
	if n := atomic.LoadInt32(hits); n != 0 {
		t.Errorf("%d mutation HTTP calls after a credential failure, want 0", n)
	}
}

// Bitpin builds its FINAL http.Request during preparation (round 8 #2): the prepared value holds a
// fully-constructed request (method, URL, auth header, body) — nothing fallible is left for Send.
func TestBitpinPreparedRequestBuiltBeforeSend(t *testing.T) {
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
	defer srv.Close()
	c := bitpinTestPrivate(t, srv)
	pm := c.(MutationPreparer)

	prepared, err := pm.PreparePlace(context.Background(), twoStagePlaceReq())
	if err != nil {
		t.Fatalf("PreparePlace: %v", err)
	}
	// The auth call already happened during preparation; the ORDER call has NOT.
	if n := atomic.LoadInt32(&orderHits); n != 0 {
		t.Fatalf("order endpoint hit %d times during preparation, want 0 (only Send may call it)", n)
	}
	bp, ok := prepared.(*bitpinPrepared)
	if !ok {
		t.Fatalf("prepared is %T, want *bitpinPrepared", prepared)
	}
	if bp.req == nil {
		t.Fatal("prepared.req is nil — the final request must be built during preparation")
	}
	if bp.req.Method != http.MethodPost || !strings.Contains(bp.req.URL.Path, "/odr/orders/") {
		t.Errorf("prepared request = %s %s, want POST .../odr/orders/", bp.req.Method, bp.req.URL.Path)
	}
	if got := bp.req.Header.Get("Authorization"); !strings.HasPrefix(got, "Bearer ") {
		t.Errorf("prepared request missing bearer auth header, got %q", got)
	}
	if bp.req.Body == nil {
		t.Error("prepared place request must carry its body (built before Send)")
	}

	if _, err := bp.Send(context.Background()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if n := atomic.LoadInt32(&orderHits); n != 1 {
		t.Errorf("order endpoint hit %d times after Send, want exactly 1", n)
	}
}
