package exchanges

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
)

// PR20 correction #6 — a SUCCESSFUL response can still carry throttle information. The two
// cases must stay distinct:
//   1. the operation SUCCEEDED but the quota is exhausted → keep the result, pause FUTURE
//      requests (a cooldown), never convert the success into a failure or an ambiguity;
//   2. the HTTP status is 200 but the BODY says the venue did not perform the operation →
//      not a success at all; classified by that venue's documented contract.
// These are exercised through each private adapter's real stack (factory → transport →
// adapter), because that is the path a live order actually takes.

// recordingSink captures throttle signals reported from successful responses.
type recordingSink struct {
	mu     sync.Mutex
	signal []RateLimitInfo
	code   string
}

func (s *recordingSink) NoteHeaderRateLimit(code string, info RateLimitInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.code = code
	s.signal = append(s.signal, info)
}

func (s *recordingSink) last() (RateLimitInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.signal) == 0 {
		return RateLimitInfo{}, false
	}
	return s.signal[len(s.signal)-1], true
}

// exhaustedQuotaHeaders is the reviewer's case: the call worked, the budget is now spent.
func exhaustedQuotaHeaders(w http.ResponseWriter) {
	w.Header().Set("X-RateLimit-Remaining", "0")
	w.Header().Set("X-RateLimit-Reset", "37")
}

// --- nobitex ---------------------------------------------------------------------------------

// TestNobitexSuccessWithExhaustedQuota: HTTP 200 + a real success body + remaining=0. The
// placed order MUST come back intact; the sink MUST be told to pause future requests.
func TestNobitexSuccessWithExhaustedQuota(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exhaustedQuotaHeaders(w)
		_, _ = w.Write([]byte(`{"status":"ok","order":{
			"id":991,"clientOrderId":"cid-1","type":"buy","execution":"limit",
			"srcCurrency":"btc","dstCurrency":"rls","price":"6000000000","amount":"0.01",
			"matchedAmount":"0","unmatchedAmount":"0.01","status":"Active",
			"market":"BTCIRT","created_at":"2026-06-27T10:00:00.000000Z"}}`))
	}))
	defer srv.Close()
	sink := &recordingSink{}
	c, err := NewPrivateClient(ClientConfig{
		Code: nobitexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCIRT"}, Creds: StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}},
		RateLimitSink: sink,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := c.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "cid-1", Symbol: "BTC/IRT", Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("600000000"),
	})
	// 1) The operation succeeded and the result is preserved — exhausted quota must NEVER
	//    turn a completed mutation into an error or an ambiguous outcome.
	if err != nil {
		t.Fatalf("a successful place must stay successful, got err: %v", err)
	}
	if ack.ExchangeOrderID != "991" {
		t.Errorf("order result lost: exchange_order_id = %q, want 991", ack.ExchangeOrderID)
	}
	// 2) Future requests are paused.
	info, ok := sink.last()
	if !ok {
		t.Fatal("exhausted quota on a successful response must activate a cooldown")
	}
	if info.RetryAfter != 37*time.Second {
		t.Errorf("cooldown wait = %v, want 37s from X-RateLimit-Reset", info.RetryAfter)
	}
	if info.Source != RLSourceHeader {
		t.Errorf("source = %q, want header", info.Source)
	}
	if info.DefiniteRejection {
		t.Error("nothing was rejected — the call succeeded; DefiniteRejection must be false")
	}
	if sink.code != nobitexCode {
		t.Errorf("sink got exchange %q, want %q", sink.code, nobitexCode)
	}
}

// TestNobitexHTTP200ThrottleBodyIsNotSuccess: same 200 status, but the documented throttle
// envelope — the order was NOT placed. It must not be reported as a successful mutation.
func TestNobitexHTTP200ThrottleBodyIsNotSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"failed","code":"TooManyRequests","backOff":42,"message":"slow"}`))
	}))
	defer srv.Close()
	sink := &recordingSink{}
	c, err := NewPrivateClient(ClientConfig{
		Code: nobitexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCIRT"}, Creds: StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}},
		RateLimitSink: sink,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := c.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "cid-2", Symbol: "BTC/IRT", Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("600000000"),
	})
	if err == nil {
		t.Fatal("a 200 throttle body must NOT be treated as a successful placement")
	}
	if ack.ExchangeOrderID != "" {
		t.Errorf("no order exists, yet an id came back: %q", ack.ExchangeOrderID)
	}
	if !errors.Is(err, execution.ErrRateLimited) {
		t.Errorf("err = %v, want the rate-limit sentinel", err)
	}
	rl := RateLimitOf(err)
	if rl == nil || rl.RetryAfter != 42*time.Second {
		t.Fatalf("rate-limit metadata = %+v, want 42s from backOff", rl)
	}
	// Nobitex documents status:"failed" as not-performed → the venue PROVED no execution, so
	// this (and only this) may be re-queued after the cooldown.
	if !rl.DefiniteRejection {
		t.Error("nobitex's documented throttle envelope must be a definite pre-execution rejection")
	}
}

// TestNobitexOrdinaryLimitOrderTextNotRateLimited: an everyday business rejection that merely
// contains the words "limit order" must never be classified as throttling.
func TestNobitexOrdinaryLimitOrderTextNotRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"failed","code":"InvalidOrder","message":"limit order price is out of the allowed band"}`))
	}))
	defer srv.Close()
	sink := &recordingSink{}
	c, err := NewPrivateClient(ClientConfig{
		Code: nobitexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCIRT"}, Creds: StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}},
		RateLimitSink: sink,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "cid-3", Symbol: "BTC/IRT", Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("600000000"),
	})
	if err == nil {
		t.Fatal("an invalid order must still fail")
	}
	if RateLimitOf(err) != nil {
		t.Errorf("ordinary limit-order text misclassified as a rate limit: %v", err)
	}
	if errors.Is(err, execution.ErrRateLimited) {
		t.Error("ordinary business rejection must not carry the rate-limit sentinel")
	}
	if _, ok := sink.last(); ok {
		t.Error("no cooldown may be activated by an ordinary business rejection")
	}
}

// --- wallex ----------------------------------------------------------------------------------

// TestWallexSuccessWithExhaustedQuota: same contract on a second private adapter — success is
// preserved, future requests are paused.
func TestWallexSuccessWithExhaustedQuota(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exhaustedQuotaHeaders(w)
		w.Header().Set("Retry-After", "12") // an explicit wait wins over the reset
		_, _ = w.Write([]byte(`{"success":true,"result":{
			"symbol":"BTCTMN","type":"LIMIT","side":"BUY","price":"600000000","origQty":"0.01",
			"executedQty":"0","clientOrderId":"wcid-1","status":"NEW","active":true}}`))
	}))
	defer srv.Close()
	sink := &recordingSink{}
	c, err := NewPrivateClient(ClientConfig{
		Code: wallexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCTMN"}, Creds: StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}},
		RateLimitSink: sink,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := c.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "wcid-1", Symbol: "BTC/IRT", Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("60000000"),
	})
	if err != nil {
		t.Fatalf("a successful place must stay successful, got err: %v", err)
	}
	if ack.ClientOrderID != "wcid-1" {
		t.Errorf("order result lost: client id = %q", ack.ClientOrderID)
	}
	info, ok := sink.last()
	if !ok {
		t.Fatal("exhausted quota on a successful response must activate a cooldown")
	}
	if info.RetryAfter != 12*time.Second {
		t.Errorf("cooldown wait = %v, want 12s (Retry-After wins)", info.RetryAfter)
	}
}

// TestSuccessWithoutThrottleHeadersActivatesNothing: the common case — a plain success must
// not park anything.
func TestSuccessWithoutThrottleHeadersActivatesNothing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "97") // quota is fine
		_, _ = w.Write([]byte(`{"success":true,"result":{
			"symbol":"BTCTMN","type":"LIMIT","side":"BUY","price":"600000000","origQty":"0.01",
			"executedQty":"0","clientOrderId":"wcid-2","status":"NEW","active":true}}`))
	}))
	defer srv.Close()
	sink := &recordingSink{}
	c, err := NewPrivateClient(ClientConfig{
		Code: wallexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCTMN"}, Creds: StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}},
		RateLimitSink: sink,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "wcid-2", Symbol: "BTC/IRT", Side: "buy", OrderType: "limit",
		Quantity: decimal.RequireFromString("0.01"), LimitPrice: decimal.RequireFromString("60000000"),
	}); err != nil {
		t.Fatalf("place: %v", err)
	}
	if _, ok := sink.last(); ok {
		t.Error("a healthy quota must not activate any cooldown")
	}
}
