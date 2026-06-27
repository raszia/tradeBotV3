package exchanges

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
)

// nobitexPublicTestServer serves canned responses for the public endpoints.
func nobitexPublicTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/options":
			_, _ = w.Write([]byte(`{"nobitex":{
				"amountPrecisions":{"BTCIRT":"0.000001","USDTIRT":"0.01","ETHUSDT":"0.001"},
				"pricePrecisions":{"BTCIRT":"10","USDTIRT":"1","ETHUSDT":"0.01"},
				"minOrders":{"rls":"5000000","usdt":"5"}
			}}`))
		case strings.HasPrefix(r.URL.Path, "/v3/orderbook/"):
			// Native RIAL prices; adapter must ×0.1 for IRT markets.
			_, _ = w.Write([]byte(`{"status":"ok",
				"bids":[["6000000000","1.5"],["5999000000","2.0"]],
				"asks":[["6001000000","1.0"],["6002000000","0.5"]]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestNobitexGetMarkets(t *testing.T) {
	srv := nobitexPublicTestServer(t)
	defer srv.Close()

	pub, err := newNobitexPublic(ClientConfig{Code: nobitexCode, BaseURL: srv.URL, HTTPClient: srv.Client()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	markets, err := pub.GetMarkets(context.Background())
	if err != nil {
		t.Fatalf("GetMarkets: %v", err)
	}
	if len(markets) != 3 {
		t.Fatalf("got %d markets, want 3", len(markets))
	}
	byCanon := map[string]NormalizedMarket{}
	for _, m := range markets {
		byCanon[m.CanonicalSymbol] = m
	}

	btc, ok := byCanon["BTC/IRT"]
	if !ok {
		t.Fatalf("BTC/IRT market missing; got %v", byCanon)
	}
	if btc.QuoteAssetType != "IRT" || btc.BaseAsset != "BTC" || btc.QuoteAsset != "IRT" {
		t.Errorf("BTC/IRT metadata mismatch: %+v", btc)
	}
	// pricePrecision "10" rial → ×0.1 = 1 toman tick (value compare: arithmetic
	// may leave a non-canonical exponent that .String() could render as "1.0").
	if !btc.TickSize.Equal(decimal.NewFromInt(1)) {
		t.Errorf("BTC/IRT tick = %s, want 1", btc.TickSize)
	}
	if !btc.StepSize.Equal(mustDec("0.000001")) {
		t.Errorf("BTC/IRT step = %s, want 0.000001", btc.StepSize)
	}
	if btc.QuantityPrecision != 6 {
		t.Errorf("BTC/IRT qty precision = %d, want 6", btc.QuantityPrecision)
	}
	// minOrders.rls 5,000,000 rial → ×0.1 = 500,000 toman.
	if !btc.MinOrderAmount.Equal(decimal.NewFromInt(500000)) {
		t.Errorf("BTC/IRT min order = %s, want 500000", btc.MinOrderAmount)
	}
	if !btc.Tradable {
		t.Error("BTC/IRT should be tradable")
	}

	eth, ok := byCanon["ETH/USDT"]
	if !ok {
		t.Fatalf("ETH/USDT market missing")
	}
	// USDT markets are unconverted.
	if !eth.TickSize.Equal(mustDec("0.01")) {
		t.Errorf("ETH/USDT tick = %s, want 0.01", eth.TickSize)
	}
	if !eth.MinOrderAmount.Equal(decimal.NewFromInt(5)) {
		t.Errorf("ETH/USDT min order = %s, want 5", eth.MinOrderAmount)
	}
}

func TestNobitexGetOrderBook(t *testing.T) {
	srv := nobitexPublicTestServer(t)
	defer srv.Close()

	pub, err := newNobitexPublic(ClientConfig{
		Code: nobitexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCIRT"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	book, err := pub.GetOrderBook(context.Background(), "BTC/IRT")
	if err != nil {
		t.Fatalf("GetOrderBook: %v", err)
	}
	if book.Symbol != "BTC/IRT" || book.QuoteUnit != "IRT" {
		t.Errorf("book header mismatch: %+v", book)
	}
	// Rial → toman: raw 6,000,000,000 ×0.1 = 600,000,000. Best bid is the highest.
	bid, ok := book.BestBid()
	if !ok || !bid.Price.Equal(decimal.NewFromInt(600000000)) {
		t.Errorf("best bid = %v ok=%v, want 600000000 (rial→toman)", bid, ok)
	}
	// Best ask is the lowest: raw 6,001,000,000 ×0.1 = 600,100,000.
	ask, ok := book.BestAsk()
	if !ok || !ask.Price.Equal(decimal.NewFromInt(600100000)) {
		t.Errorf("best ask = %v ok=%v, want 600100000", ask, ok)
	}
	// Sorting: bids descending, asks ascending.
	if len(book.Bids) != 2 || book.Bids[0].Price.LessThan(book.Bids[1].Price) {
		t.Errorf("bids not sorted descending: %+v", book.Bids)
	}
	if len(book.Asks) != 2 || book.Asks[0].Price.GreaterThan(book.Asks[1].Price) {
		t.Errorf("asks not sorted ascending: %+v", book.Asks)
	}
}

func TestNobitexSubscribeOrderBookUnsupported(t *testing.T) {
	pub, err := newNobitexPublic(ClientConfig{Code: nobitexCode, BaseURL: "http://x", HTTPClient: http.DefaultClient}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pub.SubscribeOrderBook(context.Background(), []string{"BTC/IRT"}); !isUnsupported(err) {
		t.Errorf("SubscribeOrderBook err = %v, want ErrUnsupported", err)
	}
}

func newNobitexPrivateTest(t *testing.T, srv *httptest.Server) PrivateClient {
	t.Helper()
	priv, err := newNobitexPrivate(ClientConfig{
		Code: nobitexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTCIRT"},
		Creds:   StaticCredentialProvider{Creds: Credentials{APIKey: "k"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}

func TestNobitexGetBalances(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/users/wallets/list") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Token k" {
			t.Errorf("auth header = %q, want %q", got, "Token k")
		}
		_, _ = w.Write([]byte(`{"status":"ok","wallets":[
			{"currency":"btc","balance":"1.5","blockedBalance":"0.5","activeBalance":"1.0"},
			{"currency":"rls","balance":"60000000","blockedBalance":"0","activeBalance":"60000000"}
		]}`))
	}))
	defer srv.Close()

	priv := newNobitexPrivateTest(t, srv)
	bals, err := priv.GetBalances(context.Background())
	if err != nil {
		t.Fatalf("GetBalances: %v", err)
	}
	if len(bals) != 2 {
		t.Fatalf("got %d balances, want 2", len(bals))
	}
	byAsset := map[string]string{}
	avail := map[string]string{}
	for _, b := range bals {
		byAsset[b.Asset] = b.Total.String()
		avail[b.Asset] = b.Available.String()
	}
	if byAsset["BTC"] != "1.5" || avail["BTC"] != "1" {
		t.Errorf("BTC balance mismatch: total=%s avail=%s", byAsset["BTC"], avail["BTC"])
	}
	// rls → IRT and rial→toman: 60,000,000 rial ×0.1 = 6,000,000 toman.
	if !mustDec(byAsset["IRT"]).Equal(decimal.NewFromInt(6000000)) {
		t.Errorf("IRT balance = %s, want 6000000 (rls→IRT, rial→toman)", byAsset["IRT"])
	}
}

func TestNobitexPlaceOrderPassesThroughTypeAndTIF(t *testing.T) {
	var gotFields map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/market/orders/add" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotFields = parseMultipartFields(t, r)
		_, _ = w.Write([]byte(`{"status":"ok","order":{
			"id":12345,"clientOrderId":"opp-1","type":"buy","execution":"limit",
			"srcCurrency":"btc","dstCurrency":"rls","price":"6000000000","amount":"0.01",
			"matchedAmount":"0","unmatchedAmount":"0.01","status":"Active",
			"market":"BTCIRT","created_at":"2026-06-27T10:00:00.000000Z"}}`))
	}))
	defer srv.Close()

	priv := newNobitexPrivateTest(t, srv)
	ack, err := priv.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "opp-1",
		Symbol:        "BTC/IRT",
		Side:          execution.SideBuy,
		Quantity:      mustDec("0.01"),
		LimitPrice:    mustDec("600000000"), // internal IRT (toman)
		OrderType:     execution.OrderTypeLimit,
		TimeInForce:   execution.TIFGTC,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	// Type/TIF must pass through, NOT be forced to IOC/market.
	if gotFields["execution"] != "limit" {
		t.Errorf("execution = %q, want limit (not forced)", gotFields["execution"])
	}
	if gotFields["mode"] != "default" {
		t.Errorf("mode = %q, want default for GTC (not forced IOC)", gotFields["mode"])
	}
	if gotFields["type"] != "buy" {
		t.Errorf("type = %q, want buy", gotFields["type"])
	}
	if gotFields["srcCurrency"] != "btc" || gotFields["dstCurrency"] != "rls" {
		t.Errorf("currencies mismatch: %v", gotFields)
	}
	if gotFields["amount"] != "0.01" {
		t.Errorf("amount = %q, want 0.01", gotFields["amount"])
	}
	// Internal IRT 600,000,000 → venue rial ×10 = 6,000,000,000.
	if !mustDec(gotFields["price"]).Equal(decimal.NewFromInt(6000000000)) {
		t.Errorf("price = %q, want 6000000000 (toman→rial)", gotFields["price"])
	}
	// clientOrderId supported and forwarded.
	if gotFields["clientOrderId"] != "opp-1" {
		t.Errorf("clientOrderId = %q, want opp-1", gotFields["clientOrderId"])
	}
	if gotFields["pro"] != "true" {
		t.Errorf("pro = %q, want true", gotFields["pro"])
	}

	// Ack parsing.
	if ack.ExchangeOrderID != "12345" {
		t.Errorf("ack order id = %q, want 12345", ack.ExchangeOrderID)
	}
	if ack.ClientOrderID != "opp-1" {
		t.Errorf("ack client id = %q", ack.ClientOrderID)
	}
	if ack.Status != execution.StateOpen {
		t.Errorf("ack status = %q, want open", ack.Status)
	}
	if !ack.Active {
		t.Error("ack should be active")
	}
	if ack.RequestedQty.String() != "0.01" {
		t.Errorf("ack requested qty = %s", ack.RequestedQty)
	}
}

func TestNobitexPlaceOrderMarketAndIOCNotForced(t *testing.T) {
	var gotFields map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFields = parseMultipartFields(t, r)
		_, _ = w.Write([]byte(`{"status":"ok","order":{"id":7,"type":"sell","status":"Active","market":"BTCIRT","amount":"0.01"}}`))
	}))
	defer srv.Close()

	priv := newNobitexPrivateTest(t, srv)
	_, err := priv.PlaceOrder(context.Background(), execution.OrderRequest{
		Symbol:      "BTC/IRT",
		Side:        execution.SideSell,
		Quantity:    mustDec("0.01"),
		OrderType:   execution.OrderTypeMarket,
		TimeInForce: execution.TIFIOC,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if gotFields["execution"] != "market" {
		t.Errorf("execution = %q, want market (passed through)", gotFields["execution"])
	}
	if gotFields["mode"] != "ioc" {
		t.Errorf("mode = %q, want ioc (passed through, not forced/suppressed)", gotFields["mode"])
	}
	// Market orders carry no price.
	if _, ok := gotFields["price"]; ok {
		t.Errorf("market order should not send a price; got %q", gotFields["price"])
	}
}

func TestNobitexCancelOrder(t *testing.T) {
	var gotFields map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/market/orders/update-status" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotFields = parseMultipartFields(t, r)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	priv := newNobitexPrivateTest(t, srv)
	if err := priv.CancelOrder(context.Background(), "12345"); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if gotFields["order"] != "12345" || gotFields["status"] != "canceled" {
		t.Errorf("cancel fields mismatch: %v", gotFields)
	}
}

func TestNobitexGetOrderStatusMapping(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		wantState execution.NormalizedOrderState
		wantFill  string
	}{
		{
			name:      "active no fill -> open",
			body:      `{"status":"ok","order":{"id":1,"type":"buy","status":"Active","amount":"1","matchedAmount":"0","unmatchedAmount":"1","market":"BTCIRT"}}`,
			wantState: execution.StateOpen,
			wantFill:  "0",
		},
		{
			name:      "active partial -> partially_filled",
			body:      `{"status":"ok","order":{"id":1,"type":"buy","status":"Active","amount":"1","matchedAmount":"0.4","unmatchedAmount":"0.6","market":"BTCIRT"}}`,
			wantState: execution.StatePartiallyFilled,
			wantFill:  "0.4",
		},
		{
			name:      "done -> filled",
			body:      `{"status":"ok","order":{"id":1,"type":"buy","status":"Done","amount":"1","matchedAmount":"1","unmatchedAmount":"0","market":"BTCIRT"}}`,
			wantState: execution.StateFilled,
			wantFill:  "1",
		},
		{
			name:      "canceled no fill -> canceled",
			body:      `{"status":"ok","order":{"id":1,"type":"buy","status":"Canceled","amount":"1","matchedAmount":"0","unmatchedAmount":"1","market":"BTCIRT"}}`,
			wantState: execution.StateCanceled,
			wantFill:  "0",
		},
		{
			name:      "canceled partial -> partially_canceled",
			body:      `{"status":"ok","order":{"id":1,"type":"buy","status":"Canceled","amount":"1","matchedAmount":"0.3","unmatchedAmount":"0.7","market":"BTCIRT"}}`,
			wantState: execution.StatePartiallyCanceled,
			wantFill:  "0.3",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Path, "/market/orders/status") {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			priv := newNobitexPrivateTest(t, srv)
			st, err := priv.GetOrder(context.Background(), "1")
			if err != nil {
				t.Fatalf("GetOrder: %v", err)
			}
			if st.Status != tc.wantState {
				t.Errorf("status = %q, want %q", st.Status, tc.wantState)
			}
			if st.FilledQty.String() != tc.wantFill {
				t.Errorf("filled = %s, want %s", st.FilledQty, tc.wantFill)
			}
			if st.Symbol != "BTC/IRT" {
				t.Errorf("symbol = %q, want BTC/IRT", st.Symbol)
			}
		})
	}
}

func TestNobitexGetOrderRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":"TooManyRequests","backOff":60}`))
	}))
	defer srv.Close()

	priv := newNobitexPrivateTest(t, srv)
	_, err := priv.GetOrder(context.Background(), "1")
	if err == nil {
		t.Fatal("expected error on 429")
	}
	if !isSentinel(err, execution.ErrRateLimited) {
		t.Errorf("err = %v, want wrapped ErrRateLimited", err)
	}
}

func TestNobitexAuthFailureWrapsSentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"detail":"Invalid token."}`))
	}))
	defer srv.Close()

	priv := newNobitexPrivateTest(t, srv)
	_, err := priv.GetBalances(context.Background())
	if !isSentinel(err, execution.ErrAuthFailed) {
		t.Errorf("err = %v, want wrapped ErrAuthFailed", err)
	}
}

func TestNobitexCapabilityFlags(t *testing.T) {
	r, ok := Lookup(nobitexCode)
	if !ok {
		t.Fatal("nobitex not registered")
	}
	if r.NewPublic == nil || r.NewPrivate == nil {
		t.Fatal("nobitex must have both public and private constructors")
	}
	c := r.Capabilities
	want := Capabilities{
		MarketMetadata:  true,
		OrderBookREST:   true,
		OrderBookWS:     false,
		BalanceFetch:    true,
		PlaceOrder:      true,
		CancelByOrderID: true,
		FetchByOrderID:  true,
		FetchOpenOrders: true,
		RecentFills:     false,
		OrderUpdatesWS:  false,
		OrderStatusPoll: true,
		ClientOrderID:   true,
	}
	if c != want {
		t.Errorf("capabilities = %+v, want %+v", c, want)
	}
}

func TestNobitexSubscribeOrderUpdatesUnsupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	priv := newNobitexPrivateTest(t, srv)
	if _, err := priv.SubscribeOrderUpdates(context.Background()); !isUnsupported(err) {
		t.Errorf("SubscribeOrderUpdates err = %v, want ErrUnsupported", err)
	}
}

// ─── test helpers ──────────────────────────────────────────────────────────

func parseMultipartFields(t *testing.T, r *http.Request) map[string]string {
	t.Helper()
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		// Some bodies are read raw; fall back to manual parse.
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("ParseMultipartForm: %v (body=%s)", err, string(body))
	}
	out := map[string]string{}
	for k, v := range r.MultipartForm.Value {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

func mustDec(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func isUnsupported(err error) bool {
	return err != nil && errors.Is(err, ErrUnsupported)
}

func isSentinel(err, target error) bool {
	return err != nil && errors.Is(err, target)
}
