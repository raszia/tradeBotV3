package exchanges

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
)

// bitpinTestServer stubs the Bitpin REST surface, including the JWT
// authenticate/refresh endpoints. captured (if non-nil) records the last place
// order request body for assertions.
func bitpinTestServer(t *testing.T, captured *bitpinPlaceOrderRequest) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["api_key"] == "" || body["secret_key"] == "" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"detail":"missing credentials"}`))
				return
			}
			_, _ = w.Write([]byte(`{"access":"ACCESS-TOKEN-1","refresh":"REFRESH-TOKEN-1"}`))

		case r.URL.Path == "/api/v1/usr/refresh_token/":
			_, _ = w.Write([]byte(`{"access":"ACCESS-TOKEN-2"}`))

		case r.URL.Path == "/api/v1/mkt/markets/":
			_, _ = w.Write([]byte(`[
				{"symbol":"BTC_IRT","code":"BTC_IRT","base":"BTC","quote":"IRT","tradable":true,"suspended":false,"price_precision":0,"base_amount_precision":8},
				{"symbol":"USDT_IRT","code":"USDT_IRT","base":"USDT","quote":"IRT","tradable":true,"suspended":false,"price_precision":0,"base_amount_precision":2},
				{"symbol":"ETH_IRT","code":"ETH_IRT","base":"ETH","quote":"IRT","tradable":true,"suspended":true,"price_precision":0,"base_amount_precision":6}
			]`))

		case strings.HasPrefix(r.URL.Path, "/api/v1/mth/orderbook/"):
			_, _ = w.Write([]byte(`{"bids":[["1200000","1.5"],["1199000","2.0"]],"asks":[["1201000","1.0"],["1202000","0.5"]]}`))

		case r.URL.Path == "/api/v1/wlt/wallets/":
			// Require the bearer token to assert auth wiring.
			if got := r.Header.Get("Authorization"); !strings.HasPrefix(got, "Bearer ") {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"results":[
				{"asset":"BTC","service":"main","available":"0.5","frozen":"0.1","balance":"0.6"},
				{"asset":"RIAL","service":"main","available":"1000000","frozen":"0","balance":"1000000"},
				{"asset":"DOGE","service":"futures","available":"99","balance":"99"}
			]}`))

		case r.URL.Path == "/api/v1/odr/orders/" && r.Method == http.MethodPost:
			var body bitpinPlaceOrderRequest
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &body)
			if captured != nil {
				*captured = body
			}
			_, _ = w.Write([]byte(`{"id":987654,"identifier":"` + body.Identifier + `","state":"active","side":"buy","symbol":"BTC_IRT","price":"1201000","base_amount":"0.01"}`))

		// GetOrder by numeric id.
		case r.URL.Path == "/api/v1/odr/orders/987654/" && r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"id":987654,"identifier":"cid-1","state":"closed","side":"buy","symbol":"BTC_IRT",
				"price":"1201000","average_price":"1201000","base_amount":"0.01","dealed_base_amount":"0.01",
				"dealed_quote_amount":"12010","commission":"0.00001","commission_currency":"BTC","created_at":"2026-06-27T10:00:00Z","closed_at":"2026-06-27T10:00:01Z"}`))

		// GetOrder by identifier returns a top-level array.
		case r.URL.Path == "/api/v1/odr/orders/" && r.Method == http.MethodGet && r.URL.Query().Get("identifier") != "":
			_, _ = w.Write([]byte(`[{"id":555,"identifier":"` + r.URL.Query().Get("identifier") + `","state":"active","side":"sell","symbol":"BTC_IRT","base_amount":"0.02","dealed_base_amount":"0.005","remain_amount":"0.015"}]`))

		// GetOpenOrders.
		case r.URL.Path == "/api/v1/odr/orders/" && r.Method == http.MethodGet && r.URL.Query().Get("state") == "active":
			_, _ = w.Write([]byte(`{"results":[
				{"id":1,"identifier":"open-1","state":"active","side":"buy","symbol":"BTC_IRT","base_amount":"0.02","dealed_base_amount":"0"},
				{"id":2,"identifier":"done-1","state":"closed","side":"buy","symbol":"BTC_IRT","base_amount":"0.02","dealed_base_amount":"0.02"}
			]}`))

		// CancelOrder by numeric id and by identifier.
		case strings.HasPrefix(r.URL.Path, "/api/v1/odr/orders/") && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func bitpinTestPrivate(t *testing.T, srv *httptest.Server) PrivateClient {
	t.Helper()
	cfg := ClientConfig{
		Code:       bitpinCode,
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
		Creds:      StaticCredentialProvider{Creds: Credentials{APIKey: "k", APISecret: "s"}},
	}
	priv, err := newBitpinPrivate(cfg, nil)
	if err != nil {
		t.Fatalf("newBitpinPrivate: %v", err)
	}
	return priv
}

func TestBitpinCapabilities(t *testing.T) {
	r, ok := Lookup(bitpinCode)
	if !ok {
		t.Fatal("bitpin not registered")
	}
	c := r.Capabilities
	if !c.MarketMetadata || !c.OrderBookREST || !c.BalanceFetch || !c.PlaceOrder ||
		!c.CancelByOrderID || !c.FetchByOrderID || !c.FetchOpenOrders {
		t.Errorf("missing expected capabilities: %+v", c)
	}
	if c.OrderBookWS || c.OrderUpdatesWS {
		t.Errorf("WS capabilities must be false (deferred): %+v", c)
	}
	if !c.OrderStatusPoll {
		t.Error("OrderStatusPoll should be true (polling-only)")
	}
	if !c.ClientOrderID {
		t.Error("ClientOrderID should be true (identifier field)")
	}
	if r.NewPublic == nil || r.NewPrivate == nil {
		t.Error("bitpin should have both public and private constructors")
	}
}

func TestBitpinGetMarkets(t *testing.T) {
	srv := bitpinTestServer(t, nil)
	defer srv.Close()

	pub, err := newBitpinPublic(ClientConfig{Code: bitpinCode, BaseURL: srv.URL, HTTPClient: srv.Client()}, nil)
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
	var btc, eth NormalizedMarket
	for _, m := range markets {
		switch m.ExchangeSymbol {
		case "BTC_IRT":
			btc = m
		case "ETH_IRT":
			eth = m
		}
	}
	if btc.CanonicalSymbol != "BTC/IRT" || btc.QuoteAssetType != "IRT" {
		t.Errorf("BTC market mismatch: %+v", btc)
	}
	if !btc.Tradable {
		t.Error("BTC_IRT should be tradable")
	}
	if eth.Tradable {
		t.Error("ETH_IRT is suspended and must not be tradable")
	}
	// price_precision 0 -> tick 1; base_amount_precision 8 -> step 0.00000001
	if btc.TickSize.String() != "1" {
		t.Errorf("BTC tick = %s, want 1", btc.TickSize)
	}
	if btc.StepSize.String() != "0.00000001" {
		t.Errorf("BTC step = %s, want 0.00000001", btc.StepSize)
	}
}

func TestBitpinGetOrderBook(t *testing.T) {
	srv := bitpinTestServer(t, nil)
	defer srv.Close()

	pub, err := newBitpinPublic(ClientConfig{
		Code: bitpinCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/IRT": "BTC_IRT"},
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
	bid, ok := book.BestBid()
	if !ok || bid.Price.String() != "1200000" {
		t.Errorf("best bid = %v ok=%v", bid, ok)
	}
	ask, ok := book.BestAsk()
	if !ok || ask.Price.String() != "1201000" {
		t.Errorf("best ask = %v ok=%v", ask, ok)
	}
}

func TestBitpinSubscribeUnsupported(t *testing.T) {
	srv := bitpinTestServer(t, nil)
	defer srv.Close()
	pub, err := newBitpinPublic(ClientConfig{Code: bitpinCode, BaseURL: srv.URL, HTTPClient: srv.Client()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pub.SubscribeOrderBook(context.Background(), []string{"BTC/IRT"}); !errors.Is(err, ErrUnsupported) {
		t.Errorf("SubscribeOrderBook err = %v, want ErrUnsupported", err)
	}
	priv := bitpinTestPrivate(t, srv)
	if _, err := priv.SubscribeOrderUpdates(context.Background()); !errors.Is(err, ErrUnsupported) {
		t.Errorf("SubscribeOrderUpdates err = %v, want ErrUnsupported", err)
	}
}

func TestBitpinGetBalances(t *testing.T) {
	srv := bitpinTestServer(t, nil)
	defer srv.Close()
	priv := bitpinTestPrivate(t, srv)

	balances, err := priv.GetBalances(context.Background())
	if err != nil {
		t.Fatalf("GetBalances: %v", err)
	}
	byAsset := map[string]struct{ avail, locked string }{}
	for _, b := range balances {
		byAsset[b.Asset] = struct{ avail, locked string }{b.Available.String(), b.Locked.String()}
	}
	if v, ok := byAsset["BTC"]; !ok || v.avail != "0.5" || v.locked != "0.1" {
		t.Errorf("BTC balance = %+v", v)
	}
	// RIAL normalizes to IRT.
	if v, ok := byAsset["IRT"]; !ok || v.avail != "1000000" {
		t.Errorf("IRT balance = %+v (RIAL should normalize to IRT)", v)
	}
	// futures service wallet is skipped.
	if _, ok := byAsset["DOGE"]; ok {
		t.Error("DOGE (futures service) should be skipped")
	}
}

func TestBitpinPlaceOrderPassThrough(t *testing.T) {
	var captured bitpinPlaceOrderRequest
	srv := bitpinTestServer(t, &captured)
	defer srv.Close()
	priv := bitpinTestPrivate(t, srv)

	req := execution.OrderRequest{
		ClientOrderID: "cid-abc",
		Symbol:        "BTC/IRT",
		Side:          execution.SideBuy,
		Quantity:      decimal.RequireFromString("0.01"),
		LimitPrice:    decimal.RequireFromString("1201000"),
		OrderType:     execution.OrderTypeLimit,
		TimeInForce:   execution.TIFGTC,
	}
	ack, err := priv.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	// type / TIF pass-through: no forced IOC/market.
	if captured.Type != "limit" {
		t.Errorf("captured type = %q, want limit (owner pass-through)", captured.Type)
	}
	if captured.TimeInForce != "GTC" {
		t.Errorf("captured TIF = %q, want GTC (pass-through)", captured.TimeInForce)
	}
	if captured.Side != "buy" {
		t.Errorf("captured side = %q", captured.Side)
	}
	if captured.BaseAmount != "0.01" {
		t.Errorf("captured base_amount = %q", captured.BaseAmount)
	}
	if captured.Price != "1201000" {
		t.Errorf("captured price = %q", captured.Price)
	}
	// identifier == ClientOrderID.
	if captured.Identifier != "cid-abc" {
		t.Errorf("captured identifier = %q, want cid-abc", captured.Identifier)
	}
	if captured.Symbol != "BTC_IRT" {
		t.Errorf("captured symbol = %q, want BTC_IRT", captured.Symbol)
	}
	if ack.ExchangeOrderID != "987654" {
		t.Errorf("ack ExchangeOrderID = %q, want 987654", ack.ExchangeOrderID)
	}
	if ack.ClientOrderID != "cid-abc" {
		t.Errorf("ack ClientOrderID = %q", ack.ClientOrderID)
	}
}

func TestBitpinPlaceOrderNoForcedDefaults(t *testing.T) {
	var captured bitpinPlaceOrderRequest
	srv := bitpinTestServer(t, &captured)
	defer srv.Close()
	priv := bitpinTestPrivate(t, srv)

	// Owner leaves OrderType and TimeInForce empty: we must not inject a TIF,
	// and only default the type to "limit".
	req := execution.OrderRequest{
		Symbol:   "BTC/IRT",
		Side:     execution.SideSell,
		Quantity: decimal.RequireFromString("0.02"),
	}
	if _, err := priv.PlaceOrder(context.Background(), req); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if captured.TimeInForce != "" {
		t.Errorf("TIF must stay empty when owner sets none, got %q", captured.TimeInForce)
	}
	if captured.Type != "limit" {
		t.Errorf("type default = %q, want limit", captured.Type)
	}
}

func TestBitpinCancelOrder(t *testing.T) {
	srv := bitpinTestServer(t, nil)
	defer srv.Close()
	priv := bitpinTestPrivate(t, srv)

	// numeric id and identifier string both accepted.
	if err := priv.CancelOrder(context.Background(), "987654"); err != nil {
		t.Errorf("CancelOrder by numeric id: %v", err)
	}
	if err := priv.CancelOrder(context.Background(), "cid-abc"); err != nil {
		t.Errorf("CancelOrder by identifier: %v", err)
	}
	if err := priv.CancelOrder(context.Background(), ""); err == nil {
		t.Error("CancelOrder with empty id should error")
	}
}

func TestBitpinGetOrderStatusMapping(t *testing.T) {
	srv := bitpinTestServer(t, nil)
	defer srv.Close()
	priv := bitpinTestPrivate(t, srv)

	// Numeric id -> fully filled (closed, dealed == base).
	st, err := priv.GetOrder(context.Background(), "987654")
	if err != nil {
		t.Fatalf("GetOrder numeric: %v", err)
	}
	if st.Status != execution.StateFilled {
		t.Errorf("status = %q, want filled", st.Status)
	}
	if st.FilledQty.String() != "0.01" {
		t.Errorf("filled = %s", st.FilledQty)
	}
	if st.Symbol != "BTC/IRT" {
		t.Errorf("symbol = %q, want BTC/IRT", st.Symbol)
	}
	if st.FeeAsset != "BTC" {
		t.Errorf("fee asset = %q", st.FeeAsset)
	}

	// Identifier string (top-level array) -> active with partial fill.
	st2, err := priv.GetOrder(context.Background(), "some-cid")
	if err != nil {
		t.Fatalf("GetOrder identifier: %v", err)
	}
	if st2.Status != execution.StatePartiallyFilled {
		t.Errorf("status = %q, want partially_filled", st2.Status)
	}
	if st2.RemainingQty.String() != "0.015" {
		t.Errorf("remaining = %s", st2.RemainingQty)
	}
}

func TestBitpinGetOpenOrders(t *testing.T) {
	srv := bitpinTestServer(t, nil)
	defer srv.Close()
	priv := bitpinTestPrivate(t, srv)

	orders, err := priv.GetOpenOrders(context.Background(), "BTC/IRT")
	if err != nil {
		t.Fatalf("GetOpenOrders: %v", err)
	}
	// The closed order must be filtered out; only the active one remains.
	if len(orders) != 1 {
		t.Fatalf("got %d open orders, want 1", len(orders))
	}
	if orders[0].ClientOrderID != "open-1" || orders[0].Status != execution.StateOpen {
		t.Errorf("open order mismatch: %+v", orders[0])
	}
}

func TestBitpinTokenAcquisitionAndAuthHeader(t *testing.T) {
	var sawBearer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/usr/authenticate/":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["api_key"] != "k" || body["secret_key"] != "s" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"access":"ACCESS-XYZ","refresh":"R1"}`))
		case "/api/v1/wlt/wallets/":
			sawBearer = r.Header.Get("Authorization")
			_, _ = w.Write([]byte(`{"results":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	priv := bitpinTestPrivate(t, srv)
	if _, err := priv.GetBalances(context.Background()); err != nil {
		t.Fatalf("GetBalances: %v", err)
	}
	if sawBearer != "Bearer ACCESS-XYZ" {
		t.Errorf("Authorization header = %q, want 'Bearer ACCESS-XYZ'", sawBearer)
	}
}

func TestBitpinAuthFailureClassified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"detail":"invalid api key"}`))
	}))
	defer srv.Close()

	priv := bitpinTestPrivate(t, srv)
	_, err := priv.GetBalances(context.Background())
	if err == nil {
		t.Fatal("expected auth error")
	}
	if !errors.Is(err, execution.ErrAuthFailed) {
		t.Errorf("err = %v, want wrapped ErrAuthFailed", err)
	}
	var apiErr *NormalizedAPIError
	if !errors.As(err, &apiErr) || apiErr.Category != CatAuth {
		t.Errorf("err = %v, want *NormalizedAPIError category=auth", err)
	}
}

func TestBitpinRateLimitThrottleParsed(t *testing.T) {
	if d := parseBitpinAuthThrottle("", []byte(`{"detail":"Request was throttled. Expected available in 29 seconds."}`)); d.Seconds() != 29 {
		t.Errorf("body parse = %s, want 29s", d)
	}
	if d := parseBitpinAuthThrottle("12", nil); d.Seconds() != 12 {
		t.Errorf("header parse = %s, want 12s", d)
	}
	if d := parseBitpinAuthThrottle("", nil); d != 0 {
		t.Errorf("empty parse = %s, want 0", d)
	}
}
