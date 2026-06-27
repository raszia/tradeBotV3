package exchanges

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"v3TradeBot/internal/execution"
)

// wallexTestServer serves canned Wallex REST responses and (optionally) records
// the last place-order body so tests can assert type/TIF pass-through.
type wallexCapturedPlace struct {
	apiKey string
	body   wallexPlaceOrderRequest
	query  string // raw query string of the cancel request
}

func wallexTestServer(t *testing.T, captured *wallexCapturedPlace) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/markets":
			_, _ = w.Write([]byte(`{"success":true,"result":{"symbols":{
				"USDTTMN":{"symbol":"USDTTMN","baseAsset":"USDT","quoteAsset":"TMN",
				          "stepSize":2,"tickSize":0,"minQty":"0.1","minNotional":"100000"},
				"BTCUSDT":{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT",
				          "stepSize":6,"tickSize":2,"minQty":"0.0001","minNotional":"10"}
			}}}`))

		case r.Method == http.MethodGet && r.URL.Path == "/v1/depth":
			// Wallex quotes in Toman/IRT already.
			_, _ = w.Write([]byte(`{"success":true,"result":{
				"bid":[{"price":"60000","quantity":"1.5"},{"price":"59999","quantity":"2"}],
				"ask":[{"price":"60001","quantity":"1"},{"price":"60002","quantity":"0.5"}]
			}}`))

		case r.Method == http.MethodGet && r.URL.Path == "/v1/account/balances":
			if captured != nil {
				captured.apiKey = r.Header.Get("X-API-Key")
			}
			_, _ = w.Write([]byte(`{"success":true,"result":{"balances":{
				"USDT":{"asset":"USDT","value":"100.5","locked":"0.5"},
				"TMN":{"asset":"TMN","value":"2500000","locked":"0"}
			}}}`))

		case r.Method == http.MethodPost && r.URL.Path == "/v1/account/orders":
			if captured != nil {
				captured.apiKey = r.Header.Get("X-API-Key")
				raw, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(raw, &captured.body)
			}
			_, _ = w.Write([]byte(`{"success":true,"result":{
				"symbol":"USDTTMN","type":"LIMIT","side":"BUY","price":"60000",
				"origQty":"2","executedQty":"0","executedSum":"0","status":"NEW",
				"active":true,"clientOrderId":"my-client-123",
				"created_at":"2026-06-27T10:00:00.000000Z","updated_at":"2026-06-27T10:00:00.000000Z"
			}}`))

		case r.Method == http.MethodDelete && r.URL.Path == "/v1/account/orders":
			if captured != nil {
				captured.query = r.URL.RawQuery
			}
			_, _ = w.Write([]byte(`{"success":true}`))

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/account/orders/"):
			// partially filled, still active
			_, _ = w.Write([]byte(`{"success":true,"result":{
				"symbol":"USDTTMN","type":"LIMIT","side":"BUY","price":"60000",
				"origQty":"2","executedQty":"0.5","executedPrice":"60000","executedSum":"30000",
				"status":"NEW","active":true,"clientOrderId":"my-client-123",
				"created_at":"2026-06-27T10:00:00.000000Z","updated_at":"2026-06-27T10:00:05.000000Z"
			}}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func wallexTestCreds() StaticCredentialProvider {
	return StaticCredentialProvider{Creds: Credentials{APIKey: "test-wallex-key"}}
}

func TestWallexCapabilities(t *testing.T) {
	r, ok := Lookup(wallexCode)
	if !ok {
		t.Fatal("wallex not registered")
	}
	c := r.Capabilities
	if !c.MarketMetadata || !c.OrderBookREST || !c.BalanceFetch || !c.PlaceOrder {
		t.Errorf("core capabilities missing: %+v", c)
	}
	if !c.ClientOrderID {
		t.Error("Wallex must report ClientOrderID=true (orders keyed by client_id)")
	}
	if !c.CancelByOrderID || !c.FetchByOrderID || !c.FetchOpenOrders {
		t.Errorf("order-management capabilities missing: %+v", c)
	}
	if !c.OrderStatusPoll {
		t.Error("Wallex must report OrderStatusPoll=true")
	}
	if c.OrderBookWS || c.OrderUpdatesWS {
		t.Errorf("WebSocket capabilities must be false (deferred): %+v", c)
	}
	if r.NewPublic == nil || r.NewPrivate == nil {
		t.Error("wallex must register both a public and a private constructor")
	}
}

func TestWallexGetMarkets(t *testing.T) {
	srv := wallexTestServer(t, nil)
	defer srv.Close()

	pub, err := newWallexPublic(ClientConfig{Code: wallexCode, BaseURL: srv.URL, HTTPClient: srv.Client()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	markets, err := pub.GetMarkets(context.Background())
	if err != nil {
		t.Fatalf("GetMarkets: %v", err)
	}
	if len(markets) != 2 {
		t.Fatalf("got %d markets, want 2", len(markets))
	}
	var usdt NormalizedMarket
	found := false
	for _, m := range markets {
		if m.BaseAsset == "USDT" {
			usdt = m
			found = true
		}
	}
	if !found {
		t.Fatal("USDT/IRT market not found")
	}
	if usdt.CanonicalSymbol != "USDT/IRT" {
		t.Errorf("canonical = %q, want USDT/IRT (TMN→IRT)", usdt.CanonicalSymbol)
	}
	if usdt.QuoteAsset != "IRT" || usdt.QuoteAssetType != "IRT" {
		t.Errorf("quote = %q type %q, want IRT/IRT", usdt.QuoteAsset, usdt.QuoteAssetType)
	}
	// stepSize=2 → 0.01 step; tickSize=0 → 1 tick.
	if usdt.StepSize.String() != "0.01" {
		t.Errorf("step = %s, want 0.01", usdt.StepSize)
	}
	if usdt.TickSize.String() != "1" {
		t.Errorf("tick = %s, want 1", usdt.TickSize)
	}
	if usdt.MinOrderQuantity.String() != "0.1" || usdt.MinOrderAmount.String() != "100000" {
		t.Errorf("min qty/notional = %s/%s", usdt.MinOrderQuantity, usdt.MinOrderAmount)
	}
	if !usdt.Tradable {
		t.Error("market should be tradable")
	}
}

func TestWallexGetOrderBook(t *testing.T) {
	srv := wallexTestServer(t, nil)
	defer srv.Close()

	pub, err := newWallexPublic(ClientConfig{
		Code: wallexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"USDT/IRT": "USDT/IRT"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	book, err := pub.GetOrderBook(context.Background(), "USDT/IRT")
	if err != nil {
		t.Fatalf("GetOrderBook: %v", err)
	}
	if book.Symbol != "USDT/IRT" || book.QuoteUnit != "IRT" {
		t.Errorf("book header = %s/%s, want USDT/IRT IRT", book.Symbol, book.QuoteUnit)
	}
	bid, ok := book.BestBid()
	if !ok || bid.Price.String() != "60000" {
		t.Errorf("best bid = %v ok=%v (priceMultiplier should be 1, no rescale)", bid, ok)
	}
	ask, ok := book.BestAsk()
	if !ok || ask.Price.String() != "60001" {
		t.Errorf("best ask = %v ok=%v", ask, ok)
	}
}

func TestWallexSubscribeOrderBookUnsupported(t *testing.T) {
	pub, err := newWallexPublic(ClientConfig{Code: wallexCode, BaseURL: "http://example.invalid"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pub.SubscribeOrderBook(context.Background(), []string{"USDT/IRT"}); !isUnsupported(err) {
		t.Errorf("SubscribeOrderBook err = %v, want ErrUnsupported", err)
	}
}

func TestWallexGetBalances(t *testing.T) {
	captured := &wallexCapturedPlace{}
	srv := wallexTestServer(t, captured)
	defer srv.Close()

	priv := newWallexPrivateForTest(t, srv)
	bals, err := priv.GetBalances(context.Background())
	if err != nil {
		t.Fatalf("GetBalances: %v", err)
	}
	if captured.apiKey != "test-wallex-key" {
		t.Errorf("X-API-Key header = %q, want test-wallex-key", captured.apiKey)
	}
	byAsset := map[string]string{}
	for _, b := range bals {
		byAsset[b.Asset] = b.Available.String()
		if b.Exchange != wallexCode {
			t.Errorf("balance exchange = %q", b.Exchange)
		}
	}
	if byAsset["USDT"] != "100.5" {
		t.Errorf("USDT available = %q, want 100.5", byAsset["USDT"])
	}
	// TMN must be normalized to IRT.
	if _, ok := byAsset["TMN"]; ok {
		t.Error("TMN must be normalized to IRT, not surfaced as TMN")
	}
	if byAsset["IRT"] != "2500000" {
		t.Errorf("IRT available = %q, want 2500000 (from TMN)", byAsset["IRT"])
	}
}

func TestWallexPlaceOrderPassesTypeAndTIF(t *testing.T) {
	captured := &wallexCapturedPlace{}
	srv := wallexTestServer(t, captured)
	defer srv.Close()

	priv := newWallexPrivateForTest(t, srv)
	ack, err := priv.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "my-client-123",
		Symbol:        "USDT/IRT",
		Side:          execution.SideBuy,
		Quantity:      mustDec("2"),
		LimitPrice:    mustDec("60000"),
		OrderType:     execution.OrderTypeLimit,
		TimeInForce:   execution.TIFGTC,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	// Faithful payload: native symbol, no forced IOC, type/TIF from the request.
	if captured.body.Symbol != "USDTTMN" {
		t.Errorf("placed symbol = %q, want USDTTMN", captured.body.Symbol)
	}
	if captured.body.Type != "LIMIT" {
		t.Errorf("placed type = %q, want LIMIT (from request, not forced)", captured.body.Type)
	}
	if captured.body.TimeInForce != "GTC" {
		t.Errorf("placed timeInForce = %q, want GTC (request pass-through, NOT forced IOC)", captured.body.TimeInForce)
	}
	if captured.body.Side != "BUY" {
		t.Errorf("placed side = %q, want BUY", captured.body.Side)
	}
	if captured.body.ClientID != "my-client-123" {
		t.Errorf("placed client_id = %q, want my-client-123", captured.body.ClientID)
	}
	if captured.body.Price != "60000" || captured.body.Quantity != "2" {
		t.Errorf("price/qty = %s/%s", captured.body.Price, captured.body.Quantity)
	}

	// Ack mirrors client id into ExchangeOrderID (Wallex keys by client_id).
	if ack.ExchangeOrderID != "my-client-123" || ack.ClientOrderID != "my-client-123" {
		t.Errorf("ack ids = %q/%q, want both my-client-123", ack.ExchangeOrderID, ack.ClientOrderID)
	}
	if ack.Status != execution.StateOpen {
		t.Errorf("ack status = %q, want %q (NEW+active)", ack.Status, execution.StateOpen)
	}
	if ack.Symbol != "USDT/IRT" {
		t.Errorf("ack symbol = %q, want USDT/IRT", ack.Symbol)
	}
}

func TestWallexPlaceOrderDoesNotForceType(t *testing.T) {
	captured := &wallexCapturedPlace{}
	srv := wallexTestServer(t, captured)
	defer srv.Close()

	priv := newWallexPrivateForTest(t, srv)
	// Owner sets a MARKET order with IOC — adapter must forward, not override.
	_, err := priv.PlaceOrder(context.Background(), execution.OrderRequest{
		ClientOrderID: "c2",
		Symbol:        "USDT/IRT",
		Side:          execution.SideSell,
		Quantity:      mustDec("1"),
		OrderType:     execution.OrderTypeMarket,
		TimeInForce:   execution.TIFIOC,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if captured.body.Type != "MARKET" {
		t.Errorf("placed type = %q, want MARKET (no forced LIMIT)", captured.body.Type)
	}
	if captured.body.TimeInForce != "IOC" {
		t.Errorf("placed TIF = %q, want IOC (forwarded as-is)", captured.body.TimeInForce)
	}
}

func TestWallexCancelOrderByClientID(t *testing.T) {
	captured := &wallexCapturedPlace{}
	srv := wallexTestServer(t, captured)
	defer srv.Close()

	priv := newWallexPrivateForTest(t, srv)
	// CancelOrder takes the client id as its "exchangeOrderID" argument (quirk).
	if err := priv.CancelOrder(context.Background(), "my-client-123"); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if !strings.Contains(captured.query, "clientOrderId=my-client-123") {
		t.Errorf("cancel query = %q, want clientOrderId=my-client-123 (keyed by client_id)", captured.query)
	}
}

func TestWallexCancelOrderRequiresID(t *testing.T) {
	srv := wallexTestServer(t, nil)
	defer srv.Close()
	priv := newWallexPrivateForTest(t, srv)
	if err := priv.CancelOrder(context.Background(), ""); err == nil {
		t.Error("CancelOrder with empty id should error")
	}
}

func TestWallexGetOrderStatusMapping(t *testing.T) {
	srv := wallexTestServer(t, nil)
	defer srv.Close()

	priv := newWallexPrivateForTest(t, srv)
	// GetOrder takes the client id as its "exchangeOrderID" argument (quirk).
	st, err := priv.GetOrder(context.Background(), "my-client-123")
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	// Server returns NEW + active + partial fill → partially_filled.
	if st.Status != execution.StatePartiallyFilled {
		t.Errorf("status = %q, want %q", st.Status, execution.StatePartiallyFilled)
	}
	if st.FilledQty.String() != "0.5" || st.RemainingQty.String() != "1.5" {
		t.Errorf("filled/remaining = %s/%s, want 0.5/1.5", st.FilledQty, st.RemainingQty)
	}
	if st.Symbol != "USDT/IRT" {
		t.Errorf("symbol = %q, want USDT/IRT", st.Symbol)
	}
	if st.ExchangeOrderID != "my-client-123" || st.ClientOrderID != "my-client-123" {
		t.Errorf("ids = %q/%q, want both my-client-123", st.ExchangeOrderID, st.ClientOrderID)
	}
}

func TestWallexMapOrderState(t *testing.T) {
	cases := []struct {
		name      string
		status    string
		active    bool
		filled    string
		requested string
		want      execution.NormalizedOrderState
	}{
		{"full fill by qty", "NEW", true, "2", "2", execution.StateFilled},
		{"filled status", "FILLED", false, "0", "0", execution.StateFilled},
		{"new active no fill", "NEW", true, "0", "2", execution.StateOpen},
		{"new active partial", "NEW", true, "0.5", "2", execution.StatePartiallyFilled},
		{"new inactive no fill", "NEW", false, "0", "2", execution.StateCanceled},
		{"new inactive partial", "NEW", false, "0.5", "2", execution.StatePartiallyCanceled},
		{"canceled no fill", "CANCELED", false, "0", "2", execution.StateCanceled},
		{"canceled partial", "CANCELLED", false, "0.5", "2", execution.StatePartiallyCanceled},
		{"rejected", "REJECTED", false, "0", "2", execution.StateRejected},
		{"expired", "EXPIRED", false, "0", "2", execution.StateExpired},
		{"partial active", "PARTIALLY_FILLED", true, "0.5", "2", execution.StatePartiallyFilled},
		{"partial inactive", "PARTIALLY_FILLED", false, "0.5", "2", execution.StatePartiallyCanceled},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mapWallexOrderState(c.status, c.active,
				mustDec(c.filled), mustDec(c.requested))
			if got != c.want {
				t.Errorf("mapWallexOrderState(%s,%v,%s,%s) = %q, want %q",
					c.status, c.active, c.filled, c.requested, got, c.want)
			}
		})
	}
}

func TestWallexAuthFailureMapsSentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":401,"message":"invalid api key"}}`))
	}))
	defer srv.Close()

	priv := newWallexPrivateForTest(t, srv)
	_, err := priv.GetBalances(context.Background())
	if err == nil {
		t.Fatal("expected auth error")
	}
	if !isSentinel(err, execution.ErrAuthFailed) {
		t.Errorf("err = %v, want wrapping execution.ErrAuthFailed", err)
	}
}

func TestWallexMissingCredentials(t *testing.T) {
	srv := wallexTestServer(t, nil)
	defer srv.Close()
	priv, err := newWallexPrivate(ClientConfig{
		Code: wallexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Creds: StaticCredentialProvider{Creds: Credentials{APIKey: ""}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := priv.GetBalances(context.Background()); !isSentinel(err, execution.ErrAuthFailed) {
		t.Errorf("missing key err = %v, want ErrAuthFailed", err)
	}
}

// ─── test helpers ──────────────────────────────────────────────────────────────

func newWallexPrivateForTest(t *testing.T, srv *httptest.Server) PrivateClient {
	t.Helper()
	priv, err := newWallexPrivate(ClientConfig{
		Code: wallexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"USDT/IRT": "USDT/IRT"},
		Creds:   wallexTestCreds(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}
