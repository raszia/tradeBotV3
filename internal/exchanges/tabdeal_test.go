package exchanges

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func tabdealTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/r/api/order_book":
			if r.URL.Query().Get("symbol") != "USDT_IRT" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			// Prices already in TOMAN (IRT). asks not necessarily sorted; bids descending.
			_, _ = w.Write([]byte(`{
				"asks":[{"price":"60200","amount":"1.0"},{"price":"60100","amount":"0.5"}],
				"bids":[{"price":"59800","amount":"3.0"},{"price":"59900","amount":"2.0"}]
			}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestTabdealGetOrderBook(t *testing.T) {
	srv := tabdealTestServer(t)
	defer srv.Close()

	pub, err := newTabdealPublic(ClientConfig{
		Code: tabdealCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"USDT/IRT": "USDT_IRT"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	book, err := pub.GetOrderBook(context.Background(), "USDT/IRT")
	if err != nil {
		t.Fatalf("GetOrderBook: %v", err)
	}
	if book.Symbol != "USDT/IRT" || book.QuoteUnit != "IRT" {
		t.Errorf("book header mismatch: symbol=%s quote=%s", book.Symbol, book.QuoteUnit)
	}
	// No rial->toman conversion: prices pass through unchanged. Sorted: best bid highest.
	bid, ok := book.BestBid()
	if !ok || bid.Price.String() != "59900" {
		t.Errorf("best bid = %v ok=%v want 59900", bid.Price, ok)
	}
	if book.Bids[1].Price.String() != "59800" {
		t.Errorf("second bid = %s want 59800", book.Bids[1].Price)
	}
	// Best ask = lowest ask after sort.
	ask, ok := book.BestAsk()
	if !ok || ask.Price.String() != "60100" {
		t.Errorf("best ask = %v ok=%v want 60100", ask.Price, ok)
	}
	if book.Asks[1].Price.String() != "60200" {
		t.Errorf("second ask = %s want 60200", book.Asks[1].Price)
	}
}

func TestTabdealDerivedSymbol(t *testing.T) {
	srv := tabdealTestServer(t)
	defer srv.Close()

	// No cfg.Symbols mapping: native symbol must be derived (USDT/IRT -> USDT_IRT).
	pub, err := newTabdealPublic(ClientConfig{
		Code: tabdealCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	book, err := pub.GetOrderBook(context.Background(), "USDT/IRT")
	if err != nil {
		t.Fatalf("GetOrderBook with derived symbol: %v", err)
	}
	if len(book.Bids) == 0 || len(book.Asks) == 0 {
		t.Errorf("expected non-empty book, got %+v", book)
	}
}

func TestTabdealGetMarketsUnsupported(t *testing.T) {
	pub, err := newTabdealPublic(ClientConfig{Code: tabdealCode}, nil)
	if err != nil {
		t.Fatal(err)
	}
	markets, err := pub.GetMarkets(context.Background())
	if markets != nil {
		t.Error("expected nil markets for unsupported GetMarkets")
	}
	if !errors.Is(err, ErrUnsupported) {
		t.Errorf("expected ErrUnsupported, got %v", err)
	}
}

func TestTabdealSubscribeOrderBookUnsupported(t *testing.T) {
	pub, err := newTabdealPublic(ClientConfig{Code: tabdealCode}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := pub.SubscribeOrderBook(context.Background(), []string{"USDT/IRT"})
	if ch != nil {
		t.Error("expected nil channel for unsupported WS")
	}
	if !errors.Is(err, ErrUnsupported) {
		t.Errorf("expected ErrUnsupported, got %v", err)
	}
}

func TestTabdealRegisteredPublicOnly(t *testing.T) {
	r, ok := Lookup(tabdealCode)
	if !ok {
		t.Fatal("tabdeal not registered")
	}
	if r.NewPublic == nil {
		t.Error("tabdeal should have a public constructor")
	}
	if r.NewPrivate != nil {
		t.Error("tabdeal must NOT have a private constructor (public-only)")
	}
	if !r.Capabilities.OrderBookREST {
		t.Errorf("expected OrderBookREST: %+v", r.Capabilities)
	}
	if r.Capabilities.MarketMetadata || r.Capabilities.OrderBookWS || r.Capabilities.PlaceOrder {
		t.Errorf("unexpected capabilities: %+v", r.Capabilities)
	}
}
