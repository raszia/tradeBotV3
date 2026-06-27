package exchanges

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func exirTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/orderbooks":
			// Whole-book payload keyed by native symbol. Prices already in TOMAN.
			// Levels are [price, qty] float arrays; bids/asks unsorted on purpose.
			_, _ = w.Write([]byte(`{
				"usdt-irt":{
					"bids":[[59800,3.0],[59900,2.0]],
					"asks":[[60200,1.0],[60100,0.5]],
					"timestamp":"2026-06-27T10:00:00Z"
				},
				"btc-irt":{"bids":[[1,1]],"asks":[[2,1]]}
			}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestExirGetOrderBook(t *testing.T) {
	srv := exirTestServer(t)
	defer srv.Close()

	pub, err := newExirPublic(ClientConfig{
		Code: exirCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"USDT/IRT": "usdt-irt"},
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
	// No conversion: toman passes through. Sorted: best bid highest.
	bid, ok := book.BestBid()
	if !ok || bid.Price.String() != "59900" {
		t.Errorf("best bid = %v ok=%v want 59900", bid.Price, ok)
	}
	if book.Bids[1].Price.String() != "59800" {
		t.Errorf("second bid = %s want 59800", book.Bids[1].Price)
	}
	ask, ok := book.BestAsk()
	if !ok || ask.Price.String() != "60100" {
		t.Errorf("best ask = %v ok=%v want 60100", ask.Price, ok)
	}
	if book.Asks[1].Price.String() != "60200" {
		t.Errorf("second ask = %s want 60200", book.Asks[1].Price)
	}
	if book.UpdatedAt.IsZero() {
		t.Error("expected timestamp from payload, got zero")
	}
}

func TestExirDerivedSymbolMatch(t *testing.T) {
	srv := exirTestServer(t)
	defer srv.Close()

	// No cfg.Symbols mapping: native derived as "usdt-irt"; matching is
	// separator/case-insensitive so it still resolves the "usdt-irt" key.
	pub, err := newExirPublic(ClientConfig{
		Code: exirCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
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

func TestExirGetOrderBookUnknownSymbol(t *testing.T) {
	srv := exirTestServer(t)
	defer srv.Close()

	pub, err := newExirPublic(ClientConfig{
		Code: exirCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pub.GetOrderBook(context.Background(), "ETH/IRT"); err == nil {
		t.Error("expected error for symbol missing from payload")
	}
}

func TestExirGetMarketsUnsupported(t *testing.T) {
	pub, err := newExirPublic(ClientConfig{Code: exirCode}, nil)
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

func TestExirSubscribeOrderBookUnsupported(t *testing.T) {
	pub, err := newExirPublic(ClientConfig{Code: exirCode}, nil)
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

func TestExirRegisteredPublicOnly(t *testing.T) {
	r, ok := Lookup(exirCode)
	if !ok {
		t.Fatal("exir not registered")
	}
	if r.NewPublic == nil {
		t.Error("exir should have a public constructor")
	}
	if r.NewPrivate != nil {
		t.Error("exir must NOT have a private constructor (public-only)")
	}
	if !r.Capabilities.OrderBookREST {
		t.Errorf("expected OrderBookREST: %+v", r.Capabilities)
	}
	if r.Capabilities.MarketMetadata || r.Capabilities.OrderBookWS || r.Capabilities.PlaceOrder {
		t.Errorf("unexpected capabilities: %+v", r.Capabilities)
	}
}
