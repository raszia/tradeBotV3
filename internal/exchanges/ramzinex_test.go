package exchanges

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func ramzinexTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/exchange/api/v2.0/exchange/pairs":
			_, _ = w.Write([]byte(`{"status":0,"data":{"pairs":[
				{"id":11,"base_currency_symbol":{"en":"USDT"},"quote_currency_symbol":{"en":"IRR"},
				 "trading_chart_settings":{"ramzinex":"usdtirt"}},
				{"id":2,"base_currency_symbol":{"en":"BTC"},"quote_currency_symbol":{"en":"IRR"},
				 "trading_chart_settings":{"ramzinex":"btcirt"}}
			]}}`))
		case strings.HasPrefix(r.URL.Path, "/exchange/api/v1.0/exchange/orderbooks/11/buys_sells"):
			// Prices are in RIAL. buys (bids) and sells (asks) are [price, qty, ...].
			// Sells are highest-first (descending) as the venue returns them.
			_, _ = w.Write([]byte(`{"status":0,"data":{
				"buys":[["599000","2.0",0],["598000","3.0",0]],
				"sells":[["602000","0.7",0],["601000","1.0",0]]
			}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestRamzinexGetOrderBook(t *testing.T) {
	srv := ramzinexTestServer(t)
	defer srv.Close()

	pub, err := newRamzinexPublic(ClientConfig{
		Code: ramzinexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"USDT/IRT": "usdtirt"},
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
	// Rial -> toman: 599000 * 0.1 = 59900 (best bid, highest after sort).
	bid, ok := book.BestBid()
	if !ok || bid.Price.String() != "59900" {
		t.Errorf("best bid = %v ok=%v want 59900", bid.Price, ok)
	}
	if book.Bids[1].Price.String() != "59800" {
		t.Errorf("second bid = %s want 59800", book.Bids[1].Price)
	}
	// Best ask = lowest sell: 601000 * 0.1 = 60100.
	ask, ok := book.BestAsk()
	if !ok || ask.Price.String() != "60100" {
		t.Errorf("best ask = %v ok=%v want 60100", ask.Price, ok)
	}
	if book.Asks[1].Price.String() != "60200" {
		t.Errorf("second ask = %s want 60200", book.Asks[1].Price)
	}
	if bid.Quantity.String() != "2" {
		t.Errorf("best bid qty = %s want 2", bid.Quantity)
	}
}

func TestRamzinexGetMarkets(t *testing.T) {
	srv := ramzinexTestServer(t)
	defer srv.Close()

	pub, err := newRamzinexPublic(ClientConfig{
		Code: ramzinexCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
	}, nil)
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
	for _, m := range markets {
		if m.BaseAsset == "USDT" {
			usdt = m
		}
	}
	// IRR quote must be normalized to IRT.
	if usdt.CanonicalSymbol != "USDT/IRT" || usdt.QuoteAsset != "IRT" || usdt.QuoteAssetType != "IRT" {
		t.Errorf("USDT market mismatch: %+v", usdt)
	}
	if usdt.ExchangeSymbol != "usdtirt" {
		t.Errorf("exchange symbol = %s want usdtirt", usdt.ExchangeSymbol)
	}
}

func TestRamzinexSubscribeOrderBookUnsupported(t *testing.T) {
	pub, err := newRamzinexPublic(ClientConfig{Code: ramzinexCode}, nil)
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

func TestRamzinexRegisteredPublicOnly(t *testing.T) {
	r, ok := Lookup(ramzinexCode)
	if !ok {
		t.Fatal("ramzinex not registered")
	}
	if r.NewPublic == nil {
		t.Error("ramzinex should have a public constructor")
	}
	if r.NewPrivate != nil {
		t.Error("ramzinex must NOT have a private constructor (public-only)")
	}
	if !r.Capabilities.OrderBookREST || !r.Capabilities.MarketMetadata {
		t.Errorf("expected OrderBookREST+MarketMetadata: %+v", r.Capabilities)
	}
	if r.Capabilities.OrderBookWS || r.Capabilities.PlaceOrder {
		t.Errorf("unexpected capabilities: %+v", r.Capabilities)
	}
}
