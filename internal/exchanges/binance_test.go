package exchanges

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func binanceTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v3/exchangeInfo":
			_, _ = w.Write([]byte(`{"symbols":[
				{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","baseAssetPrecision":8,
				 "quoteAsset":"USDT","quoteAssetPrecision":2,"isSpotTradingAllowed":true,
				 "filters":[
				   {"filterType":"PRICE_FILTER","tickSize":"0.01000000"},
				   {"filterType":"LOT_SIZE","stepSize":"0.00001000","minQty":"0.00001000"},
				   {"filterType":"NOTIONAL","notional":"5.00000000"}]},
				{"symbol":"ETHUSDT","status":"BREAK","baseAsset":"ETH","quoteAsset":"USDT","isSpotTradingAllowed":false,"filters":[]}
			]}`))
		case r.URL.Path == "/api/v3/depth":
			_, _ = w.Write([]byte(`{"bids":[["50000.00","1.5"],["49999.00","2.0"]],"asks":[["50001.00","1.0"],["50002.00","0.5"]]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestBinanceGetMarkets(t *testing.T) {
	srv := binanceTestServer(t)
	defer srv.Close()

	pub, err := newBinancePublic(ClientConfig{Code: binanceCode, BaseURL: srv.URL, HTTPClient: srv.Client()}, nil)
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
	var btc NormalizedMarket
	for _, m := range markets {
		if m.ExchangeSymbol == "BTCUSDT" {
			btc = m
		}
	}
	if btc.CanonicalSymbol != "BTC/USDT" || btc.QuoteAssetType != "USDT" {
		t.Errorf("BTC market mismatch: %+v", btc)
	}
	if !btc.Tradable {
		t.Error("BTCUSDT should be tradable")
	}
	if btc.TickSize.String() != "0.01" || btc.StepSize.String() != "0.00001" {
		t.Errorf("tick/step = %s/%s", btc.TickSize, btc.StepSize)
	}
	if btc.MinOrderAmount.String() != "5" {
		t.Errorf("min notional = %s", btc.MinOrderAmount)
	}
}

func TestBinanceGetOrderBook(t *testing.T) {
	srv := binanceTestServer(t)
	defer srv.Close()

	pub, err := newBinancePublic(ClientConfig{
		Code: binanceCode, BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{"BTC/USDT": "BTCUSDT"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	book, err := pub.GetOrderBook(context.Background(), "BTC/USDT")
	if err != nil {
		t.Fatalf("GetOrderBook: %v", err)
	}
	if book.Symbol != "BTC/USDT" || book.QuoteUnit != "USDT" {
		t.Errorf("book header mismatch: %+v", book)
	}
	bid, ok := book.BestBid()
	if !ok || bid.Price.String() != "50000" {
		t.Errorf("best bid = %v ok=%v", bid, ok)
	}
	ask, ok := book.BestAsk()
	if !ok || ask.Price.String() != "50001" {
		t.Errorf("best ask = %v ok=%v", ask, ok)
	}
}

func TestBinanceRegisteredPublicOnly(t *testing.T) {
	r, ok := Lookup(binanceCode)
	if !ok {
		t.Fatal("binance not registered")
	}
	if r.NewPublic == nil {
		t.Error("binance should have a public constructor")
	}
	if r.NewPrivate != nil {
		t.Error("binance must NOT have a private constructor (read-only reference)")
	}
	if !r.Capabilities.OrderBookREST || r.Capabilities.PlaceOrder {
		t.Errorf("unexpected capabilities: %+v", r.Capabilities)
	}
}
