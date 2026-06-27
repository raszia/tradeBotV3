package collector

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestLoadCollectionMarkets(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	rows := sqlmock.NewRows([]string{"code", "canonical_symbol", "exchange_symbol"}).
		AddRow("binance", "BTC/USDT", "BTCUSDT").
		AddRow("nobitex", "BTC/IRT", "BTCIRT").
		AddRow("nobitex", "ETH/IRT", "ETHIRT")
	mock.ExpectQuery("FROM exchange_markets").WillReturnRows(rows)

	got, err := LoadCollectionMarkets(context.Background(), mockDB)
	if err != nil {
		t.Fatalf("LoadCollectionMarkets: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d markets, want 3", len(got))
	}
	if got[0].ExchangeCode != "binance" || got[0].ExchangeSymbol != "BTCUSDT" {
		t.Errorf("row 0 = %+v", got[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBuildTargetsGroupsByExchangeAndSkipsUnknown(t *testing.T) {
	markets := []CollectionMarket{
		{ExchangeCode: "binance", CanonicalSymbol: "BTC/USDT", ExchangeSymbol: "BTCUSDT"},
		{ExchangeCode: "nobitex", CanonicalSymbol: "BTC/IRT", ExchangeSymbol: "BTCIRT"},
		{ExchangeCode: "nobitex", CanonicalSymbol: "ETH/IRT", ExchangeSymbol: "ETHIRT"},
		{ExchangeCode: "no_such_exchange", CanonicalSymbol: "X/Y", ExchangeSymbol: "XY"},
	}
	targets, err := BuildTargets(markets, nil)
	if err != nil {
		t.Fatalf("BuildTargets: %v", err)
	}
	// binance + nobitex (real registered public adapters); unknown skipped.
	if len(targets) != 2 {
		t.Fatalf("got %d targets, want 2 (unknown exchange skipped)", len(targets))
	}
	byCode := map[string]Target{}
	for _, tg := range targets {
		byCode[tg.ExchangeCode] = tg
	}
	if len(byCode["nobitex"].Symbols) != 2 {
		t.Errorf("nobitex should have 2 symbols, got %v", byCode["nobitex"].Symbols)
	}
	if byCode["binance"].Client == nil || byCode["binance"].Client.Name() != "binance" {
		t.Errorf("binance client not built correctly")
	}
}
