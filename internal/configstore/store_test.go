package configstore

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
)

func mustDec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestActiveVersion(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	s := New(mockDB)

	mock.ExpectQuery("FROM config_versions WHERE status = 'active'").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(5)))
	if v, err := s.ActiveVersion(context.Background()); err != nil || v != 5 {
		t.Fatalf("ActiveVersion = %d, %v", v, err)
	}

	mock.ExpectQuery("FROM config_versions WHERE status = 'active'").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	if _, err := s.ActiveVersion(context.Background()); !errors.Is(err, ErrNoActiveVersion) {
		t.Fatalf("expected ErrNoActiveVersion, got %v", err)
	}
}

func TestLoadMarkets(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	s := New(mockDB)

	cols := []string{"id", "exchange_id", "code", "canonical_symbol",
		"enabled_for_collection", "enabled_for_signal", "enabled_for_trading", "enabled_for_sell_manage",
		"sc_id", "min_spread_bps", "buy_size", "buy_size_unit", "sell_offset_bps",
		"reprice_interval_seconds", "order_timeout_ms", "max_retries", "retry_backoff_ms", "config_version",
		"maker_first_enabled", "maker_attempts_before_taker", "maker_signal_window_seconds",
		"maker_wait_before_cancel_ms", "maker_price_offset_bps", "taker_price_mode", "max_taker_slippage_bps",
		"tick_size", "step_size", "min_order_amount", "min_order_quantity"}
	rows := sqlmock.NewRows(cols).
		// market 1 with a symbol_config (maker-first, escalate after 2 attempts)
		AddRow(int64(1), int64(3), "nobitex", "BTC/IRT", 1, 1, 1, 1,
			int64(1), int64(50), "0.001", "base", int64(30),
			int64(5), int64(3000), int64(3), int64(500), int64(7),
			1, int64(2), int64(90), int64(2500), int64(8), "ASK", int64(40),
			"0.01", "0.0001", "10", "0.001").
		// market 2 with NO symbol_config (LEFT JOIN nulls); market still has venue precision
		AddRow(int64(2), int64(4), "wallex", "ETH/IRT", 1, 0, 0, 1,
			nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil,
			nil, nil, nil, nil)
	mock.ExpectQuery("FROM exchange_markets").WillReturnRows(rows)

	snap := emptySnapshot()
	if err := s.loadMarkets(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	m1 := snap.MarketsByID[1]
	if !m1.HasSymbolConfig || m1.MinSpreadBps != 50 || m1.BuySizeUnit != "base" || m1.SellOffsetBps != 30 {
		t.Errorf("market 1 = %+v", m1)
	}
	if !m1.EnabledForTrading || !m1.BuySize.Equal(mustDec("0.001")) {
		t.Errorf("market 1 flags/size = %+v", m1)
	}
	if !m1.Maker.MakerFirstEnabled || m1.Maker.MakerAttemptsBeforeTaker != 2 ||
		m1.Maker.MakerPriceOffsetBps != 8 || m1.Maker.TakerPriceMode != "ASK" || m1.Maker.MaxTakerSlippageBps != 40 {
		t.Errorf("market 1 maker policy = %+v", m1.Maker)
	}
	if !m1.TickSize.Equal(mustDec("0.01")) || !m1.StepSize.Equal(mustDec("0.0001")) ||
		!m1.MinOrderAmount.Equal(mustDec("10")) || !m1.MinOrderQuantity.Equal(mustDec("0.001")) {
		t.Errorf("market 1 precision = tick %s step %s minAmt %s minQty %s", m1.TickSize, m1.StepSize, m1.MinOrderAmount, m1.MinOrderQuantity)
	}
	m2 := snap.MarketsByID[2]
	if m2.HasSymbolConfig {
		t.Errorf("market 2 should have no symbol_config: %+v", m2)
	}
	// market 2 (no symbol_config) falls back to safe maker defaults.
	if !m2.Maker.MakerFirstEnabled || m2.Maker.MakerAttemptsBeforeTaker != 1 || m2.Maker.MakerPriceOffsetBps != 5 {
		t.Errorf("market 2 maker defaults = %+v", m2.Maker)
	}
	if len(snap.MarketsBySymbol["BTC/IRT"]) != 1 {
		t.Errorf("MarketsBySymbol not populated")
	}
}

func TestLoadExchangeConfigsAndRetention(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	s := New(mockDB)

	mock.ExpectQuery("FROM exchange_configs").WillReturnRows(
		sqlmock.NewRows([]string{"exchange_id", "code", "max_concurrent_requests", "request_timeout_ms", "max_retries", "retry_backoff_ms", "rate_limit_per_sec", "balance_poll_interval_seconds", "config_version"}).
			AddRow(int64(3), "nobitex", 2, int64(5000), int64(4), int64(250), int64(10), int64(30), int64(7)))
	snap := emptySnapshot()
	if err := s.loadExchangeConfigs(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	ec := snap.Exchanges["nobitex"]
	if ec.MaxConcurrentRequests != 2 || ec.RequestTimeoutMs != 5000 || ec.RateLimitPerSec != 10 || ec.BalancePollIntervalSeconds != 30 {
		t.Errorf("exchange config = %+v", ec)
	}

	mock.ExpectQuery("FROM retention_settings").WillReturnRows(
		sqlmock.NewRows([]string{"table_name", "retention_days", "max_rows", "max_total_bytes", "enabled", "config_version"}).
			AddRow("api_call_logs", 7, nil, nil, 1, int64(7)))
	if err := s.loadRetention(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	if r := snap.Retention["api_call_logs"]; r.RetentionDays != 7 || !r.Enabled {
		t.Errorf("retention = %+v", r)
	}
}

func TestActivateVersion(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	s := New(mockDB)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE config_versions SET status='superseded'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO config_versions").WillReturnResult(sqlmock.NewResult(9, 1))
	mock.ExpectCommit()

	id, err := s.ActivateVersion(context.Background(), "admin", "tune spreads")
	if err != nil || id != 9 {
		t.Fatalf("ActivateVersion = %d, %v", id, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateMinSpreadBpsIsVersionedAndAudited(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	s := New(mockDB)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT min_spread_bps FROM symbol_configs").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"min_spread_bps"}).AddRow(int64(40)))
	mock.ExpectExec("UPDATE config_versions SET status='superseded'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO config_versions").WillReturnResult(sqlmock.NewResult(10, 1))
	mock.ExpectExec("UPDATE symbol_configs SET min_spread_bps").
		WithArgs(60, int64(10), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO config_change_audit").
		WithArgs(int64(10), "symbol_config", int64(1), "min_spread_bps", "40", "60", "admin", "widen").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	version, err := s.UpdateMinSpreadBps(context.Background(), 1, 60, "admin", "widen")
	if err != nil || version != 10 {
		t.Fatalf("UpdateMinSpreadBps = %d, %v", version, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
