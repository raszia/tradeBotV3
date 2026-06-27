package configstore

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func validMarket() MarketConfig {
	return MarketConfig{
		ExchangeMarketID:       1,
		CanonicalSymbol:        "BTC/IRT",
		EnabledForCollection:   true,
		EnabledForSignal:       true,
		EnabledForTrading:      true,
		EnabledForSellManage:   true,
		HasSymbolConfig:        true,
		MinSpreadBps:           50,
		BuySize:                decimal.RequireFromString("0.01"),
		BuySizeUnit:            "quote",
		SellOffsetBps:          20,
		RepriceIntervalSeconds: 5,
		OrderTimeoutMs:         3000,
		MaxRetries:             3,
		RetryBackoffMs:         500,
	}
}

func hasIssue(issues []Issue, field string) bool {
	for _, i := range issues {
		if i.Field == field {
			return true
		}
	}
	return false
}

func TestValidateMarketValid(t *testing.T) {
	if issues := ValidateMarket(validMarket()); len(issues) != 0 {
		t.Fatalf("valid market reported issues: %v", issues)
	}
}

func TestValidateMarketValueRules(t *testing.T) {
	m := validMarket()
	m.MinSpreadBps = -1
	m.SellOffsetBps = -5
	m.RepriceIntervalSeconds = -1
	m.MaxRetries = -2
	m.BuySizeUnit = "weird"
	issues := ValidateMarket(m)
	for _, f := range []string{"min_spread_bps", "sell_offset_bps", "reprice_interval_seconds", "max_retries", "buy_size_unit"} {
		if !hasIssue(issues, f) {
			t.Errorf("expected issue for %s; got %v", f, issues)
		}
	}
}

func TestValidateTradingRequiresPositiveBuySize(t *testing.T) {
	m := validMarket()
	m.BuySize = decimal.Zero
	if !hasIssue(ValidateMarket(m), "buy_size") {
		t.Error("trading with zero buy size should be an issue")
	}

	// trading enabled but no symbol_config at all
	m2 := validMarket()
	m2.HasSymbolConfig = false
	if !hasIssue(ValidateMarket(m2), "enabled_for_trading") {
		t.Error("trading without symbol_config should be an issue")
	}
}

func TestValidateEnableHierarchy(t *testing.T) {
	// trading on but signal off -> issue (prevents accidental enable via incomplete config)
	m := validMarket()
	m.EnabledForSignal = false
	if !hasIssue(ValidateMarket(m), "enabled_for_signal") {
		t.Error("trading without signal should be an issue")
	}
	// signal on but collection off -> issue
	m2 := validMarket()
	m2.EnabledForTrading = false
	m2.EnabledForCollection = false
	if !hasIssue(ValidateMarket(m2), "enabled_for_collection") {
		t.Error("signal without collection should be an issue")
	}
}

func TestValidateDisabledMarketWithNoConfigIsValid(t *testing.T) {
	// A fully-disabled market with no symbol_config must NOT be flagged (it just
	// isn't traded). This guards against "incomplete config" false positives.
	m := MarketConfig{ExchangeMarketID: 9, CanonicalSymbol: "X/IRT"}
	if issues := ValidateMarket(m); len(issues) != 0 {
		t.Fatalf("disabled empty market should be valid, got %v", issues)
	}
}

func TestValidateExchange(t *testing.T) {
	if issues := ValidateExchange(ExchangeConfig{MaxConcurrentRequests: 2}); len(issues) != 0 {
		t.Fatalf("valid exchange reported issues: %v", issues)
	}
	if !hasIssue(ValidateExchange(ExchangeConfig{MaxConcurrentRequests: 0}), "max_concurrent_requests") {
		t.Error("zero concurrency should be an issue")
	}
	if !hasIssue(ValidateExchange(ExchangeConfig{MaxConcurrentRequests: 1, RequestTimeoutMs: -1}), "request_timeout_ms") {
		t.Error("negative timeout should be an issue")
	}
}

func TestValidateSnapshotAggregates(t *testing.T) {
	s := emptySnapshot()
	bad := validMarket()
	bad.MinSpreadBps = -1
	s.MarketsByID[1] = bad
	s.Exchanges["nobitex"] = ExchangeConfig{ExchangeID: 3, MaxConcurrentRequests: 0}
	issues := ValidateSnapshot(s)
	if len(issues) < 2 {
		t.Fatalf("expected aggregated issues, got %v", issues)
	}
	// Issue.String is human readable.
	if !strings.Contains(ValidateSnapshot(s)[0].String(), "#") {
		t.Error("Issue.String should include the entity id")
	}
}
