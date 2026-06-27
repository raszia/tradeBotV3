package configstore

import "fmt"

// Issue is a single config validation problem. Validation is non-fatal at load
// time (a Snapshot still loads) but issues are surfaced so the dashboard/operator
// can fix them and the engine can refuse to trade a misconfigured market.
type Issue struct {
	Entity string // "market" | "exchange"
	ID     int64
	Field  string
	Msg    string
}

func (i Issue) String() string {
	return fmt.Sprintf("%s#%d %s: %s", i.Entity, i.ID, i.Field, i.Msg)
}

// ValidateMarket checks one market's config (rule #6). Referential integrity
// (the market exists) is guaranteed by the schema FKs, so this focuses on value
// sanity and the enable-flag hierarchy.
func ValidateMarket(m MarketConfig) []Issue {
	var issues []Issue
	add := func(field, msg string) { issues = append(issues, Issue{"market", m.ExchangeMarketID, field, msg}) }

	if m.HasSymbolConfig {
		if m.MinSpreadBps < 0 {
			add("min_spread_bps", "must be non-negative")
		}
		if m.SellOffsetBps < 0 {
			add("sell_offset_bps", "must be non-negative")
		}
		if m.RepriceIntervalSeconds < 0 {
			add("reprice_interval_seconds", "must be non-negative")
		}
		if m.OrderTimeoutMs < 0 {
			add("order_timeout_ms", "must be non-negative")
		}
		if m.MaxRetries < 0 {
			add("max_retries", "must be non-negative")
		}
		if m.RetryBackoffMs < 0 {
			add("retry_backoff_ms", "must be non-negative")
		}
		if m.BuySizeUnit != "" && m.BuySizeUnit != "base" && m.BuySizeUnit != "quote" {
			add("buy_size_unit", "must be 'base' or 'quote'")
		}
		if m.BuySize.IsNegative() {
			add("buy_size", "must not be negative")
		}
	}

	// Trading requires a positive buy size and a symbol config.
	if m.EnabledForTrading {
		if !m.HasSymbolConfig {
			add("enabled_for_trading", "enabled but no symbol_config exists")
		} else if !m.BuySize.IsPositive() {
			add("buy_size", "must be positive when enabled_for_trading is set")
		}
	}

	// Enable-flag hierarchy — prevents a symbol becoming "enabled" through an
	// incomplete config (rule #6): trading ⊆ signal ⊆ collection.
	if m.EnabledForTrading && !m.EnabledForSignal {
		add("enabled_for_signal", "must be set when enabled_for_trading is set")
	}
	if m.EnabledForSignal && !m.EnabledForCollection {
		add("enabled_for_collection", "must be set when enabled_for_signal is set")
	}
	return issues
}

// ValidateExchange checks one exchange's config (rule #6).
func ValidateExchange(e ExchangeConfig) []Issue {
	var issues []Issue
	add := func(field, msg string) { issues = append(issues, Issue{"exchange", e.ExchangeID, field, msg}) }

	if e.MaxConcurrentRequests <= 0 {
		add("max_concurrent_requests", "must be positive")
	}
	if e.RequestTimeoutMs < 0 {
		add("request_timeout_ms", "must be non-negative")
	}
	if e.MaxRetries < 0 {
		add("max_retries", "must be non-negative")
	}
	if e.RetryBackoffMs < 0 {
		add("retry_backoff_ms", "must be non-negative")
	}
	if e.RateLimitPerSec < 0 {
		add("rate_limit_per_sec", "must be non-negative")
	}
	return issues
}

// ValidateSnapshot validates every market and exchange in a snapshot and returns
// all issues (empty == valid).
func ValidateSnapshot(s *Snapshot) []Issue {
	var issues []Issue
	if s == nil {
		return issues
	}
	for _, m := range s.MarketsByID {
		issues = append(issues, ValidateMarket(m)...)
	}
	for _, e := range s.Exchanges {
		issues = append(issues, ValidateExchange(e)...)
	}
	return issues
}
