// Package configstore is the DB-backed, versioned TRADING configuration system.
// It loads operational config (per-market trading params + enable flags,
// per-exchange limits, fees, retention) from MariaDB into an immutable in-memory
// Snapshot, served from a copy-on-write Cache so the trading hot path never
// queries the database. Config changes are versioned (config_versions) and
// audited (config_change_audit).
//
// This is NOT the bootstrap config (internal/config) — that stays a file-only
// thing for DSN/Redis/secrets. Trading parameters and exchange API keys live in
// the database, never in env or the bootstrap file.
package configstore

import (
	"github.com/shopspring/decimal"
)

// MarketConfig is the merged per-exchange-market view: the enable flags live on
// exchange_markets; the trading parameters live on symbol_configs (LEFT JOINed,
// so a market with no symbol_config still appears with HasSymbolConfig=false).
type MarketConfig struct {
	ExchangeMarketID int64
	ExchangeID       int64
	ExchangeCode     string
	CanonicalSymbol  string

	// Per-symbol enable flags (rule #7 — represented here, enforced by the engine
	// in later PRs).
	EnabledForCollection bool
	EnabledForSignal     bool
	EnabledForTrading    bool
	EnabledForSellManage bool

	// Trading parameters (present only when HasSymbolConfig is true).
	HasSymbolConfig        bool
	MinSpreadBps           int
	BuySize                decimal.Decimal
	BuySizeUnit            string // "base" | "quote"
	SellOffsetBps          int
	RepriceIntervalSeconds int
	OrderTimeoutMs         int
	MaxRetries             int
	RetryBackoffMs         int
	SymbolConfigVersion    int64 // the config_version stamped on this symbol_config

	// Venue precision/limits (from exchange_markets) — used by the sell flow to snap
	// price/quantity and enforce minimums. Zero means "not configured / no constraint".
	TickSize         decimal.Decimal
	StepSize         decimal.Decimal
	MinOrderAmount   decimal.Decimal // min notional in quote
	MinOrderQuantity decimal.Decimal // min base quantity

	// Maker-first / taker-fallback buy policy (§2a). Used by PR9 cycle creation.
	Maker MakerPolicy
}

// MakerPolicy is the per-symbol maker-first / taker-fallback buy configuration
// (owner-defined, DB-configurable, versioned). See PROJECT_ARCHITECTURE.md §2a.
type MakerPolicy struct {
	// MakerFirstEnabled: try a maker-style limit below the ask first; if false,
	// every attempt is a taker buy.
	MakerFirstEnabled bool
	// MakerAttemptsBeforeTaker: how many maker attempts (per scope, within the
	// window) before escalating to a taker buy.
	MakerAttemptsBeforeTaker int
	// MakerSignalWindowSeconds: rolling window over which maker attempts are counted.
	MakerSignalWindowSeconds int
	// MakerWaitBeforeCancelMs: simulated-IOC wait before cancelling the remainder
	// (executor uses it in PR10; carried in the request payload).
	MakerWaitBeforeCancelMs int
	// MakerPriceOffsetBps: how far below the ask to place the maker limit.
	MakerPriceOffsetBps int
	// TakerPriceMode: how the taker price is derived (e.g. "ASK").
	TakerPriceMode string
	// MaxTakerSlippageBps: cap on taker price vs the signal price (0 = no cap).
	MaxTakerSlippageBps int
}

// ExchangeConfig is per-exchange operational config (concurrency, timeouts).
type ExchangeConfig struct {
	ExchangeID            int64
	ExchangeCode          string
	MaxConcurrentRequests int
	RequestTimeoutMs      int
	MaxRetries            int
	RetryBackoffMs        int
	RateLimitPerSec       int
	// BalancePollIntervalSeconds is this exchange's balance-sync cadence (0 = use the syncer
	// default). A rate-limited venue can be polled less often; see balance.Config.IntervalFor.
	BalancePollIntervalSeconds int
	ConfigVersion              int64
}

// FeeConfig is a fee schedule entry. ExchangeMarketID == 0 means the
// exchange-wide default for ExchangeID (stored in DefaultFeesByExchangeID).
type FeeConfig struct {
	ExchangeID       int64
	ExchangeMarketID int64
	MakerFee         decimal.Decimal
	TakerFee         decimal.Decimal
	ConfigVersion    int64
}

// RetentionSetting is the retention policy for one high-volume table.
type RetentionSetting struct {
	TableName     string
	RetentionDays int
	MaxRows       int64
	MaxTotalBytes int64
	Enabled       bool
	ConfigVersion int64
}

// Snapshot is an immutable point-in-time view of all trading config. It is
// produced by Store.LoadSnapshot and swapped atomically into the Cache. Readers
// MUST treat it as read-only (never mutate its maps/slices) — copy-on-write means
// a reload publishes a brand-new Snapshot rather than mutating this one.
type Snapshot struct {
	// Version is the active config_versions id at load time (0 if none active).
	Version int64

	MarketsByID     map[int64]MarketConfig    // keyed by exchange_market_id
	MarketsBySymbol map[string][]MarketConfig // keyed by canonical symbol (across exchanges)
	Exchanges       map[string]ExchangeConfig // keyed by exchange code

	// Fees are split into market-specific overrides and per-exchange defaults so a
	// default fee is NEVER shared across exchanges. A single map keyed by
	// exchange_market_id with 0 == "default" would make every exchange's default
	// collide at key 0 (last write wins). Use FeeFor to resolve the applicable fee.
	FeesByMarketID          map[int64]FeeConfig // keyed by exchange_market_id (overrides)
	DefaultFeesByExchangeID map[int64]FeeConfig // keyed by exchange_id (per-exchange default)

	Retention map[string]RetentionSetting

	// LoadedAt is set by the loader (wall clock) for observability; not used for
	// trading decisions.
}

// ConfigVersion returns the active global config version to stamp onto a cycle
// at signal time (rule #5). Per-symbol/exchange versions are on their configs.
func (s *Snapshot) ConfigVersion() int64 {
	if s == nil {
		return 0
	}
	return s.Version
}

// Market returns the config for an exchange_market_id.
func (s *Snapshot) Market(id int64) (MarketConfig, bool) {
	if s == nil {
		return MarketConfig{}, false
	}
	m, ok := s.MarketsByID[id]
	return m, ok
}

// FeeFor resolves the fee that applies to one exchange-market:
//
//  1. a market-specific override keyed by exchangeMarketID, if present; else
//  2. the DEFAULT fee for the SAME exchangeID; else
//  3. not found (false).
//
// It NEVER falls back to another exchange's default — defaults are scoped per
// exchange. The bool is false when no fee (override or default) is configured.
func (s *Snapshot) FeeFor(exchangeID, exchangeMarketID int64) (FeeConfig, bool) {
	if s == nil {
		return FeeConfig{}, false
	}
	if exchangeMarketID != 0 {
		if fc, ok := s.FeesByMarketID[exchangeMarketID]; ok {
			return fc, true
		}
	}
	if fc, ok := s.DefaultFeesByExchangeID[exchangeID]; ok {
		return fc, true
	}
	return FeeConfig{}, false
}

// TradableMarkets returns markets currently enabled_for_trading.
func (s *Snapshot) TradableMarkets() []MarketConfig {
	var out []MarketConfig
	if s == nil {
		return out
	}
	for _, m := range s.MarketsByID {
		if m.EnabledForTrading {
			out = append(out, m)
		}
	}
	return out
}

// emptySnapshot returns a usable, empty snapshot (no active config).
func emptySnapshot() *Snapshot {
	return &Snapshot{
		MarketsByID:             map[int64]MarketConfig{},
		MarketsBySymbol:         map[string][]MarketConfig{},
		Exchanges:               map[string]ExchangeConfig{},
		FeesByMarketID:          map[int64]FeeConfig{},
		DefaultFeesByExchangeID: map[int64]FeeConfig{},
		Retention:               map[string]RetentionSetting{},
	}
}
