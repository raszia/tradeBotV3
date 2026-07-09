package configstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

// Config-editing (PR17). Every mutation is VERSIONED + AUDITED + VALIDATED: in one
// transaction it activates a new config_version, updates only the provided fields,
// and writes a config_change_audit row (old/new/operator/reason) per changed field.
// Nothing here trades or touches cycles/orders/queue.

// ErrNoChanges means an update request carried no actual field changes.
var ErrNoChanges = errors.New("configstore: no fields to change")

// ErrStaleConfigVersion is returned when a config edit's expected_config_version does not
// match the current active version — optimistic concurrency: someone else activated a new
// version first, so applying this edit would be a LOST UPDATE. The dashboard maps it to 409.
var ErrStaleConfigVersion = errors.New("configstore: stale expected_config_version (config changed since you loaded it)")

// ErrSellManageExposed is returned when a caller tries to DISABLE sell management for a
// market that still has open/unresolved exposure (an open buy-filled/selling/reconcile
// cycle) — doing so would strand purchased inventory unmanaged. Mapped to 409.
var ErrSellManageExposed = errors.New("configstore: sell management cannot be disabled while open exposure exists")

// openExposureStates are the cycle states that mean a market has UNRESOLVED exposure that
// sell management must keep resolving. Disabling sell management while any cycle for that
// market is in one of these is unsafe. This INCLUDES the in-flight buy states
// (BUY_REQUEST_QUEUED/BUY_SUBMITTED): a buy that is queued or submitted can fill at any
// moment, and if sell management were disabled first that fresh inventory would be
// stranded. Only the truly terminal states (CLOSED/CANCELLED/FAILED) — and the pre-buy NEW/
// SIGNAL_DETECTED states that have not yet placed a buy — are excluded.
var openExposureStates = []string{
	"BUY_REQUEST_QUEUED", "BUY_SUBMITTED", "BUY_PARTIALLY_FILLED", "BUY_FILLED",
	"SELL_REQUEST_QUEUED", "SELL_SUBMITTED", "SELL_REPRICE_PENDING", "SELL_PARTIALLY_FILLED", "SELL_FILLED",
	"CANCEL_PENDING", "NEEDS_RECONCILE",
}

// lockActiveVersion locks the active config_version row FOR UPDATE and verifies it equals
// `expected` (optimistic concurrency). It serializes concurrent edits: the loser blocks,
// then sees the newly-activated version and gets ErrStaleConfigVersion — so two editors who
// both loaded version V can never both commit (no lost update). A missing/zero `expected`
// is rejected (a version is mandatory for every edit).
func lockActiveVersion(ctx context.Context, tx *sql.Tx, expected int64) error {
	if expected <= 0 {
		return invalid("expected_config_version is required (optimistic concurrency)")
	}
	// Lock ALL active rows FOR UPDATE (no LIMIT). Detecting "exactly one active" is a
	// safety invariant: silently picking one of several actives could edit against the
	// wrong config. 0 → ErrNoActiveVersion, 2+ → ErrMultipleActiveVersions, 1 → compare.
	rows, err := tx.QueryContext(ctx, "SELECT id FROM config_versions WHERE status='active' ORDER BY id FOR UPDATE")
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	switch len(ids) {
	case 0:
		return ErrNoActiveVersion
	case 1:
		if ids[0] != expected {
			return ErrStaleConfigVersion
		}
		return nil
	default:
		return fmt.Errorf("%w: ids=%v", ErrMultipleActiveVersions, ids)
	}
}

// hasOpenExposure reports whether the exchange_market has any cycle in an open-exposure
// state (see openExposureStates). Uses idx_cycles_market.
func hasOpenExposure(ctx context.Context, tx *sql.Tx, emID int64) (bool, error) {
	// Build the IN list from the constant slice.
	ph := make([]string, len(openExposureStates))
	args := make([]any, 0, len(openExposureStates)+1)
	args = append(args, emID)
	for i, st := range openExposureStates {
		ph[i] = "?"
		args = append(args, st)
	}
	var n int
	q := "SELECT COUNT(*) FROM cycles WHERE exchange_market_id=? AND state IN (" + strings.Join(ph, ",") + ")"
	if err := tx.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// ValidationError is a rejected config edit (mapped to HTTP 400 by the dashboard).
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, a ...any) error { return &ValidationError{Msg: fmt.Sprintf(format, a...)} }

// ValidationFail builds a validation error (exported for other config domains, e.g.
// regime, to return consistent 400-mapped rejections).
func ValidationFail(format string, a ...any) error { return invalid(format, a...) }

// IsValidation reports whether err is a validation rejection.
func IsValidation(err error) bool {
	var v *ValidationError
	return errors.As(err, &v)
}

// fieldSet accumulates SET clauses + audit entries for changed fields.
type fieldSet struct {
	cols   []string
	args   []any
	audits []AuditEntry
}

func (f *fieldSet) setInt(col string, oldVal sql.NullInt64, newVal *int) {
	if newVal == nil || (oldVal.Valid && int(oldVal.Int64) == *newVal) {
		return
	}
	f.cols = append(f.cols, col+"=?")
	f.args = append(f.args, *newVal)
	f.audits = append(f.audits, AuditEntry{Field: col, OldValue: nullIntStr(oldVal), NewValue: fmt.Sprint(*newVal)})
}

func (f *fieldSet) setBool(col string, oldVal sql.NullBool, newVal *bool) {
	if newVal == nil || (oldVal.Valid && oldVal.Bool == *newVal) {
		return
	}
	f.cols = append(f.cols, col+"=?")
	f.args = append(f.args, b2i(*newVal))
	f.audits = append(f.audits, AuditEntry{Field: col, OldValue: boolStr(oldVal), NewValue: fmt.Sprint(*newVal)})
}

func (f *fieldSet) setStr(col string, oldVal sql.NullString, newVal *string) {
	if newVal == nil || (oldVal.Valid && oldVal.String == *newVal) {
		return
	}
	f.cols = append(f.cols, col+"=?")
	f.args = append(f.args, *newVal)
	f.audits = append(f.audits, AuditEntry{Field: col, OldValue: oldVal.String, NewValue: *newVal})
}

func (f *fieldSet) setDec(col string, oldVal sql.NullString, newVal *string) {
	if newVal == nil {
		return
	}
	if oldVal.Valid && decEq(oldVal.String, *newVal) {
		return
	}
	f.cols = append(f.cols, col+"=?")
	f.args = append(f.args, *newVal)
	f.audits = append(f.audits, AuditEntry{Field: col, OldValue: oldVal.String, NewValue: *newVal})
}

// ---- symbol_configs ----

// SymbolConfigUpdate carries the editable symbol-config fields (nil = unchanged).
type SymbolConfigUpdate struct {
	MinSpreadBps             *int    `json:"min_spread_bps"`
	BuySize                  *string `json:"buy_size"` // decimal
	BuySizeUnit              *string `json:"buy_size_unit"`
	SellOffsetBps            *int    `json:"sell_offset_bps"`
	RepriceIntervalSeconds   *int    `json:"reprice_interval_seconds"`
	OrderTimeoutMs           *int    `json:"order_timeout_ms"`
	MaxRetries               *int    `json:"max_retries"`
	RetryBackoffMs           *int    `json:"retry_backoff_ms"`
	MakerFirstEnabled        *bool   `json:"maker_first_enabled"`
	MakerAttemptsBeforeTaker *int    `json:"maker_attempts_before_taker"`
	MakerSignalWindowSeconds *int    `json:"maker_signal_window_seconds"`
	MakerWaitBeforeCancelMs  *int    `json:"maker_wait_before_cancel_ms"`
	MakerPriceOffsetBps      *int    `json:"maker_price_offset_bps"`
	TakerPriceMode           *string `json:"taker_price_mode"`
	MaxTakerSlippageBps      *int    `json:"max_taker_slippage_bps"`
}

// Validate enforces the PR17 value rules. tradingEnabled gates the buy-size > 0 rule.
func (u SymbolConfigUpdate) Validate() error {
	if u.MinSpreadBps != nil && *u.MinSpreadBps < 0 {
		return invalid("min_spread_bps must be >= 0")
	}
	if u.BuySize != nil {
		d, err := decimal.NewFromString(*u.BuySize)
		if err != nil || !d.IsPositive() {
			return invalid("buy_size must be a positive number")
		}
	}
	if u.BuySizeUnit != nil && *u.BuySizeUnit != "base" && *u.BuySizeUnit != "quote" {
		return invalid("buy_size_unit must be 'base' or 'quote'")
	}
	// The sell price is BinanceReference × (1 − sell_offset_bps/10000): an offset of 10000
	// bps yields a ZERO price and >10000 a NEGATIVE one, which would break sell creation on
	// an already-filled buy. Bound it to [0, 10000).
	if u.SellOffsetBps != nil && (*u.SellOffsetBps < 0 || *u.SellOffsetBps >= 10000) {
		return invalid("sell_offset_bps must be in [0, 10000) (>=10000 would make the sell price zero or negative)")
	}
	if u.RepriceIntervalSeconds != nil && *u.RepriceIntervalSeconds < 0 {
		return invalid("reprice_interval_seconds must be >= 0")
	}
	if u.OrderTimeoutMs != nil && *u.OrderTimeoutMs <= 0 {
		return invalid("order_timeout_ms must be > 0")
	}
	if u.MaxRetries != nil && *u.MaxRetries < 0 {
		return invalid("max_retries must be >= 0")
	}
	if u.RetryBackoffMs != nil && *u.RetryBackoffMs < 0 {
		return invalid("retry_backoff_ms must be >= 0")
	}
	if u.MakerAttemptsBeforeTaker != nil && *u.MakerAttemptsBeforeTaker < 0 {
		return invalid("maker_attempts_before_taker must be >= 0")
	}
	if u.MakerSignalWindowSeconds != nil && *u.MakerSignalWindowSeconds <= 0 {
		return invalid("maker_signal_window_seconds must be > 0")
	}
	// If maker-first is being enabled, a positive signal window is required.
	if u.MakerFirstEnabled != nil && *u.MakerFirstEnabled && u.MakerSignalWindowSeconds != nil && *u.MakerSignalWindowSeconds <= 0 {
		return invalid("maker_signal_window_seconds must be > 0 when maker-first is enabled")
	}
	if u.MakerWaitBeforeCancelMs != nil && *u.MakerWaitBeforeCancelMs < 0 {
		return invalid("maker_wait_before_cancel_ms must be >= 0")
	}
	// The maker-buy price is likewise discounted by maker_price_offset_bps/10000, so an
	// offset >= 10000 would make it zero or negative. Bound it to [0, 10000).
	if u.MakerPriceOffsetBps != nil && (*u.MakerPriceOffsetBps < 0 || *u.MakerPriceOffsetBps >= 10000) {
		return invalid("maker_price_offset_bps must be in [0, 10000) (>=10000 would make the maker-buy price zero or negative)")
	}
	if u.MaxTakerSlippageBps != nil && *u.MaxTakerSlippageBps < 0 {
		return invalid("max_taker_slippage_bps must be >= 0")
	}
	if u.TakerPriceMode != nil && *u.TakerPriceMode != "ASK" {
		return invalid("taker_price_mode must be 'ASK'")
	}
	return nil
}

// UpdateSymbolConfig validates + applies a symbol-config edit (versioned + audited).
// expectedVersion is the config version the editor loaded; a mismatch → ErrStaleConfigVersion
// (optimistic concurrency, no lost updates).
func (s *Store) UpdateSymbolConfig(ctx context.Context, emID int64, u SymbolConfigUpdate, by, reason string, expectedVersion int64) (int64, error) {
	if err := u.Validate(); err != nil {
		return 0, err
	}
	var version int64
	err := s.withTxRetry(ctx, func(tx *sql.Tx) error {
		if err := lockActiveVersion(ctx, tx, expectedVersion); err != nil {
			return err
		}
		var (
			minSpread, sellOff, reprice, oTimeout, maxRetry, backoff, makerAtt, makerWin, makerWait, makerOff, takerSlip sql.NullInt64
			buySize, buyUnit, takerMode                                                                                  sql.NullString
			makerFirst                                                                                                   sql.NullBool
		)
		err := tx.QueryRowContext(ctx, `
SELECT min_spread_bps, buy_size, buy_size_unit, sell_offset_bps, reprice_interval_seconds, order_timeout_ms,
       max_retries, retry_backoff_ms, maker_first_enabled, maker_attempts_before_taker, maker_signal_window_seconds,
       maker_wait_before_cancel_ms, maker_price_offset_bps, taker_price_mode, max_taker_slippage_bps
FROM symbol_configs WHERE exchange_market_id=? FOR UPDATE`, emID).Scan(
			&minSpread, &buySize, &buyUnit, &sellOff, &reprice, &oTimeout, &maxRetry, &backoff,
			&makerFirst, &makerAtt, &makerWin, &makerWait, &makerOff, &takerMode, &takerSlip)
		if errors.Is(err, sql.ErrNoRows) {
			return invalid("no symbol_config for exchange_market %d", emID)
		}
		if err != nil {
			return err
		}
		var fs fieldSet
		fs.setInt("min_spread_bps", minSpread, u.MinSpreadBps)
		fs.setDec("buy_size", buySize, u.BuySize)
		fs.setStr("buy_size_unit", buyUnit, u.BuySizeUnit)
		fs.setInt("sell_offset_bps", sellOff, u.SellOffsetBps)
		fs.setInt("reprice_interval_seconds", reprice, u.RepriceIntervalSeconds)
		fs.setInt("order_timeout_ms", oTimeout, u.OrderTimeoutMs)
		fs.setInt("max_retries", maxRetry, u.MaxRetries)
		fs.setInt("retry_backoff_ms", backoff, u.RetryBackoffMs)
		fs.setBool("maker_first_enabled", makerFirst, u.MakerFirstEnabled)
		fs.setInt("maker_attempts_before_taker", makerAtt, u.MakerAttemptsBeforeTaker)
		fs.setInt("maker_signal_window_seconds", makerWin, u.MakerSignalWindowSeconds)
		fs.setInt("maker_wait_before_cancel_ms", makerWait, u.MakerWaitBeforeCancelMs)
		fs.setInt("maker_price_offset_bps", makerOff, u.MakerPriceOffsetBps)
		fs.setStr("taker_price_mode", takerMode, u.TakerPriceMode)
		fs.setInt("max_taker_slippage_bps", takerSlip, u.MaxTakerSlippageBps)
		var err2 error
		version, err2 = applyFieldSet(ctx, tx, "symbol_configs", "exchange_market_id", emID, "symbol_config", "config_version", fs, by, reason)
		return err2
	})
	return version, err
}

// ---- exchange_markets enable flags (hierarchy enforced) ----

// MarketFlags is the desired enable-flag state (nil = unchanged).
type MarketFlags struct {
	Collection *bool `json:"enabled_for_collection"`
	Signal     *bool `json:"enabled_for_signal"`
	Trading    *bool `json:"enabled_for_trading"`
	SellManage *bool `json:"enabled_for_sell_manage"`
}

// UpdateMarketFlags applies enable-flag changes, enforcing the hierarchy
// trading ⊆ signal ⊆ collection and the trading ⟹ sell_manage invariant.
//
// LOCK ORDER: it locks the exchange_markets row FIRST, then the active config version —
// the SAME order buyflow.CreateBuyCycle uses (it locks the market row FOR UPDATE, then
// takes the config_versions FK lock when it inserts the cycle). Matching the order in both
// paths prevents the classic ABBA deadlock between a concurrent config edit and a buy.
func (s *Store) UpdateMarketFlags(ctx context.Context, emID int64, f MarketFlags, by, reason string, expectedVersion int64) (int64, error) {
	var version int64
	err := s.withTxRetry(ctx, func(tx *sql.Tx) error {
		// 1. Lock the market row FIRST (same as CreateBuyCycle's step 0).
		var col, sig, trd, sell sql.NullBool
		err := tx.QueryRowContext(ctx,
			"SELECT enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage FROM exchange_markets WHERE id=? FOR UPDATE", emID).
			Scan(&col, &sig, &trd, &sell)
		if errors.Is(err, sql.ErrNoRows) {
			return invalid("no exchange_market %d", emID)
		}
		if err != nil {
			return err
		}
		// 2. THEN lock/verify the active config version.
		if err := lockActiveVersion(ctx, tx, expectedVersion); err != nil {
			return err
		}
		// Resolve the effective post-change values, then enforce the hierarchy.
		eff := func(cur sql.NullBool, want *bool) bool {
			if want != nil {
				return *want
			}
			return cur.Bool
		}
		nCol, nSig, nTrd, nSell := eff(col, f.Collection), eff(sig, f.Signal), eff(trd, f.Trading), eff(sell, f.SellManage)
		if nSig && !nCol {
			return invalid("enabled_for_signal requires enabled_for_collection")
		}
		if nTrd && !nSig {
			return invalid("enabled_for_trading requires enabled_for_signal")
		}
		// SAFETY INVARIANT: trading may not be enabled while sell management is disabled —
		// otherwise the engine could open new buys the sell manager ignores, stranding the
		// acquired inventory. Enforced on the EFFECTIVE post-change state, so it also blocks
		// "disable sell_manage" on a market where trading is (or stays) on.
		if nTrd && !nSell {
			return invalid("enabled_for_trading requires enabled_for_sell_manage")
		}
		// SAFETY: never disable sell management while the market still has open exposure —
		// that would strand purchased inventory unmanaged. (Only a real transition to
		// disabled is guarded; leaving it already-off, or a no-op, is fine.)
		if f.SellManage != nil && !*f.SellManage && sell.Bool {
			exposed, err := hasOpenExposure(ctx, tx, emID)
			if err != nil {
				return err
			}
			if exposed {
				return ErrSellManageExposed
			}
		}
		var fs fieldSet
		fs.setBool("enabled_for_collection", col, f.Collection)
		fs.setBool("enabled_for_signal", sig, f.Signal)
		fs.setBool("enabled_for_trading", trd, f.Trading)
		fs.setBool("enabled_for_sell_manage", sell, f.SellManage)
		var err2 error
		version, err2 = applyFieldSet(ctx, tx, "exchange_markets", "id", emID, "exchange_market", "", fs, by, reason)
		return err2
	})
	return version, err
}

// ---- exchange_configs ----

// ExchangeConfigUpdate carries editable per-exchange operational fields.
type ExchangeConfigUpdate struct {
	MaxConcurrentRequests *int `json:"max_concurrent_requests"`
	RequestTimeoutMs      *int `json:"request_timeout_ms"`
	MaxRetries            *int `json:"max_retries"`
	RetryBackoffMs        *int `json:"retry_backoff_ms"`
	RateLimitPerSec       *int `json:"rate_limit_per_sec"`
}

func (u ExchangeConfigUpdate) Validate() error {
	if u.MaxConcurrentRequests != nil && *u.MaxConcurrentRequests <= 0 {
		return invalid("max_concurrent_requests must be > 0")
	}
	if u.RequestTimeoutMs != nil && *u.RequestTimeoutMs <= 0 {
		return invalid("request_timeout_ms must be > 0")
	}
	if u.MaxRetries != nil && *u.MaxRetries < 0 {
		return invalid("max_retries must be >= 0")
	}
	if u.RetryBackoffMs != nil && *u.RetryBackoffMs < 0 {
		return invalid("retry_backoff_ms must be >= 0")
	}
	if u.RateLimitPerSec != nil && *u.RateLimitPerSec < 0 {
		return invalid("rate_limit_per_sec must be >= 0")
	}
	return nil
}

// UpdateExchangeConfig validates + applies an exchange-config edit (versioned+audited).
func (s *Store) UpdateExchangeConfig(ctx context.Context, exchangeID int64, u ExchangeConfigUpdate, by, reason string, expectedVersion int64) (int64, error) {
	if err := u.Validate(); err != nil {
		return 0, err
	}
	var version int64
	err := s.withTxRetry(ctx, func(tx *sql.Tx) error {
		if err := lockActiveVersion(ctx, tx, expectedVersion); err != nil {
			return err
		}
		var maxConc, reqTo, maxRetry, backoff, rate sql.NullInt64
		err := tx.QueryRowContext(ctx,
			"SELECT max_concurrent_requests, request_timeout_ms, max_retries, retry_backoff_ms, rate_limit_per_sec FROM exchange_configs WHERE exchange_id=? FOR UPDATE", exchangeID).
			Scan(&maxConc, &reqTo, &maxRetry, &backoff, &rate)
		if errors.Is(err, sql.ErrNoRows) {
			return invalid("no exchange_config for exchange %d", exchangeID)
		}
		if err != nil {
			return err
		}
		var fs fieldSet
		fs.setInt("max_concurrent_requests", maxConc, u.MaxConcurrentRequests)
		fs.setInt("request_timeout_ms", reqTo, u.RequestTimeoutMs)
		fs.setInt("max_retries", maxRetry, u.MaxRetries)
		fs.setInt("retry_backoff_ms", backoff, u.RetryBackoffMs)
		fs.setInt("rate_limit_per_sec", rate, u.RateLimitPerSec)
		var err2 error
		version, err2 = applyFieldSet(ctx, tx, "exchange_configs", "exchange_id", exchangeID, "exchange_config", "config_version", fs, by, reason)
		return err2
	})
	return version, err
}

// ---- exchange_fees ----

// UpsertFee sets the maker/taker fee for an exchange (exchangeMarketID 0 = exchange
// default), versioned + audited. Fees must be non-negative.
func (s *Store) UpsertFee(ctx context.Context, exchangeID, exchangeMarketID int64, makerFee, takerFee string, by, reason string, expectedVersion int64) (int64, error) {
	mk, err1 := decimal.NewFromString(makerFee)
	tk, err2 := decimal.NewFromString(takerFee)
	if err1 != nil || err2 != nil || mk.IsNegative() || tk.IsNegative() {
		return 0, invalid("maker_fee and taker_fee must be non-negative numbers")
	}
	var version int64
	err := s.withTxRetry(ctx, func(tx *sql.Tx) error {
		if err := lockActiveVersion(ctx, tx, expectedVersion); err != nil {
			return err
		}
		// A market-specific fee's exchange_market_id MUST belong to exchange_id — otherwise
		// a fee for a Wallex market could be filed under Nobitex. Verify ownership first.
		if exchangeMarketID > 0 {
			var owner int64
			err := tx.QueryRowContext(ctx, "SELECT exchange_id FROM exchange_markets WHERE id=?", exchangeMarketID).Scan(&owner)
			if errors.Is(err, sql.ErrNoRows) {
				return invalid("no exchange_market %d", exchangeMarketID)
			}
			if err != nil {
				return err
			}
			if owner != exchangeID {
				return invalid("exchange_market %d does not belong to exchange %d", exchangeMarketID, exchangeID)
			}
		}
		var oldMaker, oldTaker sql.NullString
		var feeID sql.NullInt64
		emArg := nullableMarketID(exchangeMarketID)
		// Only sql.ErrNoRows means "no previous fee". Any other read/scan error must abort
		// the tx BEFORE we activate a version, update a fee, or write a misleading audit.
		if err := tx.QueryRowContext(ctx,
			"SELECT id, maker_fee, taker_fee FROM exchange_fees WHERE exchange_id=? AND ((exchange_market_id IS NULL AND ? IS NULL) OR exchange_market_id=?) ORDER BY id DESC LIMIT 1 FOR UPDATE",
			exchangeID, emArg, emArg).Scan(&feeID, &oldMaker, &oldTaker); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// No-op detection (DECIMAL comparison, not string): a resubmission of the same fees
		// (e.g. 0.001 vs 0.00100000) must NOT mint a version or write audit rows. A new fee
		// (no previous row) always counts as changed. Per-field so only truly-changed fields
		// are audited.
		makerChanged := feeDiffers(oldMaker, mk, feeID.Valid)
		takerChanged := feeDiffers(oldTaker, tk, feeID.Valid)
		if !makerChanged && !takerChanged {
			return ErrNoChanges // returned BEFORE activateVersionTx → no version/fee/audit
		}
		v, err := activateVersionTx(ctx, tx, by, reason)
		if err != nil {
			return err
		}
		version = v
		if feeID.Valid {
			if _, err := tx.ExecContext(ctx, "UPDATE exchange_fees SET maker_fee=?, taker_fee=?, config_version=? WHERE id=?",
				makerFee, takerFee, v, feeID.Int64); err != nil {
				return err
			}
		} else if _, err := tx.ExecContext(ctx,
			"INSERT INTO exchange_fees (exchange_id, exchange_market_id, maker_fee, taker_fee, config_version) VALUES (?, ?, ?, ?, ?)",
			exchangeID, emArg, makerFee, takerFee, v); err != nil {
			return err
		}
		// The audit entity must UNIQUELY identify what changed. A market-specific fee is
		// keyed by its exchange_market_id (so two markets of the same exchange get distinct
		// audit rows); an exchange-wide default is keyed by exchange_id. The reason carries
		// the associated exchange for a market fee.
		entityType, entityID, feeReason := "exchange_default_fee", exchangeID, reason
		if exchangeMarketID > 0 {
			entityType, entityID = "exchange_market_fee", exchangeMarketID
			feeReason = fmt.Sprintf("%s (exchange_id=%d)", reason, exchangeID)
		}
		var audits []AuditEntry
		if makerChanged {
			audits = append(audits, AuditEntry{Field: "maker_fee", OldValue: oldMaker.String, NewValue: makerFee})
		}
		if takerChanged {
			audits = append(audits, AuditEntry{Field: "taker_fee", OldValue: oldTaker.String, NewValue: takerFee})
		}
		for _, a := range audits {
			a.ConfigVersion, a.EntityType, a.EntityID, a.ChangedBy, a.Reason = v, entityType, entityID, by, feeReason
			if err := RecordAudit(ctx, tx, a); err != nil {
				return err
			}
		}
		return nil
	})
	return version, err
}

// feeDiffers reports whether the new fee differs from the stored old fee by DECIMAL value
// (so 0.001 and 0.00100000 are equal). When there is no previous fee row (hasPrev=false)
// or the stored value is null/unparseable, it counts as changed.
func feeDiffers(old sql.NullString, newVal decimal.Decimal, hasPrev bool) bool {
	if !hasPrev || !old.Valid {
		return true
	}
	oldVal, err := decimal.NewFromString(old.String)
	if err != nil {
		return true
	}
	return !oldVal.Equal(newVal)
}

// applyFieldSet mints a new version, applies the changed columns to one row, and
// audits each. stampCol is the row's config_version column (empty when the table has
// none, e.g. exchange_markets — the version still lives on config_versions + audit).
// Returns ErrNoChanges when nothing changed.
func applyFieldSet(ctx context.Context, tx *sql.Tx, table, idCol string, id int64, entityType, stampCol string, fs fieldSet, by, reason string) (int64, error) {
	if len(fs.cols) == 0 {
		return 0, ErrNoChanges
	}
	version, err := activateVersionTx(ctx, tx, by, reason)
	if err != nil {
		return 0, err
	}
	cols := append([]string{}, fs.cols...)
	args := append([]any{}, fs.args...)
	if stampCol != "" {
		cols = append(cols, stampCol+"=?")
		args = append(args, version)
	}
	args = append(args, id)
	q := "UPDATE " + table + " SET " + joinComma(cols) + " WHERE " + idCol + "=?"
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return 0, err
	}
	for _, a := range fs.audits {
		a.ConfigVersion, a.EntityType, a.EntityID, a.ChangedBy, a.Reason = version, entityType, id, by, reason
		if err := RecordAudit(ctx, tx, a); err != nil {
			return 0, err
		}
	}
	return version, nil
}

// AuditHistory returns recent config_change_audit rows (most recent first).
func (s *Store) AuditHistory(ctx context.Context, limit int) ([]AuditEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT COALESCE(config_version,0), entity_type, COALESCE(entity_id,0), COALESCE(field,''), COALESCE(old_value,''), COALESCE(new_value,''), COALESCE(changed_by,''), COALESCE(reason,'') FROM config_change_audit ORDER BY id DESC LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ConfigVersion, &e.EntityType, &e.EntityID, &e.Field, &e.OldValue, &e.NewValue, &e.ChangedBy, &e.Reason); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- small helpers ----

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func boolStr(v sql.NullBool) string {
	if !v.Valid {
		return ""
	}
	return fmt.Sprint(v.Bool)
}

func decEq(a, b string) bool {
	da, err1 := decimal.NewFromString(a)
	db, err2 := decimal.NewFromString(b)
	if err1 != nil || err2 != nil {
		return a == b
	}
	return da.Equal(db)
}

func nullableMarketID(emID int64) any {
	if emID == 0 {
		return nil
	}
	return emID
}
