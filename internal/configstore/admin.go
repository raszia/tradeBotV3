package configstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
)

// Config-editing (PR17). Every mutation is VERSIONED + AUDITED + VALIDATED: in one
// transaction it activates a new config_version, updates only the provided fields,
// and writes a config_change_audit row (old/new/operator/reason) per changed field.
// Nothing here trades or touches cycles/orders/queue.

// ErrNoChanges means an update request carried no actual field changes.
var ErrNoChanges = errors.New("configstore: no fields to change")

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
	if u.SellOffsetBps != nil && *u.SellOffsetBps < 0 {
		return invalid("sell_offset_bps must be >= 0")
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
	if u.MakerPriceOffsetBps != nil && *u.MakerPriceOffsetBps < 0 {
		return invalid("maker_price_offset_bps must be >= 0")
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
func (s *Store) UpdateSymbolConfig(ctx context.Context, emID int64, u SymbolConfigUpdate, by, reason string) (int64, error) {
	if err := u.Validate(); err != nil {
		return 0, err
	}
	var version int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var (
			minSpread, sellOff, reprice, oTimeout, maxRetry, backoff, makerAtt, makerWin, makerWait, makerOff, takerSlip sql.NullInt64
			buySize, buyUnit, takerMode                                                                                  sql.NullString
			makerFirst                                                                                                   sql.NullBool
		)
		err := tx.QueryRowContext(ctx, `
SELECT min_spread_bps, buy_size, buy_size_unit, sell_offset_bps, reprice_interval_seconds, order_timeout_ms,
       max_retries, retry_backoff_ms, maker_first_enabled, maker_attempts_before_taker, maker_signal_window_seconds,
       maker_wait_before_cancel_ms, maker_price_offset_bps, taker_price_mode, max_taker_slippage_bps
FROM symbol_configs WHERE exchange_market_id=?`, emID).Scan(
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
// trading ⊆ signal ⊆ collection. (sell_manage is independent so existing cycles can
// keep being managed even when new-cycle trading is turned off.)
func (s *Store) UpdateMarketFlags(ctx context.Context, emID int64, f MarketFlags, by, reason string) (int64, error) {
	var version int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var col, sig, trd, sell sql.NullBool
		err := tx.QueryRowContext(ctx,
			"SELECT enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage FROM exchange_markets WHERE id=?", emID).
			Scan(&col, &sig, &trd, &sell)
		if errors.Is(err, sql.ErrNoRows) {
			return invalid("no exchange_market %d", emID)
		}
		if err != nil {
			return err
		}
		// Resolve the effective post-change values, then enforce the hierarchy.
		eff := func(cur sql.NullBool, want *bool) bool {
			if want != nil {
				return *want
			}
			return cur.Bool
		}
		nCol, nSig, nTrd := eff(col, f.Collection), eff(sig, f.Signal), eff(trd, f.Trading)
		if nSig && !nCol {
			return invalid("enabled_for_signal requires enabled_for_collection")
		}
		if nTrd && !nSig {
			return invalid("enabled_for_trading requires enabled_for_signal")
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
func (s *Store) UpdateExchangeConfig(ctx context.Context, exchangeID int64, u ExchangeConfigUpdate, by, reason string) (int64, error) {
	if err := u.Validate(); err != nil {
		return 0, err
	}
	var version int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var maxConc, reqTo, maxRetry, backoff, rate sql.NullInt64
		err := tx.QueryRowContext(ctx,
			"SELECT max_concurrent_requests, request_timeout_ms, max_retries, retry_backoff_ms, rate_limit_per_sec FROM exchange_configs WHERE exchange_id=?", exchangeID).
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
func (s *Store) UpsertFee(ctx context.Context, exchangeID, exchangeMarketID int64, makerFee, takerFee string, by, reason string) (int64, error) {
	mk, err1 := decimal.NewFromString(makerFee)
	tk, err2 := decimal.NewFromString(takerFee)
	if err1 != nil || err2 != nil || mk.IsNegative() || tk.IsNegative() {
		return 0, invalid("maker_fee and taker_fee must be non-negative numbers")
	}
	var version int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var oldMaker, oldTaker sql.NullString
		var feeID sql.NullInt64
		emArg := nullableMarketID(exchangeMarketID)
		_ = tx.QueryRowContext(ctx,
			"SELECT id, maker_fee, taker_fee FROM exchange_fees WHERE exchange_id=? AND ((exchange_market_id IS NULL AND ? IS NULL) OR exchange_market_id=?) ORDER BY id DESC LIMIT 1",
			exchangeID, emArg, emArg).Scan(&feeID, &oldMaker, &oldTaker)
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
		for _, a := range []AuditEntry{
			{Field: "maker_fee", OldValue: oldMaker.String, NewValue: makerFee},
			{Field: "taker_fee", OldValue: oldTaker.String, NewValue: takerFee},
		} {
			a.ConfigVersion, a.EntityType, a.EntityID, a.ChangedBy, a.Reason = v, "exchange_fee", exchangeID, by, reason
			if err := RecordAudit(ctx, tx, a); err != nil {
				return err
			}
		}
		return nil
	})
	return version, err
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
