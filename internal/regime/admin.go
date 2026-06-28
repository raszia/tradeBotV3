package regime

import (
	"context"
	"database/sql"
	"errors"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
)

// Regime config editing (PR17): versioned + audited + validated edits to baskets,
// their symbols, and their timeframes. Each mutation mints a new config_version and
// writes config_change_audit rows (reusing configstore's helpers). Nothing trades.

// BasketUpdate carries editable basket fields (nil = unchanged).
type BasketUpdate struct {
	Name                  *string `json:"name"`
	Enabled               *bool   `json:"enabled"`
	UpdateIntervalSeconds *int    `json:"update_interval_seconds"`
	NeutralBandBps        *int    `json:"neutral_band_bps"`
	ModerateThresholdBps  *int    `json:"moderate_threshold_bps"`
	StrongThresholdBps    *int    `json:"strong_threshold_bps"`
}

// UpdateBasket validates + applies a basket edit. Thresholds must be ordered
// (0 <= neutral <= moderate <= strong) and the update interval positive.
func (s *Store) UpdateBasket(ctx context.Context, basketID int64, u BasketUpdate, by, reason string) (int64, error) {
	var version int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var name sql.NullString
		var enabled sql.NullBool
		var interval, neutral, moderate, strong sql.NullInt64
		err := tx.QueryRowContext(ctx,
			"SELECT name, enabled, update_interval_seconds, neutral_band_bps, moderate_threshold_bps, strong_threshold_bps FROM market_regime_baskets WHERE id=?", basketID).
			Scan(&name, &enabled, &interval, &neutral, &moderate, &strong)
		if errors.Is(err, sql.ErrNoRows) {
			return configstore.ValidationFail("no regime basket %d", basketID)
		}
		if err != nil {
			return err
		}
		// Effective post-change thresholds for ordering validation.
		eff := func(cur sql.NullInt64, want *int) int {
			if want != nil {
				return *want
			}
			return int(cur.Int64)
		}
		n, m, st := eff(neutral, u.NeutralBandBps), eff(moderate, u.ModerateThresholdBps), eff(strong, u.StrongThresholdBps)
		if n < 0 || m < n || st < m {
			return configstore.ValidationFail("regime thresholds must satisfy 0 <= neutral <= moderate <= strong")
		}
		if u.UpdateIntervalSeconds != nil && *u.UpdateIntervalSeconds <= 0 {
			return configstore.ValidationFail("update_interval_seconds must be > 0")
		}

		cols, args, audits := buildSets(map[string]changed{
			"name":                    {strNew(name, u.Name)},
			"enabled":                 {boolNew(enabled, u.Enabled)},
			"update_interval_seconds": {intNew(interval, u.UpdateIntervalSeconds)},
			"neutral_band_bps":        {intNew(neutral, u.NeutralBandBps)},
			"moderate_threshold_bps":  {intNew(moderate, u.ModerateThresholdBps)},
			"strong_threshold_bps":    {intNew(strong, u.StrongThresholdBps)},
		})
		if len(cols) == 0 {
			return configstore.ErrNoChanges
		}
		v, err := configstore.ActivateVersionTx(ctx, tx, by, reason)
		if err != nil {
			return err
		}
		version = v
		args = append(args, v, basketID)
		if _, err := tx.ExecContext(ctx, "UPDATE market_regime_baskets SET "+joinComma(cols)+", config_version=? WHERE id=?", args...); err != nil {
			return err
		}
		return recordAudits(ctx, tx, v, "regime_basket", basketID, audits, by, reason)
	})
	return version, err
}

// UpsertBasketSymbol adds or updates a symbol's weight/enabled in a basket (weight>0),
// versioned + audited.
func (s *Store) UpsertBasketSymbol(ctx context.Context, basketID int64, symbol, weight string, enabled bool, by, reason string) (int64, error) {
	w, err := decimal.NewFromString(weight)
	if err != nil || !w.IsPositive() {
		return 0, configstore.ValidationFail("symbol weight must be a positive number")
	}
	if symbol == "" {
		return 0, configstore.ValidationFail("symbol is required")
	}
	var version int64
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		v, err := configstore.ActivateVersionTx(ctx, tx, by, reason)
		if err != nil {
			return err
		}
		version = v
		if _, err := tx.ExecContext(ctx, `
INSERT INTO market_regime_basket_symbols (basket_id, binance_symbol, weight, enabled) VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE weight=VALUES(weight), enabled=VALUES(enabled)`,
			basketID, symbol, weight, boolInt(enabled)); err != nil {
			return err
		}
		return recordAudits(ctx, tx, v, "regime_basket_symbol", basketID,
			[]configstore.AuditEntry{{Field: "symbol:" + symbol, NewValue: "weight=" + weight + " enabled=" + boolStr(enabled)}}, by, reason)
	})
	return version, err
}

// UpsertTimeframe adds or updates a timeframe (seconds>0, weight>0), versioned+audited.
func (s *Store) UpsertTimeframe(ctx context.Context, basketID int64, label string, seconds int, weight string, by, reason string) (int64, error) {
	w, err := decimal.NewFromString(weight)
	if err != nil || !w.IsPositive() {
		return 0, configstore.ValidationFail("timeframe weight must be a positive number")
	}
	if label == "" || seconds <= 0 {
		return 0, configstore.ValidationFail("timeframe label is required and seconds must be > 0")
	}
	var version int64
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		v, err := configstore.ActivateVersionTx(ctx, tx, by, reason)
		if err != nil {
			return err
		}
		version = v
		if _, err := tx.ExecContext(ctx, `
INSERT INTO market_regime_timeframes (basket_id, label, seconds, weight) VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE seconds=VALUES(seconds), weight=VALUES(weight)`,
			basketID, label, seconds, weight); err != nil {
			return err
		}
		return recordAudits(ctx, tx, v, "regime_timeframe", basketID,
			[]configstore.AuditEntry{{Field: "timeframe:" + label, NewValue: "seconds=" + itoa(seconds) + " weight=" + weight}}, by, reason)
	})
	return version, err
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) (retErr error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		retErr = err
		return retErr
	}
	retErr = tx.Commit()
	return retErr
}

// ---- field-diff helpers ----

type changed struct {
	val *changedVal
}
type changedVal struct {
	arg      any
	old, new string
}

func intNew(old sql.NullInt64, n *int) *changedVal {
	if n == nil || (old.Valid && int(old.Int64) == *n) {
		return nil
	}
	return &changedVal{arg: *n, old: nullIntToStr(old), new: itoa(*n)}
}
func boolNew(old sql.NullBool, n *bool) *changedVal {
	if n == nil || (old.Valid && old.Bool == *n) {
		return nil
	}
	return &changedVal{arg: boolInt(*n), old: boolStr(old.Bool), new: boolStr(*n)}
}
func strNew(old sql.NullString, n *string) *changedVal {
	if n == nil || (old.Valid && old.String == *n) {
		return nil
	}
	return &changedVal{arg: *n, old: old.String, new: *n}
}

func buildSets(m map[string]changed) (cols []string, args []any, audits []configstore.AuditEntry) {
	for col, c := range m {
		if c.val == nil {
			continue
		}
		cols = append(cols, col+"=?")
		args = append(args, c.val.arg)
		audits = append(audits, configstore.AuditEntry{Field: col, OldValue: c.val.old, NewValue: c.val.new})
	}
	return
}

func recordAudits(ctx context.Context, tx *sql.Tx, version int64, entityType string, id int64, audits []configstore.AuditEntry, by, reason string) error {
	for _, a := range audits {
		a.ConfigVersion, a.EntityType, a.EntityID, a.ChangedBy, a.Reason = version, entityType, id, by, reason
		if err := configstore.RecordAudit(ctx, tx, a); err != nil {
			return err
		}
	}
	return nil
}

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
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
func nullIntToStr(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	return itoa(int(v.Int64))
}
