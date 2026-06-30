package configstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
)

// ErrNoActiveVersion means no config_versions row has status='active'. The
// loader tolerates this (Snapshot.Version = 0) so the system can run unconfigured.
var ErrNoActiveVersion = errors.New("configstore: no active config version")

// ErrMultipleActiveVersions means MORE THAN ONE config_versions row has
// status='active'. That is a corrupt invariant (activation always supersedes the
// prior active in one transaction), so the system must NOT silently pick one — it
// stops and surfaces the ambiguity for an operator to resolve.
var ErrMultipleActiveVersions = errors.New("configstore: multiple active config versions")

// Store is the DB access layer for trading config. It wraps *sql.DB.
type Store struct {
	db *sql.DB
}

// New builds a Store over db.
func New(db *sql.DB) *Store { return &Store{db: db} }

// ActiveVersion returns the single active config version id. It does NOT use
// "ORDER BY id DESC LIMIT 1": if the table somehow holds more than one active
// version, silently picking the latest could run trading on the wrong config, so
// instead it returns ErrMultipleActiveVersions. Zero active → ErrNoActiveVersion.
func (s *Store) ActiveVersion(ctx context.Context) (int64, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id FROM config_versions WHERE status = 'active' ORDER BY id")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	switch len(ids) {
	case 0:
		return 0, ErrNoActiveVersion
	case 1:
		return ids[0], nil
	default:
		return 0, fmt.Errorf("%w: ids=%v", ErrMultipleActiveVersions, ids)
	}
}

const marketsQuery = `
SELECT em.id, em.exchange_id, e.code, em.canonical_symbol,
       em.enabled_for_collection, em.enabled_for_signal, em.enabled_for_trading, em.enabled_for_sell_manage,
       sc.exchange_market_id, sc.min_spread_bps, sc.buy_size, sc.buy_size_unit, sc.sell_offset_bps,
       sc.reprice_interval_seconds, sc.order_timeout_ms, sc.max_retries, sc.retry_backoff_ms, sc.config_version,
       sc.maker_first_enabled, sc.maker_attempts_before_taker, sc.maker_signal_window_seconds,
       sc.maker_wait_before_cancel_ms, sc.maker_price_offset_bps, sc.taker_price_mode, sc.max_taker_slippage_bps,
       em.tick_size, em.step_size, em.min_order_amount, em.min_order_quantity
FROM exchange_markets em
JOIN exchanges e ON e.id = em.exchange_id
LEFT JOIN symbol_configs sc ON sc.exchange_market_id = em.id`

const exchangeConfigsQuery = `
SELECT ec.exchange_id, e.code, ec.max_concurrent_requests, ec.request_timeout_ms,
       ec.max_retries, ec.retry_backoff_ms, ec.rate_limit_per_sec, ec.config_version
FROM exchange_configs ec
JOIN exchanges e ON e.id = ec.exchange_id`

const retentionQuery = `
SELECT table_name, retention_days, max_rows, max_total_bytes, enabled, config_version
FROM retention_settings`

const feesQuery = `
SELECT exchange_id, exchange_market_id, maker_fee, taker_fee, config_version
FROM exchange_fees`

// LoadSnapshot reads the full trading config into an immutable Snapshot. A
// missing active version is not an error (Version=0); markets/exchanges still
// load so flags are visible.
func (s *Store) LoadSnapshot(ctx context.Context) (*Snapshot, error) {
	snap := emptySnapshot()

	v, err := s.ActiveVersion(ctx)
	if err != nil && !errors.Is(err, ErrNoActiveVersion) {
		return nil, err
	}
	snap.Version = v // 0 if none active

	if err := s.loadMarkets(ctx, snap); err != nil {
		return nil, err
	}
	if err := s.loadExchangeConfigs(ctx, snap); err != nil {
		return nil, err
	}
	if err := s.loadRetention(ctx, snap); err != nil {
		return nil, err
	}
	if err := s.loadFees(ctx, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

func (s *Store) loadMarkets(ctx context.Context, snap *Snapshot) error {
	rows, err := s.db.QueryContext(ctx, marketsQuery)
	if err != nil {
		return fmt.Errorf("configstore: load markets: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id, exchangeID          int64
			code, canonical         string
			fCol, fSig, fTrd, fSell int
			scID                    sql.NullInt64
			minSpread               sql.NullInt64
			buySize                 decimal.NullDecimal
			buySizeUnit             sql.NullString
			sellOffset              sql.NullInt64
			reprice                 sql.NullInt64
			timeout                 sql.NullInt64
			maxRetries              sql.NullInt64
			backoff                 sql.NullInt64
			scVersion               sql.NullInt64
			makerFirst              sql.NullInt64
			makerAttempts           sql.NullInt64
			makerWindow             sql.NullInt64
			makerWait               sql.NullInt64
			makerOffset             sql.NullInt64
			takerMode               sql.NullString
			takerSlippage           sql.NullInt64
			tickSize                decimal.NullDecimal
			stepSize                decimal.NullDecimal
			minAmount               decimal.NullDecimal
			minQty                  decimal.NullDecimal
		)
		if err := rows.Scan(&id, &exchangeID, &code, &canonical, &fCol, &fSig, &fTrd, &fSell,
			&scID, &minSpread, &buySize, &buySizeUnit, &sellOffset, &reprice, &timeout, &maxRetries, &backoff, &scVersion,
			&makerFirst, &makerAttempts, &makerWindow, &makerWait, &makerOffset, &takerMode, &takerSlippage,
			&tickSize, &stepSize, &minAmount, &minQty); err != nil {
			return err
		}
		m := MarketConfig{
			ExchangeMarketID:       id,
			ExchangeID:             exchangeID,
			ExchangeCode:           code,
			CanonicalSymbol:        canonical,
			EnabledForCollection:   fCol != 0,
			EnabledForSignal:       fSig != 0,
			EnabledForTrading:      fTrd != 0,
			EnabledForSellManage:   fSell != 0,
			HasSymbolConfig:        scID.Valid,
			MinSpreadBps:           int(minSpread.Int64),
			BuySize:                buySize.Decimal,
			BuySizeUnit:            buySizeUnit.String,
			SellOffsetBps:          int(sellOffset.Int64),
			RepriceIntervalSeconds: int(reprice.Int64),
			OrderTimeoutMs:         int(timeout.Int64),
			MaxRetries:             int(maxRetries.Int64),
			RetryBackoffMs:         int(backoff.Int64),
			SymbolConfigVersion:    scVersion.Int64,
			Maker: MakerPolicy{
				// NULL only for a market without a symbol_config (which cannot trade
				// anyway); fall back to safe defaults mirroring the DDL.
				MakerFirstEnabled:        !makerFirst.Valid || makerFirst.Int64 != 0,
				MakerAttemptsBeforeTaker: intOrDefault(makerAttempts, 1),
				MakerSignalWindowSeconds: intOrDefault(makerWindow, 60),
				MakerWaitBeforeCancelMs:  intOrDefault(makerWait, 2000),
				MakerPriceOffsetBps:      intOrDefault(makerOffset, 5),
				TakerPriceMode:           strOrDefault(takerMode, "ASK"),
				MaxTakerSlippageBps:      int(takerSlippage.Int64), // 0 (incl. NULL) = no cap
			},
			TickSize:         tickSize.Decimal,  // zero = no tick constraint
			StepSize:         stepSize.Decimal,  // zero = no step constraint
			MinOrderAmount:   minAmount.Decimal, // zero = no min notional
			MinOrderQuantity: minQty.Decimal,    // zero = no min quantity
		}
		snap.MarketsByID[id] = m
		snap.MarketsBySymbol[canonical] = append(snap.MarketsBySymbol[canonical], m)
	}
	return rows.Err()
}

func (s *Store) loadExchangeConfigs(ctx context.Context, snap *Snapshot) error {
	rows, err := s.db.QueryContext(ctx, exchangeConfigsQuery)
	if err != nil {
		return fmt.Errorf("configstore: load exchange configs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			exID                                       int64
			code                                       string
			maxConc                                    int
			reqTimeout, maxRetries, backoff, rateLimit sql.NullInt64
			version                                    sql.NullInt64
		)
		if err := rows.Scan(&exID, &code, &maxConc, &reqTimeout, &maxRetries, &backoff, &rateLimit, &version); err != nil {
			return err
		}
		snap.Exchanges[code] = ExchangeConfig{
			ExchangeID:            exID,
			ExchangeCode:          code,
			MaxConcurrentRequests: maxConc,
			RequestTimeoutMs:      int(reqTimeout.Int64),
			MaxRetries:            int(maxRetries.Int64),
			RetryBackoffMs:        int(backoff.Int64),
			RateLimitPerSec:       int(rateLimit.Int64),
			ConfigVersion:         version.Int64,
		}
	}
	return rows.Err()
}

func (s *Store) loadRetention(ctx context.Context, snap *Snapshot) error {
	rows, err := s.db.QueryContext(ctx, retentionQuery)
	if err != nil {
		return fmt.Errorf("configstore: load retention: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			table            string
			days, enabled    int
			maxRows, maxByte sql.NullInt64
			version          sql.NullInt64
		)
		if err := rows.Scan(&table, &days, &maxRows, &maxByte, &enabled, &version); err != nil {
			return err
		}
		snap.Retention[table] = RetentionSetting{
			TableName:     table,
			RetentionDays: days,
			MaxRows:       maxRows.Int64,
			MaxTotalBytes: maxByte.Int64,
			Enabled:       enabled != 0,
			ConfigVersion: version.Int64,
		}
	}
	return rows.Err()
}

func (s *Store) loadFees(ctx context.Context, snap *Snapshot) error {
	rows, err := s.db.QueryContext(ctx, feesQuery)
	if err != nil {
		return fmt.Errorf("configstore: load fees: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			exID    int64
			emID    sql.NullInt64
			maker   decimal.Decimal
			taker   decimal.Decimal
			version sql.NullInt64
		)
		if err := rows.Scan(&exID, &emID, &maker, &taker, &version); err != nil {
			return err
		}
		fc := FeeConfig{
			ExchangeID:    exID,
			MakerFee:      maker,
			TakerFee:      taker,
			ConfigVersion: version.Int64,
		}
		if emID.Valid {
			// Market-specific override (exchange_market_id set).
			fc.ExchangeMarketID = emID.Int64
			snap.FeesByMarketID[emID.Int64] = fc
		} else {
			// Exchange-wide DEFAULT (exchange_market_id IS NULL). Scoped by exchange_id
			// so two exchanges' defaults never collide / overwrite each other.
			snap.DefaultFeesByExchangeID[exID] = fc
		}
	}
	return rows.Err()
}

// AuditEntry is one config_change_audit row. It must NEVER carry plaintext
// secrets (credential changes use exchange_credential_audit, not this table).
type AuditEntry struct {
	ConfigVersion int64
	EntityType    string
	EntityID      int64
	Field         string
	OldValue      string
	NewValue      string
	ChangedBy     string
	Reason        string
}

// RecordAudit inserts a config_change_audit row within tx.
func RecordAudit(ctx context.Context, tx *sql.Tx, e AuditEntry) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO config_change_audit
		  (config_version, entity_type, entity_id, field, old_value, new_value, changed_by, reason, activated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(6))`,
		nullableInt(e.ConfigVersion), e.EntityType, nullableInt(e.EntityID), nullStr(e.Field),
		nullStr(e.OldValue), nullStr(e.NewValue), nullStr(e.ChangedBy), nullStr(e.Reason))
	return err
}

// ActivateVersionTx supersedes the current active version and inserts a new active
// one within the caller's tx, returning its id. Exported so other config domains
// (e.g. regime baskets) can mint a version atomically with their own update + audit.
func ActivateVersionTx(ctx context.Context, tx *sql.Tx, createdBy, note string) (int64, error) {
	return activateVersionTx(ctx, tx, createdBy, note)
}

// activateVersionTx supersedes the current active version and inserts a new
// active one, returning its id. Runs inside tx.
func activateVersionTx(ctx context.Context, tx *sql.Tx, createdBy, note string) (int64, error) {
	if _, err := tx.ExecContext(ctx,
		"UPDATE config_versions SET status='superseded' WHERE status='active'"); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx,
		"INSERT INTO config_versions (status, created_by, activated_at, note) VALUES ('active', ?, NOW(6), ?)",
		nullStr(createdBy), nullStr(note))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// ActivateVersion creates a new active config version (superseding the prior
// one) in its own transaction. Returns the new version id.
func (s *Store) ActivateVersion(ctx context.Context, createdBy, note string) (int64, error) {
	var newID int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		id, err := activateVersionTx(ctx, tx, createdBy, note)
		newID = id
		return err
	})
	return newID, err
}

// UpdateMinSpreadBps is a representative VERSIONED + AUDITED config write: in one
// transaction it activates a new config version, updates the symbol_config, and
// records an audit row with old/new values. The dashboard edit forms (PR17) build
// on this pattern.
func (s *Store) UpdateMinSpreadBps(ctx context.Context, exchangeMarketID int64, newVal int, changedBy, reason string) (newVersion int64, err error) {
	// Validate BEFORE opening the transaction so an invalid value (e.g. a negative
	// spread) activates no version, mutates no symbol_config, and writes no audit row.
	if newVal < 0 {
		return 0, invalid("min_spread_bps must be >= 0")
	}
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		var oldVal sql.NullInt64
		if err := tx.QueryRowContext(ctx,
			"SELECT min_spread_bps FROM symbol_configs WHERE exchange_market_id = ?", exchangeMarketID).Scan(&oldVal); err != nil {
			return fmt.Errorf("configstore: read old min_spread_bps: %w", err)
		}
		version, err := activateVersionTx(ctx, tx, changedBy, reason)
		if err != nil {
			return err
		}
		newVersion = version
		if _, err := tx.ExecContext(ctx,
			"UPDATE symbol_configs SET min_spread_bps = ?, config_version = ? WHERE exchange_market_id = ?",
			newVal, version, exchangeMarketID); err != nil {
			return err
		}
		return RecordAudit(ctx, tx, AuditEntry{
			ConfigVersion: version, EntityType: "symbol_config", EntityID: exchangeMarketID,
			Field: "min_spread_bps", OldValue: nullIntStr(oldVal), NewValue: fmt.Sprint(newVal),
			ChangedBy: changedBy, Reason: reason,
		})
	})
	return newVersion, err
}

// withTx runs fn in a transaction, rolling back on error or panic.
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
		return err
	}
	return tx.Commit()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableInt(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullIntStr(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	return fmt.Sprint(v.Int64)
}

// intOrDefault returns v as an int, or def when the column was NULL.
func intOrDefault(v sql.NullInt64, def int) int {
	if !v.Valid {
		return def
	}
	return int(v.Int64)
}

// strOrDefault returns v, or def when the column was NULL/empty.
func strOrDefault(v sql.NullString, def string) string {
	if !v.Valid || v.String == "" {
		return def
	}
	return v.String
}
