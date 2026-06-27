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

// Store is the DB access layer for trading config. It wraps *sql.DB.
type Store struct {
	db *sql.DB
}

// New builds a Store over db.
func New(db *sql.DB) *Store { return &Store{db: db} }

// ActiveVersion returns the active config version id, or ErrNoActiveVersion.
func (s *Store) ActiveVersion(ctx context.Context) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx,
		"SELECT id FROM config_versions WHERE status = 'active' ORDER BY id DESC LIMIT 1").Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNoActiveVersion
	}
	if err != nil {
		return 0, err
	}
	return id, nil
}

const marketsQuery = `
SELECT em.id, em.exchange_id, e.code, em.canonical_symbol,
       em.enabled_for_collection, em.enabled_for_signal, em.enabled_for_trading, em.enabled_for_sell_manage,
       sc.exchange_market_id, sc.min_spread_bps, sc.buy_size, sc.buy_size_unit, sc.sell_offset_bps,
       sc.reprice_interval_seconds, sc.order_timeout_ms, sc.max_retries, sc.retry_backoff_ms, sc.config_version
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
		)
		if err := rows.Scan(&id, &exchangeID, &code, &canonical, &fCol, &fSig, &fTrd, &fSell,
			&scID, &minSpread, &buySize, &buySizeUnit, &sellOffset, &reprice, &timeout, &maxRetries, &backoff, &scVersion); err != nil {
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
		snap.Fees[emID.Int64] = FeeConfig{ // emID.Int64 == 0 => exchange default
			ExchangeID:       exID,
			ExchangeMarketID: emID.Int64,
			MakerFee:         maker,
			TakerFee:         taker,
			ConfigVersion:    version.Int64,
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
