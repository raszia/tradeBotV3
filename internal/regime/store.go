package regime

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// ErrInvalidBasketConfig means a basket's stored config violates a validation rule
// (weights/seconds must be positive, thresholds non-negative and correctly ordered). It
// is returned by LoadBaskets so invalid config NEVER produces a regime result. The DB
// also enforces these via CHECK constraints (migration 027); this is defence in depth.
var ErrInvalidBasketConfig = errors.New("regime: invalid basket config")

// Store loads regime basket config and writes regime current/history.
type Store struct{ db *sql.DB }

// NewStore builds a Store over db.
func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// LoadBaskets returns all ENABLED baskets with their enabled symbols + timeframes.
func (s *Store) LoadBaskets(ctx context.Context) ([]Basket, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, name, update_interval_seconds, neutral_band_bps, moderate_threshold_bps, strong_threshold_bps, COALESCE(config_version,0) FROM market_regime_baskets WHERE enabled=1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	baskets := map[int64]*Basket{}
	var order []int64
	for rows.Next() {
		var b Basket
		var interval int
		if err := rows.Scan(&b.ID, &b.Name, &interval, &b.NeutralBandBps, &b.ModerateBps, &b.StrongBps, &b.ConfigVersion); err != nil {
			return nil, err
		}
		b.UpdateIntervalSeconds = interval
		bb := b
		baskets[b.ID] = &bb
		order = append(order, b.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(order) == 0 {
		return nil, nil
	}

	symRows, err := s.db.QueryContext(ctx,
		"SELECT basket_id, binance_symbol, weight FROM market_regime_basket_symbols WHERE enabled=1")
	if err != nil {
		return nil, err
	}
	defer symRows.Close()
	for symRows.Next() {
		var bid int64
		var name string
		var w decimal.Decimal
		if err := symRows.Scan(&bid, &name, &w); err != nil {
			return nil, err
		}
		if b := baskets[bid]; b != nil {
			b.Symbols = append(b.Symbols, SymbolWeight{Symbol: name, Weight: w})
		}
	}
	if err := symRows.Err(); err != nil {
		return nil, err
	}

	tfRows, err := s.db.QueryContext(ctx,
		"SELECT basket_id, label, seconds, weight FROM market_regime_timeframes")
	if err != nil {
		return nil, err
	}
	defer tfRows.Close()
	for tfRows.Next() {
		var bid int64
		var tf Timeframe
		if err := tfRows.Scan(&bid, &tf.Label, &tf.Seconds, &tf.Weight); err != nil {
			return nil, err
		}
		if b := baskets[bid]; b != nil {
			b.Timeframes = append(b.Timeframes, tf)
		}
	}
	if err := tfRows.Err(); err != nil {
		return nil, err
	}

	out := make([]Basket, 0, len(order))
	for _, id := range order {
		b := *baskets[id]
		// Reject invalid config so the calculator never computes a regime from it.
		if err := b.Validate(); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// Validate enforces the regime-config invariants in code (the DB enforces the same via
// migration 027 CHECK constraints). A basket with no enabled symbols/timeframes is NOT a
// config error — Calculate degrades it to UNKNOWN — so those are not rejected here; only
// genuinely invalid values are. Returns an error wrapping ErrInvalidBasketConfig.
func (b Basket) Validate() error {
	if b.UpdateIntervalSeconds < 0 {
		return fmt.Errorf("%w: basket %q update_interval_seconds must be >= 0 (got %d)", ErrInvalidBasketConfig, b.Name, b.UpdateIntervalSeconds)
	}
	if b.NeutralBandBps < 0 {
		return fmt.Errorf("%w: basket %q neutral_band_bps must be >= 0 (got %d)", ErrInvalidBasketConfig, b.Name, b.NeutralBandBps)
	}
	if b.ModerateBps < b.NeutralBandBps {
		return fmt.Errorf("%w: basket %q moderate_threshold_bps (%d) must be >= neutral_band_bps (%d)", ErrInvalidBasketConfig, b.Name, b.ModerateBps, b.NeutralBandBps)
	}
	if b.StrongBps < b.ModerateBps {
		return fmt.Errorf("%w: basket %q strong_threshold_bps (%d) must be >= moderate_threshold_bps (%d)", ErrInvalidBasketConfig, b.Name, b.StrongBps, b.ModerateBps)
	}
	for _, sw := range b.Symbols {
		if !sw.Weight.IsPositive() {
			return fmt.Errorf("%w: basket %q symbol %q weight must be > 0 (got %s)", ErrInvalidBasketConfig, b.Name, sw.Symbol, sw.Weight.String())
		}
	}
	for _, tf := range b.Timeframes {
		if tf.Seconds <= 0 {
			return fmt.Errorf("%w: basket %q timeframe %q seconds must be > 0 (got %d)", ErrInvalidBasketConfig, b.Name, tf.Label, tf.Seconds)
		}
		if !tf.Weight.IsPositive() {
			return fmt.Errorf("%w: basket %q timeframe %q weight must be > 0 (got %s)", ErrInvalidBasketConfig, b.Name, tf.Label, tf.Weight.String())
		}
	}
	return nil
}

// WriteResult upserts the basket's current regime and appends a history row ONLY when
// the regime CHANGED — where "changed" is a content hash over ALL important output
// fields (direction, level, confidence, score, per-timeframe scores, per-symbol
// contributions, stale_reason, config_version), NOT just the direction/level label.
// So BULLISH/STRONG @0.35 → @0.90 records a new history row, and an UNKNOWN whose
// stale_reason changes records one too — the dashboard sees the full evolution. A
// genuinely identical regime is idempotent (current upserted, no history). An UNKNOWN
// result is written to current with its stale_reason (never a fabricated regime).
func (s *Store) WriteResult(ctx context.Context, b Basket, r Result, computedAt time.Time) error {
	tfJSON, _ := json.Marshal(decimalMap(r.TimeframeScores))
	symJSON, _ := json.Marshal(decimalMap(r.SymbolContributions))
	hash := regimeStateHash(r, string(tfJSON), string(symJSON), b.ConfigVersion)

	// Detect change vs the current row's state_hash. Only sql.ErrNoRows is safe to treat
	// as "no previous state" (→ changed). Any OTHER error (DB/schema/connection/scan) must
	// be returned — silently swallowing it and inserting would write misleading history.
	var prevHash sql.NullString
	var changed bool
	switch err := s.db.QueryRowContext(ctx, "SELECT state_hash FROM market_regime_current WHERE basket_id=?", b.ID).Scan(&prevHash); {
	case errors.Is(err, sql.ErrNoRows):
		changed = true
	case err != nil:
		return err
	default:
		changed = !prevHash.Valid || prevHash.String != hash
	}

	if _, err := s.db.ExecContext(ctx, `
INSERT INTO market_regime_current
  (basket_id, direction, level, confidence, score_bps, timeframe_scores, symbol_contributions, stale_reason, state_hash, config_version, computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE direction=VALUES(direction), level=VALUES(level), confidence=VALUES(confidence),
  score_bps=VALUES(score_bps), timeframe_scores=VALUES(timeframe_scores), symbol_contributions=VALUES(symbol_contributions),
  stale_reason=VALUES(stale_reason), state_hash=VALUES(state_hash), config_version=VALUES(config_version), computed_at=VALUES(computed_at)`,
		b.ID, r.Direction, r.Level, r.Confidence.String(), decimalOrNull(r.ScoreBps), string(tfJSON), string(symJSON),
		nullStr(r.StaleReason), hash, b.ConfigVersion, computedAt.UTC()); err != nil {
		return err
	}

	if changed {
		// History carries the SAME full-field payload as current — including stale_reason
		// and the state_hash itself — so a history row shows WHAT changed, not just that
		// something did (e.g. an UNKNOWN whose stale_reason evolved is self-describing).
		if _, err := s.db.ExecContext(ctx, `
INSERT INTO market_regime_history
  (basket_id, direction, level, confidence, score_bps, timeframe_scores, symbol_contributions, stale_reason, state_hash, config_version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			b.ID, r.Direction, r.Level, r.Confidence.String(), decimalOrNull(r.ScoreBps), string(tfJSON), string(symJSON),
			nullStr(r.StaleReason), hash, b.ConfigVersion); err != nil {
			return err
		}
	}
	return nil
}

// regimeStateHash is the full-field content hash used for history change detection.
// json.Marshal of a Go map emits keys in sorted order, so the timeframe/symbol JSON is
// deterministic and equal regimes hash equally.
func regimeStateHash(r Result, tfJSON, symJSON string, configVersion int64) string {
	h := sha256.Sum256([]byte(strings.Join([]string{
		r.Direction, r.Level, r.Confidence.String(), r.ScoreBps.String(),
		tfJSON, symJSON, r.StaleReason, strconv.FormatInt(configVersion, 10),
	}, "|")))
	return hex.EncodeToString(h[:])
}

func decimalMap(m map[string]decimal.Decimal) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v.String()
	}
	return out
}

func decimalOrNull(d decimal.Decimal) any {
	if d.IsZero() {
		return nil
	}
	return d.String()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
