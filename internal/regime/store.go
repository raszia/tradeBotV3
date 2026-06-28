package regime

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/shopspring/decimal"
)

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
		out = append(out, *baskets[id])
	}
	return out, nil
}

// WriteResult upserts the basket's current regime and appends a history row ONLY when
// the regime CHANGED (direction or level differs from the stored current) — so a
// steady regime does not spam history (repeated identical calculations are idempotent
// for the current row and add no history). Config version is stamped on both.
func (s *Store) WriteResult(ctx context.Context, b Basket, r Result, computedAt time.Time) error {
	tfJSON, _ := json.Marshal(decimalMap(r.TimeframeScores))
	symJSON, _ := json.Marshal(decimalMap(r.SymbolContributions))

	var prevDir, prevLvl sql.NullString
	_ = s.db.QueryRowContext(ctx, "SELECT direction, level FROM market_regime_current WHERE basket_id=?", b.ID).Scan(&prevDir, &prevLvl)
	changed := !prevDir.Valid || prevDir.String != r.Direction || prevLvl.String != r.Level

	if _, err := s.db.ExecContext(ctx, `
INSERT INTO market_regime_current
  (basket_id, direction, level, confidence, score_bps, timeframe_scores, symbol_contributions, stale_reason, config_version, computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE direction=VALUES(direction), level=VALUES(level), confidence=VALUES(confidence),
  score_bps=VALUES(score_bps), timeframe_scores=VALUES(timeframe_scores), symbol_contributions=VALUES(symbol_contributions),
  stale_reason=VALUES(stale_reason), config_version=VALUES(config_version), computed_at=VALUES(computed_at)`,
		b.ID, r.Direction, r.Level, r.Confidence.String(), decimalOrNull(r.ScoreBps), string(tfJSON), string(symJSON),
		nullStr(r.StaleReason), b.ConfigVersion, computedAt.UTC()); err != nil {
		return err
	}

	if changed {
		if _, err := s.db.ExecContext(ctx, `
INSERT INTO market_regime_history
  (basket_id, direction, level, confidence, score_bps, timeframe_scores, symbol_contributions, config_version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			b.ID, r.Direction, r.Level, r.Confidence.String(), decimalOrNull(r.ScoreBps), string(tfJSON), string(symJSON), b.ConfigVersion); err != nil {
			return err
		}
	}
	return nil
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
