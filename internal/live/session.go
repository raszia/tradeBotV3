package live

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/preflight"
)

// A live_run_session is an explicit, operator-started canary run. Starting one requires a
// valid (ready) preflight + a current acknowledgement and DOES NOT broaden scope — it is
// still the single configured canary exchange/symbol. The guard requires an ACTIVE session
// for every live buy, so stopping the session blocks new buys immediately while
// risk-reducing sell/cancel/status paths keep working.

var (
	ErrSessionOperatorRequired = errors.New("live: session requires an operator")
	ErrSessionReasonRequired   = errors.New("live: session requires a reason")
	ErrOutOfCanaryScope        = errors.New("live: exchange/symbol is not the configured canary scope")
	ErrNoValidAck              = errors.New("live: no valid acknowledgement for the scope (run preflight + acknowledge)")
	ErrAckStale                = errors.New("live: acknowledgement is stale (config changed since preflight)")
	ErrAckExpired              = errors.New("live: acknowledgement has expired (re-run preflight + re-acknowledge)")
	ErrNotReady                = errors.New("live: dynamic safety re-check failed (not ready)")
	ErrSessionAlreadyActive    = errors.New("live: a canary session is already active for this scope")
	ErrNoActiveSession         = errors.New("live: no active canary session for this scope")
)

// sessionQuerier is satisfied by *sql.DB and *sql.Tx.
type sessionQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Session is an active/stopped live run session.
type Session struct {
	ID            int64
	Operator      string
	ExchangeID    int64
	MarketID      int64
	Symbol        string
	Status        string
	PreflightHash string
	AckID         sql.NullInt64
	StartedAt     time.Time
}

// ActiveSession returns the ACTIVE session for the scope, if any.
func ActiveSession(ctx context.Context, q sessionQuerier, exchangeID, marketID int64) (Session, bool) {
	var s Session
	err := q.QueryRowContext(ctx,
		"SELECT id, operator, canonical_symbol, preflight_hash, acknowledgement_id, started_at FROM live_run_sessions WHERE exchange_id=? AND exchange_market_id=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1",
		exchangeID, marketID).Scan(&s.ID, &s.Operator, &s.Symbol, &s.PreflightHash, &s.AckID, &s.StartedAt)
	if err != nil {
		return Session{}, false
	}
	s.ExchangeID, s.MarketID, s.Status = exchangeID, marketID, "ACTIVE"
	return s, true
}

// StartSession verifies the scope is the configured canary, that a current (hash-matching,
// non-expired) acknowledgement exists, and that the dynamic safety conditions still pass,
// then records an ACTIVE session (rejecting if one is already active). It contacts no
// exchange. Returns the new session id.
func StartSession(ctx context.Context, db *sql.DB, clk clock.Clock, exchangeID, marketID int64, operator, reason string) (int64, error) {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if operator == "" {
		return 0, ErrSessionOperatorRequired
	}
	if reason == "" {
		return 0, ErrSessionReasonRequired
	}
	// Canary scope: must be exactly the configured single exchange/symbol.
	var canEx, canMk, ackMaxMin sql.NullInt64
	_ = db.QueryRowContext(ctx,
		"SELECT canary_exchange_id, canary_market_id, canary_ack_max_age_minutes FROM live_controls WHERE id=1").
		Scan(&canEx, &canMk, &ackMaxMin)
	if !canEx.Valid || !canMk.Valid || canEx.Int64 != exchangeID || canMk.Int64 != marketID {
		return 0, ErrOutOfCanaryScope
	}
	// A current acknowledgement must exist, match the config hash, and not be expired.
	ack, ok := preflight.ActiveAck(ctx, db, exchangeID, marketID)
	if !ok {
		return 0, ErrNoValidAck
	}
	curHash, err := preflight.ConfigHash(ctx, db, exchangeID, marketID, "live")
	if err != nil {
		return 0, err
	}
	if ack.Hash != curHash {
		return 0, ErrAckStale
	}
	maxAge := time.Duration(intOr(ackMaxMin, 30)) * time.Minute
	if clk.Now().UTC().Sub(ack.AcknowledgedAt) > maxAge {
		return 0, ErrAckExpired
	}
	// Dynamic safety conditions must still hold at start.
	if dok, _ := preflight.DynamicRecheck(ctx, db, clk, exchangeID, marketID); !dok {
		return 0, ErrNotReady
	}
	// At most one active session for the scope.
	if _, exists := ActiveSession(ctx, db, exchangeID, marketID); exists {
		return 0, ErrSessionAlreadyActive
	}

	var credID sql.NullInt64
	_ = db.QueryRowContext(ctx,
		"SELECT id FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active' ORDER BY key_version DESC, id DESC LIMIT 1", exchangeID).Scan(&credID)
	var symbol string
	_ = db.QueryRowContext(ctx, "SELECT canonical_symbol FROM exchange_markets WHERE id=?", marketID).Scan(&symbol)
	caps := capsSnapshot(ctx, db)

	res, err := db.ExecContext(ctx, `
INSERT INTO live_run_sessions (operator, exchange_id, exchange_market_id, canonical_symbol, credential_id, preflight_hash, acknowledgement_id, caps_json, status, start_reason)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)`,
		operator, exchangeID, marketID, symbol, nullableInt(credID), ack.Hash, ack.ID, caps, reason)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

// StopSession stops the ACTIVE session for the scope (status=STOPPED + stop time + reason).
// New buys are blocked immediately; risk-reducing paths are unaffected. The session row is
// the audit of who stopped it, when, and why.
func StopSession(ctx context.Context, db *sql.DB, clk clock.Clock, exchangeID, marketID int64, operator, reason string) error {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if reason == "" {
		return ErrSessionReasonRequired
	}
	res, err := db.ExecContext(ctx,
		"UPDATE live_run_sessions SET status='STOPPED', stopped_at=?, stop_reason=? WHERE exchange_id=? AND exchange_market_id=? AND status='ACTIVE'",
		clk.Now().UTC(), reason, exchangeID, marketID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNoActiveSession
	}
	return nil
}

// RecordFirstOrderChecklist writes the final pre-send checklist for the FIRST real buy of
// the active session (idempotent: only the first buy writes it). It is a safety snapshot
// with NO secrets — credential STATUS only, never key material.
func (g *Guard) RecordFirstOrderChecklist(ctx context.Context, p PlaceCheck) {
	sess, ok := ActiveSession(ctx, g.db, p.ExchangeID, p.ExchangeMarketID)
	if !ok {
		return
	}
	var existing sql.NullString
	g.db.QueryRowContext(ctx, "SELECT first_order_checklist_json FROM live_run_sessions WHERE id=?", sess.ID).Scan(&existing)
	if existing.Valid {
		return // already recorded for this session
	}
	ctrl, _ := g.LoadControls(ctx)
	var credStatus string
	g.db.QueryRowContext(ctx,
		"SELECT status FROM exchange_credentials WHERE exchange_id=? AND enabled=1 ORDER BY key_version DESC, id DESC LIMIT 1", p.ExchangeID).Scan(&credStatus)

	checklist := map[string]any{
		"mode":          "live",
		"exchange_id":   p.ExchangeID,
		"exchange_code": p.ExchangeCode,
		"symbol":        p.Symbol,
		"caps_remaining": map[string]any{
			"daily_orders": ctrl.MaxDailyOrders - g.dailyOrders(ctx),
			"daily_quote":  ctrl.MaxDailyQuote.Sub(g.dailyQuote(ctx)).String(),
			"open_cycles":  ctrl.MaxOpenCycles - g.openCycles(ctx),
		},
		"credential_status":      credStatus,
		"acknowledgement_id":     sess.AckID.Int64,
		"acknowledgement_active": true,
		"session_id":             sess.ID,
		"session_status":         "ACTIVE",
		"kill_switch":            ctrl.KillSwitch,
		"request_id":             p.RequestID,
		"order_id":               p.OrderID,
		"cycle_id":               p.CycleID,
	}
	b, _ := json.Marshal(checklist)
	if _, err := g.db.ExecContext(ctx,
		"UPDATE live_run_sessions SET first_order_checklist_json=? WHERE id=? AND first_order_checklist_json IS NULL", b, sess.ID); err != nil {
		g.log.Warn("live: first-order checklist write failed", "err", err)
	}
}

// capsSnapshot returns a JSON snapshot of the current caps (no secrets).
func capsSnapshot(ctx context.Context, db *sql.DB) []byte {
	var maxOpen, maxDaily sql.NullInt64
	var maxQuote, maxNotional, maxBase sql.NullString
	_ = db.QueryRowContext(ctx,
		"SELECT max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty FROM live_controls WHERE id=1").
		Scan(&maxOpen, &maxDaily, &maxQuote, &maxNotional, &maxBase)
	b, _ := json.Marshal(map[string]any{
		"max_open_cycles":    maxOpen.Int64,
		"max_daily_orders":   maxDaily.Int64,
		"max_daily_quote":    maxQuote.String,
		"max_order_notional": maxNotional.String,
		"max_base_qty":       maxBase.String,
	})
	return b
}

func intOr(v sql.NullInt64, def int64) int64 {
	if v.Valid && v.Int64 > 0 {
		return v.Int64
	}
	return def
}

func nullableInt(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
}
