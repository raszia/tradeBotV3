package dashboard

import (
	"context"
	"errors"
	"net/http"

	"v3TradeBot/internal/live"
)

// Live canary run-session handlers (PR24). View is read-only; start/stop require admin.
// Starting needs a ready preflight + a current acknowledgement; stopping blocks new buys
// immediately (the guard requires an active session) while sell/cancel/status keep working.

func (s *Server) liveSessionStart(w http.ResponseWriter, r *http.Request, op operator) {
	if s.preflight == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "preflight unavailable"})
		return
	}
	var body struct {
		ExchangeID int64  `json:"exchange_id"`
		MarketID   int64  `json:"market_id"`
		Reason     string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	// Starting requires a READY preflight (re-run now), then the session-start verification
	// (canary scope + current, non-expired, hash-matching acknowledgement + dynamic recheck).
	report, err := s.preflight.Run(r.Context(), body.ExchangeID, body.MarketID)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !report.Ready {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "preflight not ready", "report": report})
		return
	}
	id, err := live.StartSession(r.Context(), s.db, nil, report.ExchangeID, report.MarketID, op.name, body.Reason)
	if err != nil {
		s.respondSessionErr(w, err, "start session")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "session_id": id})
}

func (s *Server) liveSessionStop(w http.ResponseWriter, r *http.Request, op operator) {
	var body struct {
		ExchangeID int64  `json:"exchange_id"`
		MarketID   int64  `json:"market_id"`
		Reason     string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if err := live.StopSession(r.Context(), s.db, nil, body.ExchangeID, body.MarketID, op.name, body.Reason); err != nil {
		s.respondSessionErr(w, err, "stop session")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// liveSession shows the current active session + live-run visibility (read-only).
func (s *Server) liveSession(w http.ResponseWriter, r *http.Request) {
	sess, _ := s.oneRow(r.Context(), "SELECT * FROM live_run_sessions WHERE status='ACTIVE' ORDER BY id DESC LIMIT 1")
	out := map[string]any{
		"session":        sess,
		"kill_switch":    s.killSwitchOn(r.Context()),
		"execution_mode": s.cfg.ExecutionMode,
	}
	if sess != nil {
		exID := asInt(sess["exchange_id"])
		mkID := asInt(sess["exchange_market_id"])
		out["order_count"] = s.scalarInt(r.Context(), "SELECT COUNT(*) FROM orders o JOIN cycles c ON c.id=o.cycle_id WHERE c.dry_run=0 AND o.exchange_id=? AND o.created_at >= CURDATE()", exID)
		out["quote_used"] = s.scalarStr(r.Context(), "SELECT COALESCE(SUM(o.quantity*o.limit_price),0) FROM orders o JOIN cycles c ON c.id=o.cycle_id WHERE c.dry_run=0 AND o.exchange_id=? AND o.role='entry_buy' AND o.created_at >= CURDATE()", exID)
		out["open_cycles"] = s.scalarInt(r.Context(), "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND buy_exchange_id=? AND state NOT IN ('CLOSED','CANCELLED','FAILED')", exID)
		out["last_order"], _ = s.oneRow(r.Context(), "SELECT id, side, role, state, quantity, limit_price, created_at FROM orders WHERE exchange_id=? ORDER BY id DESC LIMIT 1", exID)
		out["last_live_deny"], _ = s.oneRow(r.Context(), "SELECT action, reason, created_at FROM live_audit WHERE exchange_id=? AND decision='deny' ORDER BY id DESC LIMIT 1", exID)
		// Acknowledgement status for the session's scope.
		out["acknowledgement"], _ = s.oneRow(r.Context(), `
SELECT id, preflight_hash, acknowledged_at,
  (active=1 AND TIMESTAMPDIFF(MINUTE, acknowledged_at, NOW(6)) > COALESCE((SELECT canary_ack_max_age_minutes FROM live_controls WHERE id=1), 30)) AS expired
FROM live_acknowledgements WHERE exchange_id=? AND exchange_market_id=? AND active=1 ORDER BY id DESC LIMIT 1`, exID, mkID)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) killSwitchOn(ctx context.Context) bool {
	var k int
	s.db.QueryRowContext(ctx, "SELECT kill_switch FROM live_controls WHERE id=1").Scan(&k)
	return k != 0
}

func (s *Server) scalarInt(ctx context.Context, q string, args ...any) int {
	var n int
	s.db.QueryRowContext(ctx, q, args...).Scan(&n)
	return n
}

func (s *Server) scalarStr(ctx context.Context, q string, args ...any) string {
	var v string
	s.db.QueryRowContext(ctx, q, args...).Scan(&v)
	return v
}

// respondSessionErr maps live session errors: input/validation -> 400, already-active /
// no-active -> 409, else 500.
func (s *Server) respondSessionErr(w http.ResponseWriter, err error, what string) {
	switch {
	case errors.Is(err, live.ErrSessionAlreadyActive), errors.Is(err, live.ErrNoActiveSession):
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
	case errors.Is(err, live.ErrOutOfCanaryScope), errors.Is(err, live.ErrNoValidAck),
		errors.Is(err, live.ErrAckStale), errors.Is(err, live.ErrAckExpired), errors.Is(err, live.ErrNotReady),
		errors.Is(err, live.ErrNoRecentDryRun),
		errors.Is(err, live.ErrSessionOperatorRequired), errors.Is(err, live.ErrSessionReasonRequired):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
	default:
		s.log.Warn(what+" failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": what + " failed"})
	}
}
