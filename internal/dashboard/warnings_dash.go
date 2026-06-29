package dashboard

import (
	"database/sql"
	"encoding/json"
	"net/http"
)

// Operator-facing live warnings + a per-session live-audit export (PR25). Both are
// read-only and contain NO secrets.

type warning struct {
	Code     string `json:"code"`
	Severity string `json:"severity"` // info | warn | danger
	Message  string `json:"message"`
}

// liveWarnings returns strong operator warnings for live-danger states so the dashboard can
// surface them prominently. Read-only.
func (s *Server) liveWarnings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var (
		kill                  int
		canEx, canMk          sql.NullInt64
		credMaxMin, mktMaxSec sql.NullInt64
		balMaxMin             sql.NullInt64
	)
	s.db.QueryRowContext(ctx, `
SELECT kill_switch, canary_exchange_id, canary_market_id,
  credential_validation_max_age_minutes, market_data_max_age_seconds, balance_max_age_minutes
FROM live_controls WHERE id=1`).Scan(&kill, &canEx, &canMk, &credMaxMin, &mktMaxSec, &balMaxMin)

	var out []warning
	add := func(code, sev, msg string) { out = append(out, warning{Code: code, Severity: sev, Message: msg}) }

	if s.cfg.ExecutionMode == "live" {
		add("live_mode_enabled", "danger", "LIVE execution mode is enabled — real orders can be sent")
	}
	if s.cfg.ExecutionMode == "live" && kill == 0 {
		add("kill_switch_disengaged", "danger", "kill switch is DISENGAGED — new live buys are not globally blocked")
	}

	// Session-scoped warnings (only meaningful with a canary scope).
	if canEx.Valid && canMk.Valid {
		var sessID sql.NullInt64
		var checklist sql.NullString
		s.db.QueryRowContext(ctx,
			"SELECT id, first_order_checklist_json FROM live_run_sessions WHERE exchange_id=? AND exchange_market_id=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1",
			canEx.Int64, canMk.Int64).Scan(&sessID, &checklist)
		if sessID.Valid {
			add("session_active", "warn", "a live canary session is ACTIVE")
			if checklist.Valid {
				add("first_order_sent", "danger", "the first real live order has been SENT for this session")
			} else {
				add("first_order_pending", "warn", "session active; the first real live order has NOT been sent yet")
			}
		}
		// Credential validation staleness for the canary exchange.
		if s.scalarInt(ctx, `
SELECT COUNT(*) FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active'
  AND (last_checked_at IS NULL OR TIMESTAMPDIFF(MINUTE, last_checked_at, NOW(6)) > COALESCE(?, 60))`,
			canEx.Int64, nullInt64Arg(credMaxMin)) > 0 {
			add("credential_validation_stale", "danger", "the canary credential's validation is stale or missing")
		}
		// Balance staleness.
		if s.scalarInt(ctx, `
SELECT CASE WHEN MAX(COALESCE(last_seen_at, updated_at)) IS NULL
  OR TIMESTAMPDIFF(MINUTE, MAX(COALESCE(last_seen_at, updated_at)), NOW(6)) > COALESCE(?, 10) THEN 1 ELSE 0 END
FROM wallet_balances_current WHERE exchange_id=?`, nullInt64Arg(balMaxMin), canEx.Int64) > 0 {
			add("balance_stale", "danger", "balance data for the canary exchange is stale or missing")
		}
		// Market-data staleness (recent comparison_event for the canary symbol).
		var symbol string
		s.db.QueryRowContext(ctx, "SELECT canonical_symbol FROM exchange_markets WHERE id=?", canMk.Int64).Scan(&symbol)
		if s.scalarInt(ctx, `
SELECT CASE WHEN MAX(created_at) IS NULL
  OR TIMESTAMPDIFF(SECOND, MAX(created_at), NOW(6)) > COALESCE(?, 30) THEN 1 ELSE 0 END
FROM comparison_events WHERE canonical_symbol=?`, nullInt64Arg(mktMaxSec), symbol) > 0 {
			add("market_data_stale", "danger", "market data for the canary symbol is stale or missing")
		}
	}

	if s.scalarInt(ctx, "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state='NEEDS_RECONCILE'") > 0 {
		add("unresolved_reconcile", "danger", "there are unresolved NEEDS_RECONCILE cycles — resolve them before/within live trading")
	}

	writeJSON(w, http.StatusOK, map[string]any{"warnings": out})
}

// liveSessionExport exports the full live-audit bundle for a session (the current ACTIVE
// session, or ?session_id=). It includes the session, acknowledgement, caps, requests,
// orders, allow/deny decisions, the first-order checklist, and the stop reason — NO secrets.
func (s *Server) liveSessionExport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sess, err := s.oneRow(ctx, sessionSelect(r.URL.Query().Get("session_id")), sessionArg(r.URL.Query().Get("session_id"))...)
	if err != nil {
		s.fail(w, err)
		return
	}
	if sess == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "no such session (and no active session)"})
		return
	}
	sid := asInt(sess["id"])
	exID := asInt(sess["exchange_id"])
	mkID := asInt(sess["exchange_market_id"])

	ack, _ := s.oneRow(ctx, "SELECT id, preflight_hash, acknowledged_at, active, reason FROM live_acknowledgements WHERE id=?", sess["acknowledgement_id"])
	// live_audit rows correlated to this session.
	decisions, _ := s.rows(ctx, "SELECT id, action, side, notional, decision, reason, created_at FROM live_audit WHERE live_session_id=? AND decision='allow' ORDER BY id", sid)
	denials, _ := s.rows(ctx, "SELECT id, action, side, notional, decision, reason, created_at FROM live_audit WHERE live_session_id=? AND decision='deny' ORDER BY id", sid)
	// Requests + orders for the exchange/market within the session window.
	reqs, _ := s.rows(ctx, `
SELECT er.id, er.request_type, er.status, er.created_at FROM exchange_requests er
WHERE er.exchange_id=? AND er.created_at >= (SELECT started_at FROM live_run_sessions WHERE id=?) ORDER BY er.id`, exID, sid)
	orders, _ := s.rows(ctx, `
SELECT o.id, o.side, o.role, o.state, o.quantity, o.limit_price, o.filled_quantity, o.created_at FROM orders o
WHERE o.exchange_market_id=? AND o.created_at >= (SELECT started_at FROM live_run_sessions WHERE id=?) ORDER BY o.id`, mkID, sid)

	writeJSON(w, http.StatusOK, map[string]any{
		"session":               sess,
		"preflight_hash":        sess["preflight_hash"],
		"acknowledgement":       ack,
		"caps":                  rawJSON(sess["caps_json"]),
		"first_order_checklist": rawJSON(sess["first_order_checklist_json"]),
		"stop_reason":           sess["stop_reason"],
		"requests":              reqs,
		"orders":                orders,
		"decisions":             decisions,
		"denials":               denials,
	})
}

func sessionSelect(idParam string) string {
	if idParam != "" {
		return "SELECT * FROM live_run_sessions WHERE id=?"
	}
	return "SELECT * FROM live_run_sessions WHERE status='ACTIVE' ORDER BY id DESC LIMIT 1"
}
func sessionArg(idParam string) []any {
	if idParam != "" {
		return []any{idParam}
	}
	return nil
}

func nullInt64Arg(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
}

// rawJSON returns a []byte JSON column as json.RawMessage so it serializes as an object,
// not a base64 string.
func rawJSON(v any) any {
	if b, ok := v.([]byte); ok && len(b) > 0 {
		return json.RawMessage(b)
	}
	return v
}
