package dashboard

import (
	"net/http"

	"v3TradeBot/internal/opreconcile"
)

// Operator reconciliation endpoints (PR21). List/detail/audit are read-only; preview/
// apply are gated by requireReconcileOperator. The resolution NEVER contacts an exchange
// (the Resolver holds only a DB handle) and there is no blind/one-click close: the
// operator must preview the exact proposed changes, then apply with an explicit reason.

// reconcileList returns the NEEDS_RECONCILE cycles awaiting operator resolution.
func (s *Server) reconcileList(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT * FROM cycles WHERE state='NEEDS_RECONCILE' ORDER BY id DESC LIMIT ?", s.limit(r))
}

// reconcileAudit returns the immutable resolution audit history (no secrets).
func (s *Server) reconcileAudit(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, `
SELECT id, cycle_id, order_id, operator, action, old_cycle_state, new_cycle_state,
  old_order_state, new_order_state, reason, lock_released, created_at
FROM reconcile_resolutions ORDER BY id DESC LIMIT ?`, s.limit(r))
}

// reconcileDetail composes the FULL context an operator must see before resolving: the
// cycle, exchange + symbol, every order (with local + exchange order id + last state),
// fills, queue requests, state events, locks, recent logs, the NEEDS_RECONCILE reason, a
// balance snapshot for the base asset, prior resolutions, and the available actions.
func (s *Server) reconcileDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cyc, err := s.oneRow(r.Context(), "SELECT * FROM cycles WHERE id=?", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if cyc == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "cycle not found"})
		return
	}
	exch, _ := s.oneRow(r.Context(),
		"SELECT e.code AS exchange_code, c.canonical_symbol FROM cycles c JOIN exchanges e ON e.id=c.buy_exchange_id WHERE c.id=?", id)
	orders, _ := s.rows(r.Context(), "SELECT * FROM orders WHERE cycle_id=? ORDER BY id", id)
	fills, _ := s.rows(r.Context(), "SELECT * FROM fills WHERE cycle_id=? ORDER BY id", id)
	reqs, _ := s.rows(r.Context(), "SELECT * FROM exchange_requests WHERE cycle_id=? ORDER BY id", id)
	for _, m := range reqs {
		m["step_kind"] = stepKind(asString(m["status"]), asInt(m["retry_count"]))
	}
	cycEvents, _ := s.rows(r.Context(), "SELECT * FROM cycle_state_events WHERE cycle_id=? ORDER BY id", id)
	ordEvents, _ := s.rows(r.Context(), "SELECT * FROM order_events WHERE order_id IN (SELECT id FROM orders WHERE cycle_id=?) ORDER BY id", id)
	locks, _ := s.rows(r.Context(), "SELECT * FROM symbol_locks WHERE cycle_id=? ORDER BY id", id)
	logs, _ := s.rows(r.Context(), "SELECT * FROM app_logs WHERE cycle_id=? ORDER BY id DESC LIMIT 200", id)
	// The reason is the last state event that entered NEEDS_RECONCILE.
	reason, _ := s.oneRow(r.Context(),
		"SELECT event_type, from_state, message, created_at FROM cycle_state_events WHERE cycle_id=? AND to_state='NEEDS_RECONCILE' ORDER BY id DESC LIMIT 1", id)
	// Balance snapshot for the cycle's base asset on the buy exchange (if balance-sync has it).
	balances, _ := s.rows(r.Context(), `
SELECT wb.* FROM wallet_balances_current wb JOIN cycles c ON c.buy_exchange_id=wb.exchange_id
WHERE c.id=? AND wb.asset = SUBSTRING_INDEX(c.canonical_symbol,'/',1)`, id)
	resolutions, _ := s.rows(r.Context(),
		"SELECT id, operator, action, old_cycle_state, new_cycle_state, old_order_state, new_order_state, reason, lock_released, created_at FROM reconcile_resolutions WHERE cycle_id=? ORDER BY id DESC", id)

	writeJSON(w, http.StatusOK, map[string]any{
		"cycle":             cyc,
		"exchange":          exch,
		"orders":            orders,
		"fills":             fills,
		"requests":          reqs,
		"cycle_events":      cycEvents,
		"order_events":      ordEvents,
		"locks":             locks,
		"logs":              logs,
		"reconcile_reason":  reason,
		"balances":          balances,
		"resolutions":       resolutions,
		"available_actions": reconcileActions(),
	})
}

// reconcilePreview returns the exact proposed state changes WITHOUT mutating anything.
func (s *Server) reconcilePreview(w http.ResponseWriter, r *http.Request, op operator) {
	req, ok := s.decodeReconcileReq(w, r, op)
	if !ok {
		return
	}
	plan, err := s.resolver.Preview(r.Context(), req)
	if err != nil {
		s.respondResolveErr(w, err, "reconcile preview")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"preview": plan})
}

// reconcileApply applies the resolution (one transaction, audited) and returns the result.
func (s *Server) reconcileApply(w http.ResponseWriter, r *http.Request, op operator) {
	req, ok := s.decodeReconcileReq(w, r, op)
	if !ok {
		return
	}
	res, err := s.resolver.Apply(r.Context(), req)
	if err != nil {
		s.respondResolveErr(w, err, "reconcile apply")
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// decodeReconcileReq parses the body and stamps the cycle id (from the path) + the
// authenticated operator name (NEVER taken from the body — Operator is json:"-").
func (s *Server) decodeReconcileReq(w http.ResponseWriter, r *http.Request, op operator) (opreconcile.Request, bool) {
	id, ok := pathID(w, r)
	if !ok {
		return opreconcile.Request{}, false
	}
	var req opreconcile.Request
	if !decodeBody(w, r, &req) {
		return opreconcile.Request{}, false
	}
	req.CycleID = id
	req.Operator = op.name
	return req, true
}

// respondResolveErr maps a resolver error: operator-input validation → 400, else 500.
func (s *Server) respondResolveErr(w http.ResponseWriter, err error, what string) {
	if opreconcile.IsValidation(err) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	s.log.Warn(what+" failed", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": what + " failed"})
}

// reconcileActions documents the supported resolution actions for the UI (static).
func reconcileActions() []map[string]any {
	return []map[string]any{
		{"action": opreconcile.ActionCancelZeroExposure, "needs_fill": false, "description": "no exposure remains → cancel cycle + orders, release lock"},
		{"action": opreconcile.ActionAttachExchangeOrderID, "needs_fill": false, "description": "attach a known exchange order id (no state change)"},
		{"action": opreconcile.ActionMarkBuyFilled, "needs_fill": true, "description": "record the buy fill → BUY_FILLED (lock held; sell resumes)"},
		{"action": opreconcile.ActionMarkBuyZeroFilled, "needs_fill": false, "description": "buy got nothing → CANCELLED, release lock"},
		{"action": opreconcile.ActionMarkSellFilled, "needs_fill": true, "description": "record the full sell fill → CLOSED, release lock"},
		{"action": opreconcile.ActionMarkSellPartiallyFilled, "needs_fill": true, "description": "record a partial sell → SELL_PARTIALLY_FILLED (lock held)"},
		{"action": opreconcile.ActionMarkOrderCancelledZeroFill, "needs_fill": false, "description": "cancel one order with zero fill (cycle stays NEEDS_RECONCILE)"},
		{"action": opreconcile.ActionKeepNeedsReconcile, "needs_fill": false, "description": "keep in NEEDS_RECONCILE (record an audit note only)"},
		{"action": opreconcile.ActionMarkFailed, "needs_fill": false, "description": "mark unrecoverable FAILED (lock released only if no exposure)"},
	}
}
