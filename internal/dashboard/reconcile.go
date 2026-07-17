package dashboard

// Operator reconciliation HTTP layer (PR21). Wraps internal/opreconcile with the current session
// auth model (dashboard_sessions + HttpOnly cookie) and the exact-set reconcile capability.
//
// Every endpoint is gated by requireReconcileCapable (reconcile_operator or admin) — including the
// read-only list/detail/audit, so reconciliation state is never disclosed to an unauthenticated or
// unauthorized user. apply requires a matching prior preview (a short-lived, single-use, operator-
// bound token carrying the previewed state fingerprint); the resolver additionally re-checks the
// fingerprint against the LOCKED live state inside the apply transaction, so any drift is a 409.

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"v3TradeBot/internal/opreconcile"
)

// ---- preview-token store (short-lived, single-use, operator-bound) ----

type previewToken struct {
	operator  string
	cycleID   int64
	stateHash string
	expiresAt time.Time
	used      bool
}

type previewStore struct {
	mu   sync.Mutex
	toks map[string]previewToken
}

func newPreviewStore() *previewStore { return &previewStore{toks: map[string]previewToken{}} }

const previewTokenTTL = 5 * time.Minute

func (p *previewStore) mint(operator string, cycleID int64, stateHash string) (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(raw)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gcLocked()
	p.toks[tok] = previewToken{operator: operator, cycleID: cycleID, stateHash: stateHash, expiresAt: time.Now().Add(previewTokenTTL)}
	return tok, nil
}

// consumeStatus is the outcome of consuming a preview token at apply time.
type consumeStatus int

const (
	consumeOK consumeStatus = iota
	consumeMissing
	consumeExpired
	consumeWrongOperator
	consumeWrongCycle
)

// consume looks up and single-uses a token, enforcing operator + cycle binding and expiry.
func (p *previewStore) consume(tok, operator string, cycleID int64) (string, consumeStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gcLocked()
	t, ok := p.toks[tok]
	if !ok {
		return "", consumeMissing
	}
	// Single-use regardless of outcome: delete on first touch so a token can never be replayed.
	delete(p.toks, tok)
	if t.used || time.Now().After(t.expiresAt) {
		return "", consumeExpired
	}
	if t.operator != operator {
		return "", consumeWrongOperator
	}
	if t.cycleID != cycleID {
		return "", consumeWrongCycle
	}
	return t.stateHash, consumeOK
}

func (p *previewStore) gcLocked() {
	now := time.Now()
	for k, v := range p.toks {
		if now.After(v.expiresAt) {
			delete(p.toks, k)
		}
	}
}

// ---- request body ----

// reconcileReq is the JSON body for preview/apply. cycle_id comes from the path, operator from the
// session; neither may be set from the body. preview_token binds an apply to a prior preview.
type reconcileReq struct {
	Action                      opreconcile.Action    `json:"action"`
	OrderID                     int64                 `json:"order_id"`
	ExchangeOrderID             string                `json:"exchange_order_id"`
	Fill                        *opreconcile.FillData `json:"fill"`
	Reason                      string                `json:"reason"`
	ExternalResolutionConfirmed bool                  `json:"external_resolution_confirmed"`
	ExternalResolutionReason    string                `json:"external_resolution_reason"`
	PreviewToken                string                `json:"preview_token"`
}

func (b reconcileReq) toResolverReq(cycleID int64, operator string) opreconcile.Request {
	return opreconcile.Request{
		CycleID: cycleID, Action: b.Action, OrderID: b.OrderID, ExchangeOrderID: b.ExchangeOrderID,
		Fill: b.Fill, Reason: b.Reason,
		ExternalResolutionConfirmed: b.ExternalResolutionConfirmed,
		ExternalResolutionReason:    b.ExternalResolutionReason,
		Operator:                    operator,
	}
}

// ---- handlers ----

// reconcileList returns cycles currently in NEEDS_RECONCILE (with their execution mode).
func (s *Server) reconcileList(w http.ResponseWriter, r *http.Request) {
	s.list(w, r,
		"SELECT id, canonical_symbol, state, dry_run, version, buy_exchange_id, created_at, updated_at "+
			"FROM cycles WHERE state='NEEDS_RECONCILE' ORDER BY id DESC LIMIT ?",
		s.limit(r))
}

// reconcileAudit returns the immutable resolution audit trail.
func (s *Server) reconcileAudit(w http.ResponseWriter, r *http.Request) {
	s.list(w, r,
		"SELECT id, cycle_id, order_id, operator, action, old_cycle_state, new_cycle_state, "+
			"old_order_state, new_order_state, reason, lock_released, external_resolution_confirmed, "+
			"exposure_classification, created_at FROM reconcile_resolutions ORDER BY id DESC LIMIT ?",
		s.limit(r))
}

// reconcileDetail returns the full context for one cycle. EVERY sub-query error is a 500 — never a
// partial 200 that an operator could misread as "no data".
func (s *Server) reconcileDetail(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	cycle, err := s.oneRow(ctx, "SELECT * FROM cycles WHERE id=?", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if cycle == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "cycle not found"})
		return
	}
	ordersRows, err := s.rows(ctx, "SELECT * FROM orders WHERE cycle_id=? ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	fills, err := s.rows(ctx, "SELECT f.* FROM fills f JOIN orders o ON o.id=f.order_id WHERE o.cycle_id=? ORDER BY f.id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	activeReqs, err := s.rows(ctx,
		"SELECT id, request_type, status, order_id, exchange_id, created_at FROM exchange_requests "+
			"WHERE cycle_id=? AND status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT') ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	locks, err := s.rows(ctx, "SELECT id, scope, canonical_symbol, state, locked_at, released_at FROM symbol_locks WHERE cycle_id=? ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	cycEvents, err := s.rows(ctx, "SELECT * FROM cycle_state_events WHERE cycle_id=? ORDER BY id DESC LIMIT 50", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	priorRes, err := s.rows(ctx, "SELECT id, order_id, operator, action, old_cycle_state, new_cycle_state, reason, lock_released, exposure_classification, created_at FROM reconcile_resolutions WHERE cycle_id=? ORDER BY id DESC", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := map[string]any{
		"cycle":             cycle,
		"orders":            ordersRows,
		"fills":             fills,
		"active_requests":   activeReqs,
		"locks":             locks,
		"cycle_events":      cycEvents,
		"resolutions":       priorRes,
		"available_actions": availableActions(),
	}
	// Exposure classification (best-effort: only meaningful for a NEEDS_RECONCILE cycle). A DB error
	// here is a hard failure (500); a validation error (e.g. not NEEDS_RECONCILE) simply omits it.
	if s.reconciler != nil {
		class, net, cerr := s.reconciler.Classify(ctx, id)
		switch {
		case cerr == nil:
			resp["exposure_classification"] = string(class)
			resp["net_exposure"] = net
		case opreconcile.IsValidation(cerr):
			// cycle not in a classifiable state — omit
		default:
			s.fail(w, cerr)
			return
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// reconcilePreview computes the proposed resolution (read-only) and mints a preview token.
func (s *Server) reconcilePreview(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	var body reconcileReq
	if !decodeStrictJSON(w, r, &body) {
		return
	}
	if !s.validReason(w, body.Reason) {
		return
	}
	op := sessionFrom(r.Context()).Username
	plan, err := s.reconciler.Preview(r.Context(), body.toResolverReq(id, op))
	if err != nil {
		s.respondResolve(w, opreconcile.Result{}, err)
		return
	}
	tok, terr := s.previews.mint(op, id, plan.StateHash)
	if terr != nil {
		s.fail(w, terr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"preview": plan, "preview_token": tok})
}

// reconcileApply applies a resolution — but only against a matching prior preview token.
func (s *Server) reconcileApply(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	var body reconcileReq
	if !decodeStrictJSON(w, r, &body) {
		return
	}
	if !s.validReason(w, body.Reason) {
		return
	}
	op := sessionFrom(r.Context()).Username
	if body.PreviewToken == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "a preview is required before apply (missing preview_token)"})
		return
	}
	stateHash, status := s.previews.consume(body.PreviewToken, op, id)
	switch status {
	case consumeMissing, consumeExpired, consumeWrongCycle:
		writeJSON(w, http.StatusConflict, map[string]any{"error": "preview token missing/expired/for-another-cycle — re-preview and retry"})
		return
	case consumeWrongOperator:
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "preview token belongs to a different operator"})
		return
	}
	req := body.toResolverReq(id, op)
	req.ExpectedStateHash = stateHash
	res, err := s.reconciler.Apply(r.Context(), req)
	s.respondResolve(w, res, err)
}

// respondResolve maps a resolver outcome to an HTTP status: 200 ok, 400 validation, 409 conflict,
// 500 otherwise (the real error is logged, never leaked).
func (s *Server) respondResolve(w http.ResponseWriter, res opreconcile.Result, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, res)
	case opreconcile.IsValidation(err):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
	case opreconcile.IsConflict(err):
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
	default:
		s.log.Warn("reconcile resolution failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "reconcile resolution failed"})
	}
}

// availableActions is a static UI catalog of the resolution actions and their required inputs.
func availableActions() []map[string]any {
	return []map[string]any{
		{"action": opreconcile.ActionKeepNeedsReconcile, "needs_fill": false, "description": "leave in NEEDS_RECONCILE (audit note only)"},
		{"action": opreconcile.ActionAttachExchangeOrderID, "needs_fill": false, "description": "attach a discovered exchange_order_id (idempotent, conflict-checked)"},
		{"action": opreconcile.ActionCancelZeroExposure, "needs_fill": false, "description": "cancel the cycle — requires PROVEN_ZERO exposure (or external confirmation)"},
		{"action": opreconcile.ActionMarkBuyZeroFilled, "needs_fill": false, "description": "the buy got nothing — cancel + release lock (requires proven zero)"},
		{"action": opreconcile.ActionMarkBuyPartiallyFilled, "needs_fill": true, "description": "record a partial buy fill (order PARTIALLY_FILLED / cycle BUY_PARTIALLY_FILLED)"},
		{"action": opreconcile.ActionMarkBuyFilled, "needs_fill": true, "description": "record a buy fill that completes the order (order FILLED / cycle BUY_FILLED)"},
		{"action": opreconcile.ActionMarkSellPartiallyFilled, "needs_fill": true, "description": "record a partial sell fill (exposure remains, lock held)"},
		{"action": opreconcile.ActionMarkSellFilled, "needs_fill": true, "description": "record a sell fill that closes exposure (cycle CLOSED, lock released)"},
		{"action": opreconcile.ActionMarkOrderCancelledZeroFill, "needs_fill": false, "description": "cancel a single zero-fill order (cycle stays NEEDS_RECONCILE)"},
		{"action": opreconcile.ActionCorrectTerminalOrderFill, "needs_fill": true, "description": "record a fill discovered on a TERMINAL order (explicit correction)"},
		{"action": opreconcile.ActionMarkFailed, "needs_fill": false, "description": "mark FAILED — lock released only for proven zero, else external confirmation required"},
	}
}
