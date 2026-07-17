// Package opreconcile is the OPERATOR resolution tool for NEEDS_RECONCILE cycles/orders
// (PR21). It is a LOCAL tool: it inspects DB state and applies an explicit, validated,
// audited resolution chosen by an authenticated operator. It NEVER places or cancels an
// order on an exchange (no PrivateClient is held — see the package's lack of any exchange
// import), there is no automatic/blind close, every state change goes through
// internal/state, and a symbol lock is released only when the resolution PROVES no
// remaining exposure.
//
// Flow: Preview (read-only — computes the exact proposed state changes + a state fingerprint,
// writes nothing) then Apply (one transaction — locks the cycle, all its orders, all active
// exchange requests and the active symbol lock FOR UPDATE; re-validates; verifies the state
// still matches the previewed fingerprint (else 409); applies via the state machine; records
// any supplied fill; releases the lock only when exposure is PROVEN_ZERO; writes an audit row).
//
// Safety properties (PR21 blockers):
//  1. A recorded filled_quantity of 0 is NOT proof of zero exposure — exposure is classified
//     PROVEN_ZERO / OPEN / UNKNOWN / INCONSISTENT, and the lock is released only for PROVEN_ZERO
//     (UNKNOWN requires an explicit external resolution).
//  2. All active related exchange requests (mutating AND read-only recovery) are detected and
//     locked; a cycle cannot be resolved out of NEEDS_RECONCILE while one can still execute.
//  3. Order role/side is validated from the PERSISTED order (a buy action cannot touch a sell).
//  4. Order state and cycle state are computed SEPARATELY from cumulative fills.
//  5. A fill against a TERMINAL order is refused; a discovered fill uses an explicit, audited
//     terminal-order correction instead.
//  6. Apply requires a matching preview fingerprint (no blind apply; 409 on any state drift).
//  7. The preview shows EVERY mutation (all orders, all active requests, fill, accounting, lock).
//  8. attach_exchange_order_id is idempotent/conflict-checked/uniqueness-checked with a version CAS.
//  9. Cumulative fills/exposure are computed only from FOR UPDATE-locked rows.
//  10. A final-state read error rolls the whole transaction back (state + audit stay atomic).
package opreconcile

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/state"
)

// Action is a resolution action key. Each has explicit required inputs + state effects.
type Action string

const (
	ActionCancelZeroExposure         Action = "cancel_zero_exposure"           // proven-zero exposure -> cancel cycle + orders, release lock
	ActionAttachExchangeOrderID      Action = "attach_exchange_order_id"       // set orders.exchange_order_id (idempotent/conflict-checked), no state change
	ActionMarkBuyFilled              Action = "mark_buy_filled"                // record buy fill that COMPLETES the order -> FILLED / BUY_FILLED (lock held)
	ActionMarkBuyPartiallyFilled     Action = "mark_buy_partially_filled"      // record partial buy fill -> PARTIALLY_FILLED / BUY_PARTIALLY_FILLED (lock held)
	ActionMarkBuyZeroFilled          Action = "mark_buy_zero_filled"           // proven-zero buy -> CANCELLED, release lock
	ActionMarkSellFilled             Action = "mark_sell_filled"               // record sell fill that CLOSES exposure -> CLOSED, release lock (proven zero)
	ActionMarkSellPartiallyFilled    Action = "mark_sell_partially_filled"     // record sell partial (exposure remains) -> SELL_PARTIALLY_FILLED (lock held)
	ActionMarkOrderCancelledZeroFill Action = "mark_order_cancelled_zero_fill" // cancel one order, zero fill (cycle stays NEEDS_RECONCILE, lock held)
	ActionCorrectTerminalOrderFill   Action = "correct_terminal_order_fill"    // record a fill discovered on a TERMINAL order (explicit correction, cycle unchanged)
	ActionKeepNeedsReconcile         Action = "keep_needs_reconcile"           // leave as-is (audit only)
	ActionMarkFailed                 Action = "mark_failed"                    // unrecoverable -> FAILED (lock released only for proven zero, else external-confirm)
)

// Exposure classifies the cycle's base-asset exposure (PR21 blocker 1). A recorded zero fill is
// NOT proof of zero exposure: an order that reached (or may have reached) the venue with an
// unconfirmed outcome is UNKNOWN, not PROVEN_ZERO.
type Exposure string

const (
	ExposureProvenZero   Exposure = "PROVEN_ZERO"  // net 0 AND no order carries unconfirmed venue risk
	ExposureOpen         Exposure = "OPEN"         // recorded net inventory > 0
	ExposureUnknown      Exposure = "UNKNOWN"      // recorded net 0 but an order may hold unrecorded inventory
	ExposureInconsistent Exposure = "INCONSISTENT" // recorded sells exceed recorded buys (data problem)
)

// FillData is operator-supplied fill information. All numbers are decimal strings.
type FillData struct {
	ExchangeFillID string `json:"exchange_fill_id"`
	Quantity       string `json:"quantity"`
	Price          string `json:"price"`
	Fee            string `json:"fee"`
	FeeAsset       string `json:"fee_asset"`
	Side           string `json:"side"` // "buy" or "sell"
}

// Request is one resolution request.
type Request struct {
	CycleID         int64     `json:"cycle_id"`
	Action          Action    `json:"action"`
	OrderID         int64     `json:"order_id"`          // target order (0 = derive by role)
	ExchangeOrderID string    `json:"exchange_order_id"` // for attach_exchange_order_id
	Fill            *FillData `json:"fill"`
	Reason          string    `json:"reason"`
	// mark_failed / cancel with open/unknown exposure is REFUSED unless the operator explicitly
	// confirms the exposure was handled outside the system. These two fields are that override.
	ExternalResolutionConfirmed bool   `json:"external_resolution_confirmed"`
	ExternalResolutionReason    string `json:"external_resolution_reason"`
	// Operator and ExpectedStateHash are set SERVER-SIDE (from the session / preview token),
	// never from the request body.
	Operator          string `json:"-"`
	ExpectedStateHash string `json:"-"`
}

// OrderChange is one order's proposed transition (PR21 blocker 7 — the preview shows them ALL).
type OrderChange struct {
	OrderID  int64  `json:"order_id"`
	Role     string `json:"role"`
	OldState string `json:"old_state"`
	NewState string `json:"new_state"`
}

// RequestChange is one active exchange request the resolution observed. Blocking=true means it
// still can execute and therefore prevents resolving the cycle out of NEEDS_RECONCILE.
type RequestChange struct {
	RequestID int64  `json:"request_id"`
	Type      string `json:"request_type"`
	Status    string `json:"status"`
	OrderID   int64  `json:"order_id,omitempty"`
	Blocking  bool   `json:"blocking"`
}

// AccountingChange is the cumulative accounting a recorded fill will write.
type AccountingChange struct {
	OrderID       int64  `json:"order_id"`
	AddQuantity   string `json:"add_quantity"`
	NewFilled     string `json:"new_filled_quantity"`
	NewQuoteSpent string `json:"new_quote_spent"`
	NewAvgPrice   string `json:"new_avg_fill_price"`
}

// Plan is the previewed/applied effect of a resolution (the exact proposed changes).
type Plan struct {
	CycleID       int64  `json:"cycle_id"`
	Action        Action `json:"action"`
	OrderID       int64  `json:"order_id,omitempty"` // primary target order
	OldCycleState string `json:"old_cycle_state"`
	NewCycleState string `json:"new_cycle_state"`
	OldOrderState string `json:"old_order_state,omitempty"`
	NewOrderState string `json:"new_order_state,omitempty"`

	// PR21 blocker 7 — every mutation is visible.
	OrderChanges   []OrderChange     `json:"order_changes"`
	RequestChanges []RequestChange   `json:"request_changes"`
	FillToInsert   *FillData         `json:"fill_to_insert,omitempty"`
	Accounting     *AccountingChange `json:"accounting_changes,omitempty"`
	LockChange     string            `json:"lock_change"` // "held" | "released"
	ExposureBefore string            `json:"exposure_before"`
	ExposureAfter  string            `json:"exposure_after"`
	ExposureClass  Exposure          `json:"exposure_classification"`
	ExecutionMode  string            `json:"execution_mode"` // "live" | "dry_run"

	RecordsFill    bool     `json:"records_fill"`
	NetExposure    string   `json:"net_exposure"` // == exposure_before (kept for back-compat)
	LockReleased   bool     `json:"lock_released"`
	Warnings       []string `json:"warnings,omitempty"`
	RequiresReason bool     `json:"requires_reason"`

	ExposureUnresolved bool `json:"exposure_unresolved,omitempty"`
	Downgraded         bool `json:"downgraded,omitempty"`
	ExternalConfirmed  bool `json:"external_resolution_confirmed,omitempty"`

	// StateHash is a fingerprint of the EXACT cycle/order/request/lock state + operation payload
	// this plan was built against. Apply refuses unless the caller echoes it back and it still
	// matches the (locked) live state — no blind apply, and any drift is a 409 (PR21 blocker 6).
	StateHash string `json:"state_hash"`
}

// Result is the outcome of Apply.
type Result struct {
	Plan
	AuditID int64 `json:"audit_id"`
	Applied bool  `json:"applied"`
}

// ValidationError marks an operator input problem (-> HTTP 400). It carries no secrets.
type ValidationError struct{ Msg string }

func (e ValidationError) Error() string { return e.Msg }

func validationf(format string, a ...any) error {
	return ValidationError{Msg: fmt.Sprintf(format, a...)}
}

// IsValidation reports whether err is an operator-input validation error.
func IsValidation(err error) bool {
	var v ValidationError
	return errors.As(err, &v)
}

// ConflictError marks a lost-update / stale-preview conflict (-> HTTP 409). The live state no
// longer matches the previewed fingerprint; the operator must re-preview.
type ConflictError struct{ Msg string }

func (e ConflictError) Error() string { return e.Msg }

func conflictf(format string, a ...any) error { return ConflictError{Msg: fmt.Sprintf(format, a...)} }

// IsConflict reports whether err is a stale-state conflict.
func IsConflict(err error) bool {
	var c ConflictError
	return errors.As(err, &c)
}

// Resolver applies operator resolutions. It holds ONLY a DB handle + clock + logger — no
// exchange client, so it can never place/cancel an order (structurally read-only w.r.t. any venue).
type Resolver struct {
	db  *sql.DB
	clk clock.Clock
	log *slog.Logger
	// faultBeforeCommit is a TEST-ONLY fault-injection hook. When non-nil it is invoked just
	// before the apply transaction commits; returning an error forces a full rollback, proving an
	// apply can never leave state/audit partially written. NEVER set in production.
	faultBeforeCommit func() error
}

// New builds a Resolver.
func New(db *sql.DB, clk clock.Clock, log *slog.Logger) *Resolver {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Resolver{db: db, clk: clk, log: log}
}

// Preview computes the proposed resolution WITHOUT mutating anything (read-only), including the
// StateHash the operator must echo back to Apply. A ValidationError shows the operator the exact
// problem before applying.
func (r *Resolver) Preview(ctx context.Context, req Request) (Plan, error) {
	c, err := r.loadCtx(ctx, r.db, req.CycleID)
	if err != nil {
		return Plan{}, err
	}
	active, err := r.loadActiveRequests(ctx, r.db, c)
	if err != nil {
		return Plan{}, err
	}
	return r.buildPlan(ctx, r.db, c, active, req)
}

// Classify returns the exposure classification and recorded net for a cycle (read-only), for the
// detail view. A not-found cycle yields a ValidationError.
func (r *Resolver) Classify(ctx context.Context, cycleID int64) (Exposure, string, error) {
	c, err := r.loadCtx(ctx, r.db, cycleID)
	if err != nil {
		return "", "", err
	}
	class, net := c.classifyExposure()
	return class, net.String(), nil
}

// Apply runs the resolution in ONE transaction: lock the cycle + all its orders + all active
// requests + the active symbol lock FOR UPDATE, re-validate, verify the previewed fingerprint,
// apply via the state machine, record any fill, release the lock only when exposure is proven
// zero, and write the audit row. Any failure rolls everything back. No exchange I/O.
func (r *Resolver) Apply(ctx context.Context, req Request) (Result, error) {
	if strings.TrimSpace(req.Reason) == "" {
		return Result{}, validationf("a reason is required to resolve a NEEDS_RECONCILE case")
	}
	if strings.TrimSpace(req.Operator) == "" {
		return Result{}, validationf("an authenticated operator is required")
	}
	if strings.TrimSpace(req.ExpectedStateHash) == "" {
		return Result{}, validationf("a preview is required before apply (missing state fingerprint)")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()

	c, err := r.loadCtx(ctx, tx, req.CycleID) // cycle + orders + active lock locked FOR UPDATE
	if err != nil {
		return Result{}, err
	}
	active, err := r.loadActiveRequests(ctx, tx, c) // active requests locked FOR UPDATE
	if err != nil {
		return Result{}, err
	}
	// Fingerprint the LOCKED live state and compare to the previewed one BEFORE validating the
	// action — any drift in cycle/order/request/lock/payload is a conflict, not a bad request.
	if live := r.fingerprint(c, active, req); live != req.ExpectedStateHash {
		return Result{}, conflictf("state changed since preview — re-preview and retry")
	}
	plan, err := r.buildPlan(ctx, tx, c, active, req)
	if err != nil {
		return Result{}, err
	}
	before := c.snapshot()

	lockReleased, err := r.execute(ctx, tx, c, req, plan)
	if err != nil {
		return Result{}, err
	}
	plan.LockReleased = lockReleased
	if lockReleased {
		plan.LockChange = "released"
	}

	// PR21 blocker 10: a failure loading/serializing the final state must roll the WHOLE tx back —
	// the state mutation and its audit record stay atomic.
	after, err := r.loadCtx(ctx, tx, req.CycleID)
	if err != nil {
		return Result{}, fmt.Errorf("load post-change state for audit: %w", err)
	}
	auditID, err := r.writeAudit(ctx, tx, c, req, plan, before, after.snapshot())
	if err != nil {
		return Result{}, err
	}
	if r.faultBeforeCommit != nil {
		if ferr := r.faultBeforeCommit(); ferr != nil {
			return Result{}, ferr // deferred tx.Rollback() undoes EVERYTHING (state + audit)
		}
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	return Result{Plan: plan, AuditID: auditID, Applied: true}, nil
}

// ---- context loading (all rows FOR UPDATE inside a tx — PR21 blocker 9) ----

type orderRow struct {
	id              int64
	role            string
	side            string
	state           state.OrderState
	version         int64
	quantity        decimal.Decimal
	filled          decimal.Decimal
	quoteSpent      decimal.Decimal
	exchangeOrderID string
}

type lockInfo struct {
	id    int64
	state string
}

type cycleCtx struct {
	cycleID      int64
	cycleState   state.CycleState
	cycleVersion int64
	dryRun       bool
	canonical    string
	baseAsset    string
	exchangeID   int64
	exchangeCode string
	buy          *orderRow
	sells        []orderRow
	lock         lockInfo // the ACTIVE symbol lock (id 0 when none)
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (r *Resolver) loadCtx(ctx context.Context, q queryer, cycleID int64) (cycleCtx, error) {
	forUpdate := ""
	inTx := false
	if _, ok := q.(*sql.Tx); ok {
		forUpdate = " FOR UPDATE"
		inTx = true
	}
	var c cycleCtx
	var st string
	var dry int
	err := q.QueryRowContext(ctx,
		"SELECT c.id, c.state, c.version, c.dry_run, c.canonical_symbol, c.buy_exchange_id, e.code "+
			"FROM cycles c JOIN exchanges e ON e.id=c.buy_exchange_id WHERE c.id=?"+forUpdate, cycleID).
		Scan(&c.cycleID, &st, &c.cycleVersion, &dry, &c.canonical, &c.exchangeID, &c.exchangeCode)
	if errors.Is(err, sql.ErrNoRows) {
		return cycleCtx{}, validationf("cycle %d not found", cycleID)
	}
	if err != nil {
		return cycleCtx{}, err
	}
	c.cycleState = state.CycleState(st)
	c.dryRun = dry != 0
	c.baseAsset = baseOf(c.canonical)

	rows, err := q.QueryContext(ctx,
		"SELECT id, role, side, state, version, COALESCE(quantity,0), COALESCE(filled_quantity,0), COALESCE(quote_spent,0), COALESCE(exchange_order_id,'') FROM orders WHERE cycle_id=? ORDER BY id"+forUpdate, cycleID)
	if err != nil {
		return cycleCtx{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var o orderRow
		var ost string
		var qty, filled, quote string
		if err := rows.Scan(&o.id, &o.role, &o.side, &ost, &o.version, &qty, &filled, &quote, &o.exchangeOrderID); err != nil {
			return cycleCtx{}, err
		}
		o.state = state.OrderState(ost)
		o.quantity = decOrZero(qty)
		o.filled = decOrZero(filled)
		o.quoteSpent = decOrZero(quote)
		if o.role == "entry_buy" {
			cp := o
			c.buy = &cp
		} else {
			c.sells = append(c.sells, o)
		}
	}
	if err := rows.Err(); err != nil {
		return cycleCtx{}, err
	}

	// Lock the ACTIVE symbol lock row for this cycle (blocker 9). symbollock.ActiveByCycle does not
	// FOR UPDATE, so we read it directly with the lock clause when in a tx.
	var lid int64
	var lst string
	lerr := q.QueryRowContext(ctx,
		"SELECT id, state FROM symbol_locks WHERE cycle_id=? AND state='ACTIVE' LIMIT 1"+forUpdate, cycleID).Scan(&lid, &lst)
	switch {
	case errors.Is(lerr, sql.ErrNoRows):
		// no active lock (already released) — fine
	case lerr != nil:
		return cycleCtx{}, lerr
	default:
		c.lock = lockInfo{id: lid, state: lst}
	}
	_ = inTx
	return c, nil
}

// loadActiveRequests loads (and FOR UPDATE-locks in a tx) all NON-TERMINAL exchange requests
// related to the cycle — matched by the cycle_id OR by any of the cycle's order ids. Ownership is
// authoritative (the cycle's real order set); a request's own claimed cycle_id is not trusted as
// the sole key (PR21 blocker 2/9).
func (r *Resolver) loadActiveRequests(ctx context.Context, q queryer, c cycleCtx) ([]RequestChange, error) {
	forUpdate := ""
	if _, ok := q.(*sql.Tx); ok {
		forUpdate = " FOR UPDATE"
	}
	orderIDs := c.orderIDs()
	// Build: WHERE status IN (active) AND (cycle_id=? OR order_id IN (...)).
	var sb strings.Builder
	sb.WriteString("SELECT id, request_type, status, COALESCE(order_id,0) FROM exchange_requests WHERE status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT') AND (cycle_id=?")
	args := []any{c.cycleID}
	if len(orderIDs) > 0 {
		sb.WriteString(" OR order_id IN (")
		for i, id := range orderIDs {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("?")
			args = append(args, id)
		}
		sb.WriteString(")")
	}
	sb.WriteString(") ORDER BY id")
	sb.WriteString(forUpdate)
	rows, err := q.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RequestChange
	for rows.Next() {
		var rc RequestChange
		if err := rows.Scan(&rc.RequestID, &rc.Type, &rc.Status, &rc.OrderID); err != nil {
			return nil, err
		}
		out = append(out, rc)
	}
	return out, rows.Err()
}

// ---- exposure + fingerprint ----

func (c cycleCtx) buyFilled() decimal.Decimal {
	if c.buy == nil {
		return decimal.Zero
	}
	return c.buy.filled
}

func (c cycleCtx) sellFilled() decimal.Decimal {
	sum := decimal.Zero
	for _, s := range c.sells {
		sum = sum.Add(s.filled)
	}
	return sum
}

func (c cycleCtx) netExposure() decimal.Decimal { return c.buyFilled().Sub(c.sellFilled()) }

// classifyExposure returns the exposure classification (PR21 blocker 1) and the recorded net.
func (c cycleCtx) classifyExposure() (Exposure, decimal.Decimal) {
	net := c.netExposure()
	if net.IsNegative() {
		return ExposureInconsistent, net
	}
	if net.IsPositive() {
		return ExposureOpen, net
	}
	for _, o := range c.allOrders() {
		if o.hasVenueRisk() {
			return ExposureUnknown, net
		}
	}
	return ExposureProvenZero, net
}

// hasVenueRisk reports whether an order MIGHT hold unrecorded exposure: it reached (or may have
// reached) the venue with an outcome not yet proven. A zero recorded fill on such an order is NOT
// proof of zero exposure (PR21 blocker 1).
func (o orderRow) hasVenueRisk() bool {
	if strings.TrimSpace(o.exchangeOrderID) != "" {
		return true // has a venue order id ⇒ it reached the exchange
	}
	switch o.state {
	case state.OrderSubmitted, state.OrderAcked, state.OrderPartiallyFilled,
		state.OrderCancelPending, state.OrderNeedsReconcile:
		return true // may have executed on the venue with an unconfirmed outcome
	}
	return false
}

func (c cycleCtx) executionMode() string {
	if c.dryRun {
		return "dry_run"
	}
	return "live"
}

func (c cycleCtx) orderIDs() []int64 {
	var ids []int64
	if c.buy != nil {
		ids = append(ids, c.buy.id)
	}
	for _, s := range c.sells {
		ids = append(ids, s.id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// fingerprint is the state+payload hash bound into a preview and re-checked at apply (blocker 6).
func (r *Resolver) fingerprint(c cycleCtx, active []RequestChange, req Request) string {
	h := sha256.New()
	fmt.Fprintf(h, "cycle:%d:v%d:%s:%s\n", c.cycleID, c.cycleVersion, c.cycleState, c.executionMode())
	for _, o := range c.allOrders() {
		fmt.Fprintf(h, "order:%d:v%d:%s:%s:%s:%s\n", o.id, o.version, o.state, o.quantity, o.filled, o.exchangeOrderID)
	}
	sorted := append([]RequestChange(nil), active...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].RequestID < sorted[j].RequestID })
	for _, a := range sorted {
		fmt.Fprintf(h, "req:%d:%s:%s:%d\n", a.RequestID, a.Type, a.Status, a.OrderID)
	}
	fmt.Fprintf(h, "lock:%d:%s\n", c.lock.id, c.lock.state)
	fmt.Fprintf(h, "op:%s|order:%d|exoid:%s|operator:%s|reason:%s\n",
		req.Action, req.OrderID, strings.TrimSpace(req.ExchangeOrderID), req.Operator, req.Reason)
	if req.Fill != nil {
		fmt.Fprintf(h, "fill:%s:%s:%s:%s:%s:%s\n",
			req.Fill.ExchangeFillID, req.Fill.Quantity, req.Fill.Price, req.Fill.Fee, req.Fill.FeeAsset, req.Fill.Side)
	}
	fmt.Fprintf(h, "ext:%v:%s\n", req.ExternalResolutionConfirmed, strings.TrimSpace(req.ExternalResolutionReason))
	return hex.EncodeToString(h.Sum(nil))
}

func (c cycleCtx) snapshot() json.RawMessage {
	class, net := c.classifyExposure()
	m := map[string]any{
		"cycle_id":                c.cycleID,
		"cycle_state":             string(c.cycleState),
		"net_exposure":            net.String(),
		"exposure_classification": string(class),
		"execution_mode":          c.executionMode(),
	}
	var ords []map[string]any
	if c.buy != nil {
		ords = append(ords, orderSnap(*c.buy))
	}
	for _, s := range c.sells {
		ords = append(ords, orderSnap(s))
	}
	m["orders"] = ords
	b, _ := json.Marshal(m)
	return b
}

func orderSnap(o orderRow) map[string]any {
	return map[string]any{
		"id": o.id, "role": o.role, "side": o.side, "state": string(o.state),
		"quantity": o.quantity.String(), "filled": o.filled.String(),
		"exchange_order_id": o.exchangeOrderID,
	}
}

// writeAudit records an immutable resolution row (no secrets).
func (r *Resolver) writeAudit(ctx context.Context, tx *sql.Tx, c cycleCtx, req Request, plan Plan, before, after json.RawMessage) (int64, error) {
	var fillJSON any
	if req.Fill != nil {
		b, _ := json.Marshal(req.Fill)
		fillJSON = b
	}
	var orderID any
	if plan.OrderID != 0 {
		orderID = plan.OrderID
	}
	reason := req.Reason
	if plan.ExternalConfirmed && strings.TrimSpace(req.ExternalResolutionReason) != "" {
		reason = reason + " | external_resolution_confirmed: " + strings.TrimSpace(req.ExternalResolutionReason)
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO reconcile_resolutions
  (cycle_id, order_id, operator, action, old_cycle_state, new_cycle_state, old_order_state, new_order_state,
   reason, fill_json, before_json, after_json, lock_released, external_resolution_confirmed, exposure_classification)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.cycleID, orderID, req.Operator, string(req.Action),
		plan.OldCycleState, plan.NewCycleState, nullIfEmpty(plan.OldOrderState), nullIfEmpty(plan.NewOrderState),
		reason, fillJSON, []byte(before), []byte(after), b2i(plan.LockReleased), b2i(plan.ExternalConfirmed), string(plan.ExposureClass))
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

// ---- small helpers ----

func baseOf(canonical string) string {
	if i := strings.IndexByte(canonical, '/'); i >= 0 {
		return canonical[:i]
	}
	return canonical
}

func decOrZero(s string) decimal.Decimal {
	d, err := decimal.NewFromString(strings.TrimSpace(s))
	if err != nil {
		return decimal.Zero
	}
	return d
}

func parseDec(s string) (decimal.Decimal, bool) {
	d, err := decimal.NewFromString(strings.TrimSpace(s))
	if err != nil {
		return decimal.Zero, false
	}
	return d, true
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
