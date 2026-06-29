// Package opreconcile is the OPERATOR resolution tool for NEEDS_RECONCILE cycles/orders
// (PR21). It is a LOCAL tool: it inspects DB state and applies an explicit, validated,
// audited resolution chosen by an authenticated operator. It NEVER places or cancels an
// order on an exchange (no PrivateClient is held — see the package's lack of any exchange
// import), there is no automatic/blind close, every state change goes through
// internal/state, and a symbol lock is released only when the resolution proves no
// remaining exposure.
//
// Flow: Preview (read-only — computes the exact proposed state changes + warnings, writes
// nothing) then Apply (one transaction — re-validates, applies via the state machine,
// records any supplied fill, releases the lock only when safe, writes an audit row).
package opreconcile

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/state"
)

// Action is a resolution action key. Each has explicit required inputs + state effects.
type Action string

const (
	ActionCancelZeroExposure         Action = "cancel_zero_exposure"           // no exposure -> cancel cycle + orders, release lock
	ActionAttachExchangeOrderID      Action = "attach_exchange_order_id"       // set orders.exchange_order_id, no state change
	ActionMarkBuyFilled              Action = "mark_buy_filled"                // record buy fill -> BUY_FILLED (lock held)
	ActionMarkBuyZeroFilled          Action = "mark_buy_zero_filled"           // buy got nothing -> CANCELLED, release lock
	ActionMarkSellFilled             Action = "mark_sell_filled"               // record sell fill (full exit) -> CLOSED, release lock
	ActionMarkSellPartiallyFilled    Action = "mark_sell_partially_filled"     // record sell partial -> SELL_PARTIALLY_FILLED (lock held)
	ActionMarkOrderCancelledZeroFill Action = "mark_order_cancelled_zero_fill" // cancel one order, zero fill (cycle stays NEEDS_RECONCILE)
	ActionKeepNeedsReconcile         Action = "keep_needs_reconcile"           // leave as-is (audit only)
	ActionMarkFailed                 Action = "mark_failed"                    // unrecoverable -> FAILED (lock released only if no exposure)
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
	Operator        string    `json:"-"` // set from the authenticated session, never the body
}

// Plan is the previewed/applied effect of a resolution (the exact proposed changes).
type Plan struct {
	CycleID        int64    `json:"cycle_id"`
	Action         Action   `json:"action"`
	OrderID        int64    `json:"order_id,omitempty"`
	OldCycleState  string   `json:"old_cycle_state"`
	NewCycleState  string   `json:"new_cycle_state"`
	OldOrderState  string   `json:"old_order_state,omitempty"`
	NewOrderState  string   `json:"new_order_state,omitempty"`
	RecordsFill    bool     `json:"records_fill"`
	NetExposure    string   `json:"net_exposure"`       // base inventory still held before the action
	LockReleased   bool     `json:"lock_released"`      // whether applying releases the symbol lock
	Warnings       []string `json:"warnings,omitempty"` // advisory (e.g. balance cross-check) — never blocks
	RequiresReason bool     `json:"requires_reason"`    // a reason is mandatory for every apply
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

// Resolver applies operator resolutions. It holds ONLY a DB handle + clock + logger — no
// exchange client, so it can never place/cancel an order (structurally read-only w.r.t.
// any venue).
type Resolver struct {
	db  *sql.DB
	clk clock.Clock
	log *slog.Logger
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

// Preview computes the proposed resolution WITHOUT mutating anything (read-only). It
// returns a ValidationError when the request is invalid, so the operator sees the exact
// problem (or the exact proposed state changes) before applying.
func (r *Resolver) Preview(ctx context.Context, req Request) (Plan, error) {
	c, err := r.loadCtx(ctx, r.db, req.CycleID)
	if err != nil {
		return Plan{}, err
	}
	return r.buildPlan(ctx, r.db, c, req)
}

// Apply runs the resolution in ONE transaction: re-load (FOR UPDATE) + re-validate +
// apply via the state machine + record any fill + release the lock only when safe + write
// the audit row. A validation failure rolls everything back. It performs no exchange I/O.
func (r *Resolver) Apply(ctx context.Context, req Request) (Result, error) {
	if strings.TrimSpace(req.Reason) == "" {
		return Result{}, validationf("a reason is required to resolve a NEEDS_RECONCILE case")
	}
	if strings.TrimSpace(req.Operator) == "" {
		return Result{}, validationf("an authenticated operator is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()

	c, err := r.loadCtx(ctx, tx, req.CycleID) // FOR UPDATE handled inside via SELECT ... FOR UPDATE
	if err != nil {
		return Result{}, err
	}
	plan, err := r.buildPlan(ctx, tx, c, req)
	if err != nil {
		return Result{}, err
	}
	before := c.snapshot()

	lockReleased, err := r.execute(ctx, tx, c, req, plan)
	if err != nil {
		return Result{}, err
	}
	plan.LockReleased = lockReleased

	after, _ := r.loadCtx(ctx, tx, req.CycleID)
	auditID, err := r.writeAudit(ctx, tx, c, req, plan, before, after.snapshot())
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	return Result{Plan: plan, AuditID: auditID, Applied: true}, nil
}

// ---- context loading ----

type orderRow struct {
	id              int64
	role            string
	state           state.OrderState
	version         int64
	quantity        decimal.Decimal
	filled          decimal.Decimal
	quoteSpent      decimal.Decimal
	exchangeOrderID string
}

type cycleCtx struct {
	cycleID      int64
	cycleState   state.CycleState
	cycleVersion int64
	canonical    string
	baseAsset    string
	exchangeID   int64
	exchangeCode string
	buy          *orderRow
	sells        []orderRow
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// loadCtx loads the cycle + its orders. When q is a *sql.Tx the cycle row is locked
// FOR UPDATE so the apply re-validates against a stable snapshot.
func (r *Resolver) loadCtx(ctx context.Context, q queryer, cycleID int64) (cycleCtx, error) {
	forUpdate := ""
	if _, ok := q.(*sql.Tx); ok {
		forUpdate = " FOR UPDATE"
	}
	var c cycleCtx
	var st string
	err := q.QueryRowContext(ctx,
		"SELECT c.id, c.state, c.version, c.canonical_symbol, c.buy_exchange_id, e.code "+
			"FROM cycles c JOIN exchanges e ON e.id=c.buy_exchange_id WHERE c.id=?"+forUpdate, cycleID).
		Scan(&c.cycleID, &st, &c.cycleVersion, &c.canonical, &c.exchangeID, &c.exchangeCode)
	if errors.Is(err, sql.ErrNoRows) {
		return cycleCtx{}, validationf("cycle %d not found", cycleID)
	}
	if err != nil {
		return cycleCtx{}, err
	}
	c.cycleState = state.CycleState(st)
	c.baseAsset = baseOf(c.canonical)

	rows, err := q.QueryContext(ctx,
		"SELECT id, role, state, version, COALESCE(quantity,0), COALESCE(filled_quantity,0), COALESCE(quote_spent,0), COALESCE(exchange_order_id,'') FROM orders WHERE cycle_id=? ORDER BY id", cycleID)
	if err != nil {
		return cycleCtx{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var o orderRow
		var ost string
		var qty, filled, quote string
		if err := rows.Scan(&o.id, &o.role, &ost, &o.version, &qty, &filled, &quote, &o.exchangeOrderID); err != nil {
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
	return c, rows.Err()
}

// buyFilled / sellFilled / netExposure compute base inventory still held.
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

func (c cycleCtx) snapshot() json.RawMessage {
	m := map[string]any{
		"cycle_id":     c.cycleID,
		"cycle_state":  string(c.cycleState),
		"net_exposure": c.netExposure().String(),
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
		"id": o.id, "role": o.role, "state": string(o.state),
		"quantity": o.quantity.String(), "filled": o.filled.String(),
		"exchange_order_id": o.exchangeOrderID,
	}
}

// writeAudit records an immutable resolution row (no secrets). before/after are JSON
// snapshots; fill_json captures any supplied fill data.
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
	res, err := tx.ExecContext(ctx, `
INSERT INTO reconcile_resolutions
  (cycle_id, order_id, operator, action, old_cycle_state, new_cycle_state, old_order_state, new_order_state,
   reason, fill_json, before_json, after_json, lock_released)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.cycleID, orderID, req.Operator, string(req.Action),
		plan.OldCycleState, plan.NewCycleState, nullIfEmpty(plan.OldOrderState), nullIfEmpty(plan.NewOrderState),
		req.Reason, fillJSON, []byte(before), []byte(after), b2i(plan.LockReleased))
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
