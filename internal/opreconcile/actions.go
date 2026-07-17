package opreconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/state"
	"v3TradeBot/internal/symbollock"
)

// buildPlan validates the request against the LOCKED cycle/orders/requests and computes the exact
// proposed changes (used by BOTH Preview — read-only — and Apply — re-validated in-tx). A bad
// request returns a ValidationError; nothing is mutated here.
func (r *Resolver) buildPlan(ctx context.Context, q queryer, c cycleCtx, active []RequestChange, req Request) (Plan, error) {
	if c.cycleState != state.CycleNeedsReconcile {
		return Plan{}, validationf("cycle %d is %s, not NEEDS_RECONCILE", c.cycleID, c.cycleState)
	}
	class, net := c.classifyExposure()
	if class == ExposureInconsistent {
		return Plan{}, validationf("cycle %d has inconsistent fills (recorded sells exceed recorded buys: net %s %s) — data must be corrected before resolution", c.cycleID, net, c.baseAsset)
	}
	p := Plan{
		CycleID: c.cycleID, Action: req.Action,
		OldCycleState: string(c.cycleState), NewCycleState: string(c.cycleState),
		ExposureBefore: net.String(), ExposureAfter: net.String(), ExposureClass: class,
		ExecutionMode: c.executionMode(), NetExposure: net.String(),
		LockChange: "held", RequiresReason: true,
		RequestChanges: active,
	}
	expectedNet := net // base inventory expected to remain AFTER the resolution

	switch req.Action {
	case ActionKeepNeedsReconcile:
		// no change; audit only. (Active requests are informational, never blocking.)

	case ActionAttachExchangeOrderID:
		if err := r.planAttach(ctx, q, c, req, &p); err != nil {
			return Plan{}, err
		}

	case ActionCancelZeroExposure:
		if err := requireNoActiveRequests(active, 0, "cancel the cycle"); err != nil {
			return Plan{}, err
		}
		if err := requireProvenZeroOrConfirmed(class, net, c.baseAsset, req, &p, "cancel"); err != nil {
			return Plan{}, err
		}
		for _, o := range c.allOrders() {
			if !state.IsTerminalOrder(o.state) {
				p.OrderChanges = append(p.OrderChanges, orderChange(o, state.OrderCancelled))
			}
		}
		p.NewCycleState = string(state.CycleCancelled)
		p.LockReleased, p.LockChange = true, "released"
		expectedNet = decimal.Zero

	case ActionMarkBuyZeroFilled:
		if err := requireNoActiveRequests(active, 0, "cancel the buy"); err != nil {
			return Plan{}, err
		}
		if c.buy == nil {
			return Plan{}, validationf("cycle %d has no buy order", c.cycleID)
		}
		if c.buy.filled.IsPositive() {
			return Plan{}, validationf("buy already has %s filled; cannot mark zero-filled", c.buy.filled)
		}
		// Zero recorded fill is NOT proof of zero exposure (blocker 1): the buy may have executed.
		if err := requireProvenZeroOrConfirmed(class, net, c.baseAsset, req, &p, "mark the buy zero-filled"); err != nil {
			return Plan{}, err
		}
		p.OrderID = c.buy.id
		p.OldOrderState, p.NewOrderState = string(c.buy.state), string(state.OrderCancelled)
		p.OrderChanges = append(p.OrderChanges, orderChange(*c.buy, state.OrderCancelled))
		p.NewCycleState = string(state.CycleCancelled)
		p.LockReleased, p.LockChange = true, "released"
		expectedNet = decimal.Zero

	case ActionMarkBuyFilled, ActionMarkBuyPartiallyFilled:
		ord, err := c.targetOrder(req, "entry_buy")
		if err != nil {
			return Plan{}, err
		}
		if err := requireNoActiveRequests(active, 0, "resolve the buy"); err != nil {
			return Plan{}, err
		}
		if state.IsTerminalOrder(ord.state) {
			return Plan{}, validationf("buy order %d is terminal (%s); use correct_terminal_order_fill to record a discovered fill", ord.id, ord.state)
		}
		qty, err := validateFill(ctx, q, c, ord, req.Fill, "buy")
		if err != nil {
			return Plan{}, err
		}
		cumulative := ord.filled.Add(qty)
		complete := cumulative.GreaterThanOrEqual(ord.quantity)
		if req.Action == ActionMarkBuyFilled && !complete {
			return Plan{}, validationf("buy fill does not complete the order (%s of %s filled) — use mark_buy_partially_filled", cumulative, ord.quantity)
		}
		if req.Action == ActionMarkBuyPartiallyFilled && complete {
			return Plan{}, validationf("buy fill completes the order (%s of %s) — use mark_buy_filled", cumulative, ord.quantity)
		}
		newOrder := state.OrderPartiallyFilled
		newCycle := state.CycleBuyPartiallyFilled
		if complete {
			newOrder, newCycle = state.OrderFilled, state.CycleBuyFilled
		}
		r.planFill(ord, qty, req.Fill, newOrder, &p)
		p.NewCycleState = string(newCycle)
		expectedNet = net.Add(qty) // inventory increases; lock stays held

	case ActionMarkSellPartiallyFilled, ActionMarkSellFilled:
		ord, err := c.targetOrder(req, "exit_sell")
		if err != nil {
			return Plan{}, err
		}
		if err := requireNoActiveRequests(active, 0, "resolve the sell"); err != nil {
			return Plan{}, err
		}
		if state.IsTerminalOrder(ord.state) {
			return Plan{}, validationf("sell order %d is terminal (%s); use correct_terminal_order_fill to record a discovered fill", ord.id, ord.state)
		}
		qty, err := validateFill(ctx, q, c, ord, req.Fill, "sell")
		if err != nil {
			return Plan{}, err
		}
		netAfter := net.Sub(qty)
		// Order state and cycle state are computed SEPARATELY (blocker 4): a sell order can fully
		// fill while the cycle still holds inventory (another sell), and the cycle can close while
		// this sell is only partially filled (a prior sell covered the rest).
		orderComplete := ord.filled.Add(qty).GreaterThanOrEqual(ord.quantity)
		otherActive, otherUnresolved := c.otherSells(ord.id)
		if req.Action == ActionMarkSellPartiallyFilled {
			if !netAfter.IsPositive() {
				// Would close the whole exposure — must not leave a zero-exposure cycle in
				// SELL_PARTIALLY_FILLED with the lock held (blocker 4).
				return Plan{}, validationf("this sell closes the entire remaining exposure (%s %s would remain) — use mark_sell_filled", netAfter, c.baseAsset)
			}
			// A sell in NEEDS_RECONCILE may still be open on the venue; the cycle must not move on
			// (which could queue ANOTHER sell) while it is unresolved (round-3 blocker 1). Keep the
			// cycle NEEDS_RECONCILE, lock held.
			if otherUnresolved {
				return Plan{}, validationf("another sell order for this cycle is in NEEDS_RECONCILE and may still execute — resolve it before continuing this sell (cycle stays NEEDS_RECONCILE, lock held)")
			}
			// Exposure REMAINS. If this fill COMPLETES the selected sell order (it becomes terminal)
			// and NO OTHER active sell exists, the sell manager would have nothing to continue and the
			// remainder would be stranded → move to SELL_REPRICE_PENDING so it creates the next sell
			// (blocker 2). If another live sell IS being managed, SELL_PARTIALLY_FILLED (no new sell).
			newOrder := state.OrderPartiallyFilled
			newCycle := state.CycleSellPartiallyFilled
			if orderComplete {
				newOrder = state.OrderFilled
				if !otherActive {
					newCycle = state.CycleSellRepricePending
				}
			}
			r.planFill(ord, qty, req.Fill, newOrder, &p)
			p.NewCycleState = string(newCycle)
			expectedNet = netAfter
		} else { // mark_sell_filled — a full exit that closes the cycle and releases the lock
			if netAfter.IsPositive() {
				return Plan{}, validationf("sell does not fully exit (%s %s would remain) — use mark_sell_partially_filled", netAfter, c.baseAsset)
			}
			// Closing + releasing the lock requires that NO OTHER sell order in the cycle can still
			// execute — an ACKED/PARTIALLY_FILLED/NEEDS_RECONCILE sell may fill later and cause
			// oversell/negative exposure even after recorded exposure hits zero (round-3 blocker 1).
			if otherActive || otherUnresolved {
				return Plan{}, validationf("another sell order for this cycle may still execute (active or NEEDS_RECONCILE) — resolve every other sell before closing (cycle stays NEEDS_RECONCILE, lock held)")
			}
			// The SELECTED sell must also be non-executable (blocker 1): a partially-filled sell order
			// may still have an open remainder. Require the order to be fully filled, or the operator
			// to confirm its remainder is cancelled.
			newOrder := state.OrderFilled
			if !orderComplete {
				if !req.ExternalResolutionConfirmed {
					return Plan{}, validationf("sell order %d fills %s of %s — its unfilled remainder may still be open on the venue; closing the cycle would risk oversell. Confirm the remainder is cancelled (external_resolution_confirmed=true + external_resolution_reason) or record its full fill.", ord.id, ord.filled.Add(qty), ord.quantity)
				}
				if strings.TrimSpace(req.ExternalResolutionReason) == "" {
					return Plan{}, validationf("external_resolution_reason is required to close with a partially-filled sell order (confirming its remainder is cancelled)")
				}
				// Remainder confirmed cancelled → the order is terminal CANCELLED (its recorded partial
				// fill is preserved), never an active PARTIALLY_FILLED.
				newOrder = state.OrderCancelled
				p.ExternalConfirmed = true
				p.Warnings = append(p.Warnings, "closed with a partially-filled sell whose remainder the operator confirmed cancelled externally")
			}
			r.planFill(ord, qty, req.Fill, newOrder, &p)
			p.NewCycleState = string(state.CycleClosed)
			p.LockReleased, p.LockChange = true, "released"
			expectedNet = decimal.Zero
		}

	case ActionMarkOrderCancelledZeroFill:
		if req.OrderID == 0 {
			return Plan{}, validationf("order_id is required for this action")
		}
		ord, err := c.targetOrder(req, "")
		if err != nil {
			return Plan{}, err
		}
		if err := requireNoActiveRequests(active, ord.id, "cancel this order"); err != nil {
			return Plan{}, err
		}
		if state.IsTerminalOrder(ord.state) {
			return Plan{}, validationf("order %d is already terminal (%s)", ord.id, ord.state)
		}
		if ord.filled.IsPositive() {
			return Plan{}, validationf("order %d has %s filled; cannot mark zero-fill cancelled", ord.id, ord.filled)
		}
		p.OrderID = ord.id
		p.OldOrderState, p.NewOrderState = string(ord.state), string(state.OrderCancelled)
		p.OrderChanges = append(p.OrderChanges, orderChange(*ord, state.OrderCancelled))
		// cycle stays NEEDS_RECONCILE; lock held (operator chooses a cycle action next).

	case ActionCorrectTerminalOrderFill:
		if req.OrderID == 0 {
			return Plan{}, validationf("order_id is required to correct a terminal order")
		}
		ord, err := c.targetOrder(req, "")
		if err != nil {
			return Plan{}, err
		}
		// correct_terminal_order_fill resolves the WHOLE cycle out of NEEDS_RECONCILE (it may close,
		// release the lock, or advance the cycle), so an active request on ANY order in the cycle must
		// block it — not just requests on the selected order (round-3 blocker 2). Scope 0 = whole cycle.
		if err := requireNoActiveRequests(active, 0, "correct this order and advance the cycle"); err != nil {
			return Plan{}, err
		}
		if !state.IsTerminalCorrectable(ord.state) {
			return Plan{}, validationf("order %d is %s; correct_terminal_order_fill applies only to a terminal (CANCELLED/FAILED/REJECTED/EXPIRED) order", ord.id, ord.state)
		}
		side := "buy"
		if ord.role == "exit_sell" {
			side = "sell"
		}
		qty, err := validateFill(ctx, q, c, ord, req.Fill, side)
		if err != nil {
			return Plan{}, err
		}
		full := ord.filled.Add(qty).GreaterThanOrEqual(ord.quantity)
		// Record the discovered fill. The order is ALREADY terminal, so its remainder is
		// non-executable. A FULL discovered fill means the order actually filled completely (the
		// cancel/rejection never took) → represent it as FILLED. A PARTIAL discovered fill LEAVES the
		// order terminal — its remainder stays cancelled and it is never reactivated to an active
		// PARTIALLY_FILLED (blocker 3/5). Only the cycle advances, so the operator is never asked to
		// insert the same fill twice.
		p.OrderID = ord.id
		p.RecordsFill = true
		p.FillToInsert = req.Fill
		p.Accounting = accountingFor(ord, qty, req.Fill)
		if full {
			p.OldOrderState, p.NewOrderState = string(ord.state), string(state.OrderFilled)
			p.OrderChanges = append(p.OrderChanges, orderChange(*ord, state.OrderFilled))
		} else {
			p.OldOrderState, p.NewOrderState = string(ord.state), string(ord.state) // stays terminal
		}
		if side == "buy" {
			expectedNet = net.Add(qty)
			if full {
				p.NewCycleState = string(state.CycleBuyFilled)
			} else {
				p.NewCycleState = string(state.CycleBuyPartiallyFilled)
			}
			// lock held — the cycle can now progress to sell management.
		} else { // discovered sell fill
			netAfter := net.Sub(qty)
			expectedNet = netAfter
			// EVERY other sell order in the cycle must be considered before closing or queuing a
			// replacement sell (round-3 blocker 1): another active or NEEDS_RECONCILE sell may still
			// execute at the venue. This selected order is already terminal (correction only records
			// the fill; its own remainder is not executable).
			otherActive, otherUnresolved := c.otherSells(ord.id)
			switch {
			case otherUnresolved:
				// An unresolved sell may still be open — do not close or queue another sell. Keep the
				// cycle NEEDS_RECONCILE, lock held, until that sell is resolved.
				return Plan{}, validationf("another sell order for this cycle is in NEEDS_RECONCILE and may still execute — resolve it before recording this terminal fill (cycle stays NEEDS_RECONCILE, lock held)")
			case !netAfter.IsPositive():
				// Exposure closed AND no other sell can still execute → safe to close + release.
				if otherActive {
					return Plan{}, validationf("another active sell order for this cycle may still execute — resolve it before closing (cycle stays NEEDS_RECONCILE, lock held)")
				}
				p.NewCycleState = string(state.CycleClosed)
				p.LockReleased, p.LockChange = true, "released"
				expectedNet = decimal.Zero
			case otherActive:
				p.NewCycleState = string(state.CycleSellPartiallyFilled)
			default:
				p.NewCycleState = string(state.CycleSellRepricePending) // deterministic next-sell path
			}
		}

	case ActionMarkFailed:
		if err := requireNoActiveRequests(active, 0, "mark the cycle FAILED"); err != nil {
			return Plan{}, err
		}
		// FAILED is terminal: a FAILED cycle with unresolved exposure can hide risk. Move OUT of
		// NEEDS_RECONCILE only when exposure is proven zero, or the operator explicitly confirms an
		// external resolution.
		switch {
		case class == ExposureProvenZero:
			p.NewCycleState = string(state.CycleFailed)
			p.LockReleased, p.LockChange = true, "released"
			p.Warnings = append(p.Warnings, "proven zero exposure — prefer cancel_zero_exposure (FAILED is terminal and can hide risk)")
			expectedNet = decimal.Zero
		case !req.ExternalResolutionConfirmed:
			// Open/unknown exposure, no explicit confirmation: REFUSE to fail. Keep the cycle in
			// NEEDS_RECONCILE with the lock held (a warning is not enough).
			p.ExposureUnresolved = true
			p.Downgraded = true
			p.Warnings = append(p.Warnings, fmt.Sprintf(
				"%s exposure of %s %s — NOT marking FAILED; kept in NEEDS_RECONCILE with the lock held. To force, set external_resolution_confirmed=true with an external_resolution_reason confirming the exposure was handled outside the system.",
				class, net, c.baseAsset))
		default:
			if strings.TrimSpace(req.ExternalResolutionReason) == "" {
				return Plan{}, validationf("external_resolution_reason is required to force mark_failed with %s exposure", class)
			}
			p.NewCycleState = string(state.CycleFailed)
			p.LockReleased, p.LockChange = true, "released"
			p.ExposureUnresolved = true
			p.ExternalConfirmed = true
			p.Warnings = append(p.Warnings, fmt.Sprintf(
				"FAILED forced with externally-handled %s exposure of %s %s (operator confirmed) — lock released", class, net, c.baseAsset))
			expectedNet = decimal.Zero
		}

	default:
		return Plan{}, validationf("unknown action %q", req.Action)
	}

	if w := r.balanceWarning(ctx, q, c, expectedNet); w != "" {
		p.Warnings = append(p.Warnings, w)
	}
	p.ExposureAfter = expectedNet.String()
	p.StateHash = r.fingerprint(c, active, req)
	return p, nil
}

// planAttach validates an attach_exchange_order_id request (blocker 8: idempotent / conflict /
// uniqueness / version CAS) and records the (no-op or update) plan.
func (r *Resolver) planAttach(ctx context.Context, q queryer, c cycleCtx, req Request, p *Plan) error {
	newID := strings.TrimSpace(req.ExchangeOrderID)
	if newID == "" {
		return validationf("exchange_order_id is required")
	}
	ord, err := c.targetOrder(req, "entry_buy")
	if err != nil {
		return err
	}
	p.OrderID = ord.id
	p.OldOrderState, p.NewOrderState = string(ord.state), string(ord.state) // no state change
	existing := strings.TrimSpace(ord.exchangeOrderID)
	switch {
	case existing == newID:
		p.Warnings = append(p.Warnings, "exchange_order_id already attached to this order — no change (idempotent)")
		return nil
	case existing != "":
		return validationf("order %d already has exchange_order_id %q; refusing to overwrite with %q", ord.id, existing, newID)
	}
	// Uniqueness: the id must not already belong to ANOTHER order on the same exchange.
	var other int64
	err = q.QueryRowContext(ctx,
		"SELECT id FROM orders WHERE exchange_id=? AND exchange_order_id=? AND id<>? LIMIT 1",
		c.exchangeID, newID, ord.id).Scan(&other)
	switch {
	case err == nil:
		return validationf("exchange_order_id %q is already attached to order %d on this exchange", newID, other)
	case errors.Is(err, sql.ErrNoRows):
		// unique — ok
	default:
		return err
	}
	return nil
}

// planFill records the fill + accounting into the plan and sets the primary order transition.
// accountingFor computes the cumulative accounting a recorded fill will write (no side effects).
func accountingFor(ord *orderRow, qty decimal.Decimal, f *FillData) *AccountingChange {
	price, _ := parseDec(f.Price)
	quote := qty.Mul(price)
	newFilled := ord.filled.Add(qty)
	newQuote := ord.quoteSpent.Add(quote)
	avg := decimal.Zero
	if newFilled.IsPositive() {
		avg = newQuote.DivRound(newFilled, 12)
	}
	return &AccountingChange{
		OrderID: ord.id, AddQuantity: qty.String(), NewFilled: newFilled.String(),
		NewQuoteSpent: newQuote.String(), NewAvgPrice: avg.String(),
	}
}

// planFill records the fill + accounting AND the primary order transition into the plan.
func (r *Resolver) planFill(ord *orderRow, qty decimal.Decimal, f *FillData, newOrder state.OrderState, p *Plan) {
	p.OrderID = ord.id
	p.OldOrderState, p.NewOrderState = string(ord.state), string(newOrder)
	p.RecordsFill = true
	p.FillToInsert = f
	p.Accounting = accountingFor(ord, qty, f)
	p.OrderChanges = append(p.OrderChanges, orderChange(*ord, newOrder))
}

func orderChange(o orderRow, to state.OrderState) OrderChange {
	return OrderChange{OrderID: o.id, Role: o.role, OldState: string(o.state), NewState: string(to)}
}

// isDuplicateKey reports whether err is a MySQL/MariaDB duplicate-key violation (error 1062) — the
// unique (exchange_id, exchange_order_id) index rejecting a concurrent attach.
func isDuplicateKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// otherSells classifies every exit_sell order OTHER than excludeID that is not terminal (PR21
// round-3). `active` = a live sell the manager is still working (SUBMITTED/ACKED/PARTIALLY_FILLED/…,
// i.e. non-terminal AND non-reconcile). `unresolved` = a sell in NEEDS_RECONCILE, which MAY still be
// open and executable at the venue and must be resolved before the cycle can safely close, release
// its lock, or queue a replacement sell — it is never treated as harmless. Any other non-terminal
// sell (`active || unresolved`) means the cycle still has a sell that could execute.
func (c cycleCtx) otherSells(excludeID int64) (active, unresolved bool) {
	for _, s := range c.sells {
		if s.id == excludeID {
			continue
		}
		switch {
		case state.IsTerminalOrder(s.state):
			// terminal — cannot execute
		case s.state == state.OrderNeedsReconcile:
			unresolved = true
		default:
			active = true
		}
	}
	return active, unresolved
}

// requireNoActiveRequests refuses a resolution while an active exchange request can still execute
// (blocker 2). scopeOrderID==0 means "any request related to the cycle blocks"; a non-zero id means
// "only a request for THAT order blocks" (used by single-order actions). It also flags each active
// request as blocking in the plan via the caller's RequestChanges slice.
func requireNoActiveRequests(active []RequestChange, scopeOrderID int64, what string) error {
	var blocking []int64
	for i := range active {
		if scopeOrderID == 0 || active[i].OrderID == scopeOrderID {
			active[i].Blocking = true
			blocking = append(blocking, active[i].RequestID)
		}
	}
	if len(blocking) > 0 {
		return validationf("cannot %s: %d active exchange request(s) %v can still be claimed/sent/retried — let recovery finish (they will finalize) before resolving", what, len(blocking), blocking)
	}
	return nil
}

// requireProvenZeroOrConfirmed gates a lock-releasing zero-exposure action (cancel_zero_exposure /
// mark_buy_zero_filled): OPEN exposure is refused outright; UNKNOWN is refused unless the operator
// supplies an external resolution; PROVEN_ZERO proceeds (blocker 1).
func requireProvenZeroOrConfirmed(class Exposure, net decimal.Decimal, base string, req Request, p *Plan, verb string) error {
	switch class {
	case ExposureProvenZero:
		return nil
	case ExposureOpen:
		return validationf("cannot %s: open exposure of %s %s (record the fill or sell first)", verb, net, base)
	case ExposureUnknown:
		if !req.ExternalResolutionConfirmed {
			return validationf("cannot %s: exposure is UNKNOWN (an order may have executed on the venue with an unconfirmed outcome — a recorded 0 is not proof). Confirm the exchange shows no position, then set external_resolution_confirmed=true with an external_resolution_reason.", verb)
		}
		if strings.TrimSpace(req.ExternalResolutionReason) == "" {
			return validationf("external_resolution_reason is required to %s with UNKNOWN exposure", verb)
		}
		p.ExposureUnresolved = true
		p.ExternalConfirmed = true
		p.Warnings = append(p.Warnings, fmt.Sprintf("%s with UNKNOWN exposure — operator confirmed externally: exposure handled outside the system", verb))
		return nil
	default:
		return validationf("cannot %s: exposure classification %s", verb, class)
	}
}

// execute mutates the DB per the (already-validated, fingerprint-checked) plan and returns whether
// the symbol lock was released. All state changes go through internal/state; no exchange is touched.
func (r *Resolver) execute(ctx context.Context, tx *sql.Tx, c cycleCtx, req Request, plan Plan) (bool, error) {
	switch req.Action {
	case ActionKeepNeedsReconcile:
		return false, nil

	case ActionAttachExchangeOrderID:
		ord := c.mustOrder(plan.OrderID)
		newID := strings.TrimSpace(req.ExchangeOrderID)
		if strings.TrimSpace(ord.exchangeOrderID) == newID {
			return false, nil // idempotent no-op (already attached)
		}
		// Serialize attaches per exchange (blocker 4): lock the authoritative exchange row so two
		// concurrent attaches of the same venue id can't both pass the uniqueness check. Combined with
		// the UNIQUE(exchange_id, exchange_order_id) index (migration 035), exactly one can win.
		var lockedEx int64
		if err := tx.QueryRowContext(ctx, "SELECT id FROM exchanges WHERE id=? FOR UPDATE", c.exchangeID).Scan(&lockedEx); err != nil {
			return false, err
		}
		// Re-check uniqueness UNDER the exchange lock (the preview-time check is not authoritative).
		var other int64
		switch err := tx.QueryRowContext(ctx,
			"SELECT id FROM orders WHERE exchange_id=? AND exchange_order_id=? AND id<>? LIMIT 1",
			c.exchangeID, newID, ord.id).Scan(&other); {
		case err == nil:
			return false, validationf("exchange_order_id %q is already attached to order %d on this exchange", newID, other)
		case errors.Is(err, sql.ErrNoRows):
			// unique — proceed
		default:
			return false, err
		}
		// Version-CAS update (blocker 8): only when still empty and at the observed version.
		res, err := tx.ExecContext(ctx,
			"UPDATE orders SET exchange_order_id=?, version=version+1, updated_at=NOW(6) WHERE id=? AND version=? AND (exchange_order_id IS NULL OR exchange_order_id='')",
			newID, ord.id, ord.version)
		if err != nil {
			if isDuplicateKey(err) { // the unique index caught a race the lock somehow missed
				return false, conflictf("exchange_order_id %q was concurrently attached elsewhere — re-preview", newID)
			}
			return false, err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return false, conflictf("exchange_order_id attach lost a race on order %d — re-preview", ord.id)
		}
		return false, nil

	case ActionCancelZeroExposure:
		for _, o := range c.allOrders() {
			if err := r.resolveOrder(ctx, tx, o, state.OrderCancelled, req.Reason); err != nil {
				return false, err
			}
		}
		if err := r.resolveCycle(ctx, tx, c, state.CycleCancelled, req.Reason); err != nil {
			return false, err
		}
		return r.releaseLock(ctx, tx, c.cycleID)

	case ActionMarkBuyZeroFilled:
		if c.buy != nil {
			if err := r.resolveOrder(ctx, tx, *c.buy, state.OrderCancelled, req.Reason); err != nil {
				return false, err
			}
		}
		if err := r.resolveCycle(ctx, tx, c, state.CycleCancelled, req.Reason); err != nil {
			return false, err
		}
		return r.releaseLock(ctx, tx, c.cycleID)

	case ActionMarkBuyFilled, ActionMarkBuyPartiallyFilled:
		ord := c.mustOrder(plan.OrderID)
		if err := r.recordFill(ctx, tx, c, ord, req.Fill); err != nil {
			return false, err
		}
		if err := r.resolveOrder(ctx, tx, *ord, state.OrderState(plan.NewOrderState), req.Reason); err != nil {
			return false, err
		}
		return false, r.resolveCycle(ctx, tx, c, state.CycleState(plan.NewCycleState), req.Reason)

	case ActionMarkSellPartiallyFilled:
		ord := c.mustOrder(plan.OrderID)
		if err := r.recordFill(ctx, tx, c, ord, req.Fill); err != nil {
			return false, err
		}
		if err := r.resolveOrder(ctx, tx, *ord, state.OrderState(plan.NewOrderState), req.Reason); err != nil {
			return false, err
		}
		// The cycle may be SELL_PARTIALLY_FILLED (another sell is live) or SELL_REPRICE_PENDING (the
		// selected sell completed and the manager must create the next sell for the remainder).
		return false, r.resolveCycle(ctx, tx, c, state.CycleState(plan.NewCycleState), req.Reason)

	case ActionMarkSellFilled:
		ord := c.mustOrder(plan.OrderID)
		if err := r.recordFill(ctx, tx, c, ord, req.Fill); err != nil {
			return false, err
		}
		if err := r.resolveOrder(ctx, tx, *ord, state.OrderState(plan.NewOrderState), req.Reason); err != nil {
			return false, err
		}
		// Close with PnL accounting + lock release (reads the just-recorded sell fill).
		return orders.ResolveCloseFromReconcile(ctx, tx, c.cycleID, req.Reason)

	case ActionMarkOrderCancelledZeroFill:
		ord := c.mustOrder(plan.OrderID)
		return false, r.resolveOrder(ctx, tx, *ord, state.OrderCancelled, req.Reason)

	case ActionCorrectTerminalOrderFill:
		ord := c.mustOrder(plan.OrderID)
		if err := r.recordFill(ctx, tx, c, ord, req.Fill); err != nil {
			return false, err
		}
		// A FULL discovered fill re-opens the terminal order to FILLED via the explicit, audited
		// terminal-correction path; a PARTIAL discovered fill leaves the order terminal (its
		// remainder stays cancelled — never reactivated). recordFill already updated filled_quantity.
		if plan.NewOrderState == string(state.OrderFilled) && plan.NewOrderState != plan.OldOrderState {
			if _, err := state.CorrectTerminalOrder(ctx, tx, state.OrderTransition{
				OrderID: ord.id, From: ord.state, To: state.OrderFilled,
				Version: ord.version, EventType: "operator_terminal_correction", Reason: req.Reason,
			}); err != nil {
				return false, err
			}
		}
		// Advance the cycle so it is not left in a dead end (blocker 3).
		if plan.NewCycleState == string(state.CycleClosed) {
			return orders.ResolveCloseFromReconcile(ctx, tx, c.cycleID, req.Reason)
		}
		if plan.NewCycleState != "" && plan.NewCycleState != string(c.cycleState) {
			return false, r.resolveCycle(ctx, tx, c, state.CycleState(plan.NewCycleState), req.Reason)
		}
		return false, nil

	case ActionMarkFailed:
		if plan.Downgraded {
			return false, nil // refused: kept NEEDS_RECONCILE, lock held, audit only
		}
		failReason := req.Reason
		if plan.ExternalConfirmed {
			failReason += " (external resolution confirmed)"
		}
		if err := r.resolveCycle(ctx, tx, c, state.CycleFailed, failReason); err != nil {
			return false, err
		}
		for _, o := range c.allOrders() {
			if !state.IsTerminalOrder(o.state) {
				if err := r.resolveOrder(ctx, tx, o, state.OrderFailed, failReason); err != nil {
					return false, err
				}
			}
		}
		if plan.LockReleased {
			return r.releaseLock(ctx, tx, c.cycleID)
		}
		return false, nil
	}
	return false, validationf("unknown action %q", req.Action)
}

// ---- fill validation + recording ----

// validateFill checks operator-supplied fill data and returns the parsed quantity. It validates
// side/quantity/price/fee/fee-asset, prevents oversell, requires a fill id, and rejects a duplicate
// fill id (the unique (order_id, exchange_fill_id) key).
func validateFill(ctx context.Context, q queryer, c cycleCtx, ord *orderRow, f *FillData, side string) (decimal.Decimal, error) {
	if f == nil {
		return decimal.Zero, validationf("fill data is required for this action")
	}
	// The fill side must match the PERSISTED order role (blocker 3), not just the request.
	if side == "buy" && (ord.role != "entry_buy" || ord.side != "buy") {
		return decimal.Zero, validationf("order %d is not a buy order (role %s, side %s)", ord.id, ord.role, ord.side)
	}
	if side == "sell" && (ord.role != "exit_sell" || ord.side != "sell") {
		return decimal.Zero, validationf("order %d is not a sell order (role %s, side %s)", ord.id, ord.role, ord.side)
	}
	if !strings.EqualFold(strings.TrimSpace(f.Side), side) {
		return decimal.Zero, validationf("fill side %q does not match the %s order", f.Side, side)
	}
	fillID := strings.TrimSpace(f.ExchangeFillID)
	if fillID == "" {
		return decimal.Zero, validationf("a fill id is required (used for duplicate detection)")
	}
	qty, ok := parseDec(f.Quantity)
	if !ok || !qty.IsPositive() {
		return decimal.Zero, validationf("fill quantity must be a positive number")
	}
	price, ok := parseDec(f.Price)
	if !ok || !price.IsPositive() {
		return decimal.Zero, validationf("fill price must be a positive number")
	}
	fee := decimal.Zero
	if strings.TrimSpace(f.Fee) != "" {
		fee, ok = parseDec(f.Fee)
		if !ok || fee.IsNegative() {
			return decimal.Zero, validationf("fee must be a non-negative number")
		}
	}
	if fee.IsPositive() && strings.TrimSpace(f.FeeAsset) == "" {
		return decimal.Zero, validationf("fee_asset is required when a fee is supplied")
	}
	// Oversell guards.
	if side == "buy" {
		if ord.filled.Add(qty).GreaterThan(ord.quantity) {
			return decimal.Zero, validationf("buy oversell: filled %s + %s exceeds ordered %s", ord.filled, qty, ord.quantity)
		}
	} else {
		if c.sellFilled().Add(qty).GreaterThan(c.buyFilled()) {
			return decimal.Zero, validationf("sell oversell: total sold %s would exceed bought %s", c.sellFilled().Add(qty), c.buyFilled())
		}
	}
	// Duplicate fill id.
	var n int
	_ = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM fills WHERE order_id=? AND exchange_fill_id=?", ord.id, fillID).Scan(&n)
	if n > 0 {
		return decimal.Zero, validationf("duplicate fill id %q for order %d", fillID, ord.id)
	}
	return qty, nil
}

// recordFill inserts the fills row and updates the order's cumulative fields. Validation has
// already run (in buildPlan); the unique key is a final backstop against duplicates.
func (r *Resolver) recordFill(ctx context.Context, tx *sql.Tx, c cycleCtx, ord *orderRow, f *FillData) error {
	qty, _ := parseDec(f.Quantity)
	price, _ := parseDec(f.Price)
	fee := decimal.Zero
	if strings.TrimSpace(f.Fee) != "" {
		fee, _ = parseDec(f.Fee)
	}
	quote := qty.Mul(price)
	fillID := strings.TrimSpace(f.ExchangeFillID)
	feeAsset := nullIfEmpty(strings.TrimSpace(f.FeeAsset))

	if _, err := tx.ExecContext(ctx, `
INSERT INTO fills (order_id, cycle_id, exchange_fill_id, quantity, price, quote_amount, fee_amount, fee_asset, filled_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(6))`,
		ord.id, c.cycleID, fillID, qty.String(), price.String(), quote.String(), fee.String(), feeAsset); err != nil {
		return err
	}
	newFilled := ord.filled.Add(qty)
	newQuote := ord.quoteSpent.Add(quote)
	avg := decimal.Zero
	if newFilled.IsPositive() {
		avg = newQuote.DivRound(newFilled, 12)
	}
	_, err := tx.ExecContext(ctx, `
UPDATE orders SET filled_quantity=?, avg_fill_price=?, quote_spent=?,
  fee_amount=COALESCE(fee_amount,0)+?, fee_asset=COALESCE(?, fee_asset) WHERE id=?`,
		newFilled.String(), avg.String(), newQuote.String(), fee.String(), feeAsset, ord.id)
	return err
}

// ---- state-machine + lock helpers (all changes go through internal/state) ----

// resolveOrder moves an order to a resolution target. If it is not yet NEEDS_RECONCILE (and not
// terminal) it is first transitioned there via the normal map, then resolved — so every change is
// a validated state-machine transition.
func (r *Resolver) resolveOrder(ctx context.Context, tx *sql.Tx, o orderRow, to state.OrderState, reason string) error {
	if state.IsTerminalOrder(o.state) {
		return nil // already terminal; leave it (terminal corrections use CorrectTerminalOrder)
	}
	cur, ver := o.state, o.version
	if cur != state.OrderNeedsReconcile {
		res, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{
			OrderID: o.id, From: cur, To: state.OrderNeedsReconcile, Version: ver,
			EventType: "needs_reconcile", Reason: "operator pre-resolution",
		})
		if err != nil {
			return err
		}
		cur, ver = state.OrderNeedsReconcile, res.NewVersion
	}
	_, err := state.ApplyOrderResolution(ctx, tx, state.OrderTransition{
		OrderID: o.id, From: cur, To: to, Version: ver, EventType: "operator_resolution", Reason: reason,
	})
	return err
}

func (r *Resolver) resolveCycle(ctx context.Context, tx *sql.Tx, c cycleCtx, to state.CycleState, reason string) error {
	_, err := state.ApplyCycleResolution(ctx, tx, state.CycleTransition{
		CycleID: c.cycleID, From: c.cycleState, To: to, Version: c.cycleVersion,
		EventType: "operator_resolution", Reason: reason,
	})
	return err
}

func (r *Resolver) releaseLock(ctx context.Context, tx *sql.Tx, cycleID int64) (bool, error) {
	lock, ok, err := symbollock.ActiveByCycle(ctx, tx, cycleID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := symbollock.Release(ctx, tx, lock.ID); err != nil {
		return false, err
	}
	return true, nil
}

// ---- target order selection + balance cross-check ----

// targetOrder resolves the order the action applies to. When order_id is supplied it MUST belong
// to this cycle (cross-cycle orders are rejected — the loaded set is this cycle's only) AND, when a
// role is required, its persisted role must match (blocker 3). When order_id is omitted the order
// is derived from the required role.
func (c cycleCtx) targetOrder(req Request, wantRole string) (*orderRow, error) {
	if req.OrderID != 0 {
		var found *orderRow
		if c.buy != nil && c.buy.id == req.OrderID {
			found = c.buy
		}
		for i := range c.sells {
			if c.sells[i].id == req.OrderID {
				found = &c.sells[i]
			}
		}
		if found == nil {
			return nil, validationf("order %d does not belong to cycle %d", req.OrderID, c.cycleID)
		}
		if wantRole != "" && found.role != wantRole {
			return nil, validationf("order %d has role %s, not %s — this action cannot be applied to it", found.id, found.role, wantRole)
		}
		return found, nil
	}
	switch wantRole {
	case "entry_buy":
		if c.buy == nil {
			return nil, validationf("cycle %d has no buy order", c.cycleID)
		}
		return c.buy, nil
	case "exit_sell":
		if len(c.sells) == 0 {
			return nil, validationf("cycle %d has no sell order", c.cycleID)
		}
		for i := len(c.sells) - 1; i >= 0; i-- {
			if !state.IsTerminalOrder(c.sells[i].state) {
				return &c.sells[i], nil
			}
		}
		return &c.sells[len(c.sells)-1], nil
	default:
		return nil, validationf("order_id is required for this action")
	}
}

func (c cycleCtx) allOrders() []orderRow {
	var out []orderRow
	if c.buy != nil {
		out = append(out, *c.buy)
	}
	out = append(out, c.sells...)
	return out
}

// mustOrder returns the loaded order by id (present because buildPlan resolved it). Falls back to
// a zero-value pointer only if not found (never expected post-validation).
func (c cycleCtx) mustOrder(id int64) *orderRow {
	if c.buy != nil && c.buy.id == id {
		return c.buy
	}
	for i := range c.sells {
		if c.sells[i].id == id {
			return &c.sells[i]
		}
	}
	return &orderRow{id: id}
}

// balanceWarning returns an advisory message (never blocks) when the latest current balance for
// the cycle's base asset materially disagrees with the exposure the resolution implies.
func (r *Resolver) balanceWarning(ctx context.Context, q queryer, c cycleCtx, expectedNet decimal.Decimal) string {
	var total string
	if err := q.QueryRowContext(ctx,
		"SELECT total FROM wallet_balances_current WHERE exchange_id=? AND asset=?",
		c.exchangeID, c.baseAsset).Scan(&total); err != nil {
		return "" // no balance snapshot -> no cross-check
	}
	bal := decOrZero(total)
	diff := bal.Sub(expectedNet).Abs()
	ref := decimal.Max(bal.Abs(), expectedNet.Abs())
	tol := ref.Mul(decimal.NewFromFloat(0.01)) // 1% tolerance
	floor := decimal.NewFromFloat(1e-8)
	if diff.GreaterThan(tol) && diff.GreaterThan(floor) {
		return fmt.Sprintf("balance cross-check: exchange shows %s %s but this resolution implies ~%s %s held (advisory only — not blocked)", bal, c.baseAsset, expectedNet, c.baseAsset)
	}
	return ""
}
