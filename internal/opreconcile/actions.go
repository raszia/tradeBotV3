package opreconcile

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/state"
	"v3TradeBot/internal/symbollock"
)

// buildPlan validates the request against the loaded cycle and computes the exact
// proposed state changes (used by BOTH Preview — read-only — and Apply — re-validated
// in-tx). A bad request returns a ValidationError; nothing is mutated here.
func (r *Resolver) buildPlan(ctx context.Context, q queryer, c cycleCtx, req Request) (Plan, error) {
	if c.cycleState != state.CycleNeedsReconcile {
		return Plan{}, validationf("cycle %d is %s, not NEEDS_RECONCILE", c.cycleID, c.cycleState)
	}
	net := c.netExposure()
	p := Plan{
		CycleID: c.cycleID, Action: req.Action,
		OldCycleState: string(c.cycleState), NewCycleState: string(c.cycleState),
		NetExposure: net.String(), RequiresReason: true,
	}
	expectedNet := net // base inventory expected to remain AFTER the resolution

	switch req.Action {
	case ActionKeepNeedsReconcile:
		// no change; audit only.

	case ActionAttachExchangeOrderID:
		if strings.TrimSpace(req.ExchangeOrderID) == "" {
			return Plan{}, validationf("exchange_order_id is required")
		}
		ord, err := c.targetOrder(req, "entry_buy")
		if err != nil {
			return Plan{}, err
		}
		p.OrderID = ord.id
		p.OldOrderState, p.NewOrderState = string(ord.state), string(ord.state)

	case ActionCancelZeroExposure:
		if net.IsPositive() {
			return Plan{}, validationf("cannot cancel: open exposure of %s %s (record fills or sell first)", net, c.baseAsset)
		}
		p.NewCycleState = string(state.CycleCancelled)
		p.LockReleased = true
		expectedNet = decimal.Zero

	case ActionMarkBuyZeroFilled:
		if c.buyFilled().IsPositive() {
			return Plan{}, validationf("buy already has %s filled; cannot mark zero-filled", c.buyFilled())
		}
		if c.buy != nil {
			p.OrderID = c.buy.id
			p.OldOrderState, p.NewOrderState = string(c.buy.state), string(state.OrderCancelled)
		}
		p.NewCycleState = string(state.CycleCancelled)
		p.LockReleased = true
		expectedNet = decimal.Zero

	case ActionMarkBuyFilled:
		ord, err := c.targetOrder(req, "entry_buy")
		if err != nil {
			return Plan{}, err
		}
		qty, err := validateFill(ctx, q, c, ord, req.Fill, "buy")
		if err != nil {
			return Plan{}, err
		}
		p.OrderID = ord.id
		p.OldOrderState, p.NewOrderState = string(ord.state), string(state.OrderFilled)
		p.NewCycleState = string(state.CycleBuyFilled)
		p.RecordsFill = true
		expectedNet = net.Add(qty) // inventory increases; lock stays held

	case ActionMarkSellPartiallyFilled:
		ord, err := c.targetOrder(req, "exit_sell")
		if err != nil {
			return Plan{}, err
		}
		qty, err := validateFill(ctx, q, c, ord, req.Fill, "sell")
		if err != nil {
			return Plan{}, err
		}
		p.OrderID = ord.id
		p.OldOrderState, p.NewOrderState = string(ord.state), string(state.OrderPartiallyFilled)
		p.NewCycleState = string(state.CycleSellPartiallyFilled)
		p.RecordsFill = true
		expectedNet = net.Sub(qty) // still holding the remainder; lock held

	case ActionMarkSellFilled:
		ord, err := c.targetOrder(req, "exit_sell")
		if err != nil {
			return Plan{}, err
		}
		qty, err := validateFill(ctx, q, c, ord, req.Fill, "sell")
		if err != nil {
			return Plan{}, err
		}
		netAfter := net.Sub(qty)
		if netAfter.IsPositive() {
			return Plan{}, validationf("sell does not fully exit (%s %s would remain); use mark_sell_partially_filled", netAfter, c.baseAsset)
		}
		p.OrderID = ord.id
		p.OldOrderState, p.NewOrderState = string(ord.state), string(state.OrderFilled)
		p.NewCycleState = string(state.CycleClosed)
		p.RecordsFill = true
		p.LockReleased = true
		expectedNet = decimal.Zero

	case ActionMarkOrderCancelledZeroFill:
		if req.OrderID == 0 {
			return Plan{}, validationf("order_id is required for this action")
		}
		ord, err := c.targetOrder(req, "")
		if err != nil {
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
		// cycle stays NEEDS_RECONCILE; lock held (operator chooses a cycle action next).

	case ActionMarkFailed:
		p.NewCycleState = string(state.CycleFailed)
		p.LockReleased = !net.IsPositive()
		if net.IsPositive() {
			p.Warnings = append(p.Warnings, fmt.Sprintf("marking FAILED with open exposure of %s %s — the symbol lock is KEPT (inventory not exited)", net, c.baseAsset))
		}

	default:
		return Plan{}, validationf("unknown action %q", req.Action)
	}

	if w := r.balanceWarning(ctx, q, c, expectedNet); w != "" {
		p.Warnings = append(p.Warnings, w)
	}
	return p, nil
}

// execute mutates the DB per the (already-validated) plan and returns whether the symbol
// lock was released. All state changes go through internal/state; no exchange is touched.
func (r *Resolver) execute(ctx context.Context, tx *sql.Tx, c cycleCtx, req Request, plan Plan) (bool, error) {
	switch req.Action {
	case ActionKeepNeedsReconcile:
		return false, nil

	case ActionAttachExchangeOrderID:
		_, err := tx.ExecContext(ctx, "UPDATE orders SET exchange_order_id=? WHERE id=?", strings.TrimSpace(req.ExchangeOrderID), plan.OrderID)
		return false, err

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

	case ActionMarkBuyFilled:
		ord, _ := c.targetOrder(req, "entry_buy")
		if err := r.recordFill(ctx, tx, c, ord, req.Fill); err != nil {
			return false, err
		}
		if err := r.resolveOrder(ctx, tx, *ord, state.OrderFilled, req.Reason); err != nil {
			return false, err
		}
		return false, r.resolveCycle(ctx, tx, c, state.CycleBuyFilled, req.Reason)

	case ActionMarkSellPartiallyFilled:
		ord, _ := c.targetOrder(req, "exit_sell")
		if err := r.recordFill(ctx, tx, c, ord, req.Fill); err != nil {
			return false, err
		}
		if err := r.resolveOrder(ctx, tx, *ord, state.OrderPartiallyFilled, req.Reason); err != nil {
			return false, err
		}
		return false, r.resolveCycle(ctx, tx, c, state.CycleSellPartiallyFilled, req.Reason)

	case ActionMarkSellFilled:
		ord, _ := c.targetOrder(req, "exit_sell")
		if err := r.recordFill(ctx, tx, c, ord, req.Fill); err != nil {
			return false, err
		}
		if err := r.resolveOrder(ctx, tx, *ord, state.OrderFilled, req.Reason); err != nil {
			return false, err
		}
		// Close with PnL accounting + lock release (reads the just-recorded sell fill).
		return orders.ResolveCloseFromReconcile(ctx, tx, c.cycleID, req.Reason)

	case ActionMarkOrderCancelledZeroFill:
		ord, _ := c.targetOrder(req, "")
		return false, r.resolveOrder(ctx, tx, *ord, state.OrderCancelled, req.Reason)

	case ActionMarkFailed:
		if err := r.resolveCycle(ctx, tx, c, state.CycleFailed, req.Reason); err != nil {
			return false, err
		}
		for _, o := range c.allOrders() {
			if !state.IsTerminalOrder(o.state) {
				if err := r.resolveOrder(ctx, tx, o, state.OrderFailed, req.Reason); err != nil {
					return false, err
				}
			}
		}
		// Lock released only when no exposure remains; never on the button alone.
		if !c.netExposure().IsPositive() {
			return r.releaseLock(ctx, tx, c.cycleID)
		}
		return false, nil
	}
	return false, validationf("unknown action %q", req.Action)
}

// ---- fill validation + recording ----

// validateFill checks operator-supplied fill data and returns the parsed quantity. It
// validates side/quantity/price/fee/fee-asset, prevents oversell, requires a fill id, and
// rejects a duplicate fill id (the unique (order_id, exchange_fill_id) key).
func validateFill(ctx context.Context, q queryer, c cycleCtx, ord *orderRow, f *FillData, side string) (decimal.Decimal, error) {
	if f == nil {
		return decimal.Zero, validationf("fill data is required for this action")
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

// recordFill inserts the fills row and updates the order's cumulative fields. Validation
// has already run (in buildPlan); the unique key is a final backstop against duplicates.
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

// resolveOrder moves an order to a resolution target. If it is not yet NEEDS_RECONCILE
// (and not terminal) it is first transitioned there via the normal map, then resolved —
// so every change is a validated state-machine transition.
func (r *Resolver) resolveOrder(ctx context.Context, tx *sql.Tx, o orderRow, to state.OrderState, reason string) error {
	if state.IsTerminalOrder(o.state) {
		return nil // already terminal; leave it
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
	return true, symbollock.Release(ctx, tx, lock.ID)
}

// ---- target order selection + balance cross-check ----

func (c cycleCtx) targetOrder(req Request, wantRole string) (*orderRow, error) {
	if req.OrderID != 0 {
		if c.buy != nil && c.buy.id == req.OrderID {
			return c.buy, nil
		}
		for i := range c.sells {
			if c.sells[i].id == req.OrderID {
				return &c.sells[i], nil
			}
		}
		return nil, validationf("order %d not found in cycle %d", req.OrderID, c.cycleID)
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

// balanceWarning returns an advisory message (never blocks) when the latest current
// balance for the cycle's base asset materially disagrees with the exposure the
// resolution implies. With no balance data it returns "".
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
