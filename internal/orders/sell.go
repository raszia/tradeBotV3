package orders

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
	"v3TradeBot/internal/symbollock"
)

// Sell follow-up purposes (carried in FollowupPayload.Purpose).
const (
	PurposeSellReprice = "sell_reprice" // a CANCEL of a resting sell, to reprice
	PurposeSellStatus  = "sell_status"  // a GET_ORDER poll of a resting/cancelled sell
)

// CloseReasonExited is recorded on the cycle when the exit sell fully completes.
const CloseReasonExited = "EXIT_SELL_FILLED"

// SellIntentPayload is the PLACE_ORDER payload for a resting exit sell. Unlike the
// buy (simulated IOC), a sell rests until filled or repriced — there is no auto
// cancel scheduled on the ack.
type SellIntentPayload struct {
	Side               string `json:"side"` // "sell"
	OrderType          string `json:"order_type"`
	Price              string `json:"price"`
	Quantity           string `json:"quantity"`
	LocalClientOrderID string `json:"local_client_order_id"`
	SellOffsetBps      int    `json:"sell_offset_bps"`
	BinanceRef         string `json:"binance_ref"`
	QuoteUnit          string `json:"quote_unit"`
	ConfigVersion      int64  `json:"config_version"`
}

// ParseSellIntent decodes a sell PLACE_ORDER payload.
func ParseSellIntent(raw json.RawMessage) (SellIntentPayload, error) {
	var p SellIntentPayload
	err := json.Unmarshal(raw, &p)
	return p, err
}

// Validate checks a sell intent is well-formed and SAFE to send (the sell analogue of
// BuyIntentPayload.Validate — PR10 #2): a malformed/zero-value sell must never reach the
// exchange. side=sell, order_type=limit, price>0, quantity>0, non-empty client id.
func (p SellIntentPayload) Validate() error {
	if !strings.EqualFold(p.Side, "sell") {
		return fmt.Errorf("sell intent: side=%q, want sell", p.Side)
	}
	if !strings.EqualFold(p.OrderType, "limit") {
		return fmt.Errorf("sell intent: order_type=%q, want limit", p.OrderType)
	}
	price, err := decimal.NewFromString(p.Price)
	if err != nil || !price.IsPositive() {
		return fmt.Errorf("sell intent: price %q is not a positive number", p.Price)
	}
	qty, err := decimal.NewFromString(p.Quantity)
	if err != nil || !qty.IsPositive() {
		return fmt.Errorf("sell intent: quantity %q is not a positive number", p.Quantity)
	}
	if strings.TrimSpace(p.LocalClientOrderID) == "" {
		return fmt.Errorf("sell intent: local_client_order_id is empty")
	}
	return nil
}

// OrderRequest builds the exchange PlaceOrder for the resting sell (limit, no TIF).
func (p SellIntentPayload) OrderRequest(symbol string) execution.OrderRequest {
	return execution.OrderRequest{
		ClientOrderID: p.LocalClientOrderID,
		Symbol:        symbol,
		Side:          "sell",
		Quantity:      decimalOrZero(p.Quantity),
		LimitPrice:    decimalOrZero(p.Price),
		OrderType:     p.OrderType,
		TimeInForce:   "",
	}
}

// NOTE: buy/sell PLACE_ORDER routing is done from the DB order role (executor.dispatchPlace),
// NOT from the payload's "side" text — a payload must never be trusted to pick the handler
// (PR10 #2). The former PayloadSide helper was removed to prevent that unsafe pattern.

// SellPlaceAckParams is the input to OnSellPlaceAck.
type SellPlaceAckParams struct {
	RequestID int64
	OrderID   int64
	CycleID   int64
	Ack       execution.OrderAck
	RawResp   json.RawMessage
}

// OnSellPlaceAck records a successful sell PLACE_ORDER: the request is SUCCEEDED, the
// exchange order id stamped, the order advanced QUEUED→SUBMITTED→ACKED, and the cycle
// SELL_REQUEST_QUEUED→SELL_SUBMITTED. NO cancel is scheduled — the sell rests until
// it fills or the manager reprices it.
func OnSellPlaceAck(ctx context.Context, tx *sql.Tx, q *queue.Queue, p SellPlaceAckParams) error {
	if err := q.MarkSucceeded(ctx, tx, p.RequestID, p.RawResp); err != nil {
		return err
	}
	if p.Ack.ExchangeOrderID != "" || p.Ack.ClientOrderID != "" {
		if _, err := tx.ExecContext(ctx,
			"UPDATE orders SET exchange_order_id = COALESCE(NULLIF(?,''), exchange_order_id), client_order_id_sent = COALESCE(NULLIF(?,''), client_order_id_sent) WHERE id = ?",
			p.Ack.ExchangeOrderID, p.Ack.ClientOrderID, p.OrderID); err != nil {
			return err
		}
	}
	if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderSubmitted, "sell_sent", "sell order sent"); err != nil {
		return err
	}
	// PR11 #4: the sell was placed but the ack carries no usable exchange_order_id. A resting
	// sell we cannot identify cannot be polled/repriced/cancelled — never schedule a blind
	// GetOrder("")/CancelOrder(""). Push order+cycle to NEEDS_RECONCILE and KEEP the lock
	// (inventory + an untrackable resting order); the reconciler determines the real state.
	if p.Ack.ExchangeOrderID == "" {
		return MarkNeedsReconcile(ctx, tx, p.OrderID, &p.CycleID,
			"sell ack without a usable exchange_order_id — cannot track/cancel; needs reconcile")
	}
	if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderAcked, "sell_ack", "sell order acknowledged"); err != nil {
		return err
	}
	return advanceCycleTo(ctx, tx, p.CycleID, state.CycleSellSubmitted, "sell_submitted", "sell resting on the book")
}

// OnSellPlaceRejected cleanly resolves a DEFINITELY-rejected SELL place. Unlike a buy
// rejection (OnPlaceRejected — no inventory, so cycle FAILED + lock released), a sell holds
// EXISTING inventory from the buy leg: the order was refused but we may still hold the asset,
// so the symbol lock must NOT be released and the cycle must NOT be FAILED. The request is
// FAILED and the order + cycle go to NEEDS_RECONCILE with the lock HELD; reconciliation decides
// the next action. No blind retry (the request is terminal; a new sell is the reconciler's call).
func OnSellPlaceRejected(ctx context.Context, tx *sql.Tx, q *queue.Queue, p PlaceRejectedParams) error {
	if err := q.MarkFailed(ctx, tx, p.RequestID, p.Cause); err != nil {
		return err
	}
	return MarkNeedsReconcile(ctx, tx, p.OrderID, &p.CycleID, p.Cause)
}

// SellCancelParams is the input to OnSellCancelResult.
type SellCancelParams struct {
	RequestID       int64
	OrderID         int64
	CycleID         int64
	ExchangeID      int64
	Symbol          string
	ExchangeOrderID string
	RawResp         json.RawMessage
	FinalCheckDelay time.Duration
}

// OnSellCancelResult records a successful (or definite) reprice CANCEL and schedules
// the final sell-status read. The order stays CANCEL_PENDING — the GET_ORDER decides
// the real outcome (the cancel may have raced a fill), so we never assume.
func OnSellCancelResult(ctx context.Context, tx *sql.Tx, q *queue.Queue, p SellCancelParams) error {
	if err := q.MarkSucceeded(ctx, tx, p.RequestID, p.RawResp); err != nil {
		return err
	}
	payload, _ := json.Marshal(FollowupPayload{ExchangeOrderID: p.ExchangeOrderID, Purpose: PurposeSellStatus, CycleID: p.CycleID})
	_, err := q.EnqueueScheduled(ctx, tx, queue.Request{
		ExchangeID: p.ExchangeID, Symbol: p.Symbol, CycleID: &p.CycleID, OrderID: &p.OrderID,
		Type: queue.TypeGetOrder, Priority: 30, Payload: payload,
		IdempotencyKey: fmt.Sprintf("sell-cancel-status:o%d", p.OrderID),
	}, p.FinalCheckDelay)
	if err != nil && err != queue.ErrDuplicateIdempotencyKey {
		return err
	}
	return nil
}

// SellStatusParams is the input to ProcessSellStatus.
type SellStatusParams struct {
	RequestID int64
	OrderID   int64
	CycleID   int64
	Scope     string
	Symbol    string
	Status    execution.OrderStatus
	StatusErr error
	RawResp   json.RawMessage
}

// SellOutcome reports what ProcessSellStatus did.
type SellOutcome struct {
	OrderFilled  decimal.Decimal // this order's filled qty
	CycleSold    decimal.Decimal // cumulative sold across the cycle's sell orders
	Closed       bool
	LockReleased bool
	Ambiguous    bool
}

// ProcessSellStatus records a sell order's fills and drives the order + cycle. It
// handles both a resting poll (order ACKED/SUBMITTED) and a post-reprice-cancel poll
// (order CANCEL_PENDING). The cycle CLOSES only when the cumulative sold quantity
// reaches the bought inventory; ambiguity → NEEDS_RECONCILE (lock held). Idempotent.
func ProcessSellStatus(ctx context.Context, tx *sql.Tx, q *queue.Queue, p SellStatusParams) (SellOutcome, error) {
	st := p.Status
	var out SellOutcome
	out.OrderFilled = st.FilledQty

	// Ambiguous status / fetch error -> NEEDS_RECONCILE, lock held, no guess. PR11 #5: a
	// positive fill with NO usable cost basis (neither AvgPrice nor derivable ExecutedQuote)
	// is also ambiguous — we must not record a fill or close a cycle with a zero/invalid
	// sell price (accounting/PnL needs real proceeds).
	_, avgOK := usableAvgPrice(st)
	if p.StatusErr != nil || st.Status == execution.StateRejected || st.Status == execution.StateUnknown ||
		(st.Status == execution.StateFilled && (st.RemainingQty.IsPositive() || !st.FilledQty.IsPositive())) ||
		(st.FilledQty.IsPositive() && !avgOK) {
		out.Ambiguous = true
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderNeedsReconcile, "needs_reconcile", "ambiguous sell status / no cost basis"); err != nil {
			return out, err
		}
		if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleNeedsReconcile, "needs_reconcile", "ambiguous sell status / no cost basis"); err != nil {
			return out, err
		}
		return out, q.MarkSucceeded(ctx, tx, p.RequestID, p.RawResp)
	}

	// 1. Record this order's fills + accounting. Past the guard above, any positive fill has
	//    a usable cost basis (avgOK). The avg price is reported or derived (ExecutedQuote /
	//    FilledQty) inside updateOrderAccounting / upsertAggregateFill.
	requested := sellOrderQty(ctx, tx, p.OrderID)
	orderClass := ClassZero
	if st.FilledQty.IsPositive() {
		if !requested.IsZero() && st.FilledQty.GreaterThanOrEqual(requested) {
			orderClass = ClassFull
		} else {
			orderClass = ClassPartial
		}
	}
	if err := updateOrderAccounting(ctx, tx, p.OrderID, st, orderClass, requested); err != nil {
		return out, err
	}
	if st.FilledQty.IsPositive() {
		if err := upsertAggregateFill(ctx, tx, p.OrderID, p.CycleID, st); err != nil {
			return out, err
		}
	}

	// 2. Cumulative sold vs bought inventory.
	inventory := buyFilledQty(ctx, tx, p.CycleID)
	sold := sellSoldQty(ctx, tx, p.CycleID)
	out.CycleSold = sold

	// 3. Order transition for THIS order (only when not already terminal).
	switch {
	case st.Status == execution.StateFilled:
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderFilled, "sell_filled", "sell order fully filled"); err != nil {
			return out, err
		}
	case st.Status == execution.StateCanceled || st.Status == execution.StatePartiallyCanceled:
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderCancelled, "sell_cancelled", "sell order cancelled (reprice)"); err != nil {
			return out, err
		}
	case st.FilledQty.IsPositive():
		// resting, partially filled
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderPartiallyFilled, "sell_partial", "sell order partially filled"); err != nil {
			return out, err
		}
	}

	// 4. Cycle transition based on cumulative sold.
	cycleState, _, err := readCycle(ctx, tx, p.CycleID)
	if err != nil {
		return out, err
	}
	if inventory.IsPositive() && sold.GreaterThanOrEqual(inventory) {
		// Fully exited by quantity — but only CLOSE if the close accounting is complete/valid
		// (PR11 #6). If the buy/sell fill data is missing/zero/inconsistent, do NOT close/PnL:
		// divert to NEEDS_RECONCILE with the lock HELD for an operator/reconciler.
		if accErr := checkCloseAccounting(ctx, tx, p.CycleID); accErr != nil {
			out.Ambiguous = true
			if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderNeedsReconcile, "needs_reconcile", "sell complete but close accounting invalid"); err != nil {
				return out, err
			}
			if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleNeedsReconcile, "needs_reconcile", "sell complete but close accounting invalid: "+accErr.Error()); err != nil {
				return out, err
			}
			return out, q.MarkSucceeded(ctx, tx, p.RequestID, p.RawResp)
		}
		// Fully exited -> close + PnL + release lock.
		if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleSellFilled, "sell_filled", "exit sell complete"); err != nil {
			return out, err
		}
		released, err := closeCycleWithPnL(ctx, tx, p.CycleID, p.Scope, CloseReasonExited)
		if err != nil {
			return out, err
		}
		out.Closed = true
		out.LockReleased = released
	} else if sold.IsPositive() && cycleState == state.CycleSellSubmitted {
		// Partially filled while resting -> reflect it (manager keeps selling the rest).
		if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleSellPartiallyFilled, "sell_partial", "sell partially filled; managing remainder"); err != nil {
			return out, err
		}
	}
	// (cycle in SELL_REPRICE_PENDING with a partial/zero fill is left for the manager
	//  to create the replacement sell for the remaining quantity.)

	return out, q.MarkSucceeded(ctx, tx, p.RequestID, p.RawResp)
}

// closeCycleWithPnL writes the exit accounting + realized PnL on the cycle, moves it
// to CLOSED, and releases the symbol lock. realized_quote nets fees that are
// denominated in the quote currency; fees in other assets are stored raw but not
// folded into realized_quote (so it is not silently wrong).
func closeCycleWithPnL(ctx context.Context, tx *sql.Tx, cycleID int64, scope, reason string) (bool, error) {
	_ = scope // the lock is released by cycle (ActiveByCycle), not by scope string
	if err := writeCloseAccounting(ctx, tx, cycleID, reason); err != nil {
		return false, err
	}
	if err := resolveCycleTo(ctx, tx, cycleID, state.CycleClosed, "closed", reason); err != nil {
		return false, err
	}
	// Fully exited -> no exposure -> release the lock.
	if lock, ok, err := symbollock.ActiveByCycle(ctx, tx, cycleID); err != nil {
		return false, err
	} else if ok {
		return true, symbollock.Release(ctx, tx, lock.ID)
	}
	return false, nil
}

// ErrIncompleteCloseAccounting means a cycle cannot be closed with valid PnL because the
// buy/sell fill accounting is missing, zero, inconsistent, or a query failed. The caller must
// NOT close the cycle — it returns the error (operator path) or diverts to NEEDS_RECONCILE
// (automatic path). Never close a cycle with a zero/invalid cost basis or proceeds.
var ErrIncompleteCloseAccounting = errors.New("orders: incomplete/invalid close accounting")

// closeAccounting is the buy + sell fill totals needed to close a cycle with PnL.
type closeAccounting struct {
	canonical                   string
	buyQty, buyQuote, buyFee    decimal.Decimal
	buyFeeAsset                 sql.NullString
	sellQty, sellQuote, sellFee decimal.Decimal
	sellFeeAsset                sql.NullString
}

// loadCloseAccounting reads the buy/sell accounting, CHECKING every query error (a missing
// entry_buy order or a DB error becomes ErrIncompleteCloseAccounting — never silently zero).
func loadCloseAccounting(ctx context.Context, tx *sql.Tx, cycleID int64) (closeAccounting, error) {
	var a closeAccounting
	if err := tx.QueryRowContext(ctx, `
SELECT c.canonical_symbol, COALESCE(o.filled_quantity,0), COALESCE(o.quote_spent,0), COALESCE(o.fee_amount,0), o.fee_asset
FROM cycles c JOIN orders o ON o.cycle_id=c.id AND o.role='entry_buy'
WHERE c.id=? LIMIT 1`, cycleID).Scan(&a.canonical, &a.buyQty, &a.buyQuote, &a.buyFee, &a.buyFeeAsset); err != nil {
		return a, fmt.Errorf("%w: buy accounting (cycle %d): %v", ErrIncompleteCloseAccounting, cycleID, err)
	}
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(SUM(filled_quantity),0), COALESCE(SUM(quote_spent),0), COALESCE(SUM(fee_amount),0), MAX(fee_asset)
FROM orders WHERE cycle_id=? AND role='exit_sell'`, cycleID).Scan(&a.sellQty, &a.sellQuote, &a.sellFee, &a.sellFeeAsset); err != nil {
		return a, fmt.Errorf("%w: sell accounting (cycle %d): %v", ErrIncompleteCloseAccounting, cycleID, err)
	}
	return a, nil
}

// validate rejects a close whose accounting is missing/zero/inconsistent. Both sides must have
// a positive filled quantity and positive quote, and the sold quantity must match the bought
// quantity within a small tolerance (the close trigger is sold >= bought; guard runaway
// over-sell). PnL is only computed from real data.
func (a closeAccounting) validate() error {
	if !a.buyQty.IsPositive() || !a.buyQuote.IsPositive() {
		return fmt.Errorf("%w: buy qty/quote not positive (qty=%s quote=%s)", ErrIncompleteCloseAccounting, a.buyQty, a.buyQuote)
	}
	if !a.sellQty.IsPositive() || !a.sellQuote.IsPositive() {
		return fmt.Errorf("%w: sell qty/quote not positive (qty=%s quote=%s)", ErrIncompleteCloseAccounting, a.sellQty, a.sellQuote)
	}
	if a.sellQty.GreaterThan(a.buyQty.Mul(closeQtyTolerance)) {
		return fmt.Errorf("%w: sold qty %s exceeds bought %s beyond tolerance", ErrIncompleteCloseAccounting, a.sellQty, a.buyQty)
	}
	return nil
}

// closeQtyTolerance caps how far sold may exceed bought (1%) before a close is treated as
// inconsistent (→ NEEDS_RECONCILE) rather than a clean exit.
var closeQtyTolerance = decimal.RequireFromString("1.01")

// checkCloseAccounting loads + validates the close accounting; ErrIncompleteCloseAccounting
// when the cycle must NOT be closed as if all is well.
func checkCloseAccounting(ctx context.Context, tx *sql.Tx, cycleID int64) error {
	a, err := loadCloseAccounting(ctx, tx, cycleID)
	if err != nil {
		return err
	}
	return a.validate()
}

// writeCloseAccounting computes exit/PnL accounting from the recorded buy + sell order fills
// and stamps it onto the cycle. It VALIDATES the accounting first (ErrIncompleteCloseAccounting
// on missing/zero/inconsistent data — never closes with a bad cost basis/proceeds).
// realized_quote nets ONLY quote-denominated fees (non-quote fees are stored raw). It writes no
// state transition and contacts no exchange — shared by the automatic + operator-resolution close.
func writeCloseAccounting(ctx context.Context, tx *sql.Tx, cycleID int64, reason string) error {
	a, err := loadCloseAccounting(ctx, tx, cycleID)
	if err != nil {
		return err
	}
	if err := a.validate(); err != nil {
		return err
	}
	quoteUnit := quoteOf(a.canonical)
	avgSell := a.sellQuote.Div(a.sellQty) // sellQty positive (validated)
	realized := a.sellQuote.Sub(a.buyQuote)
	if a.buyFeeAsset.Valid && strings.EqualFold(a.buyFeeAsset.String, quoteUnit) {
		realized = realized.Sub(a.buyFee)
	}
	if a.sellFeeAsset.Valid && strings.EqualFold(a.sellFeeAsset.String, quoteUnit) {
		realized = realized.Sub(a.sellFee)
	}
	net := a.buyQty.Sub(a.sellQty)

	_, err = tx.ExecContext(ctx, `
UPDATE cycles SET sold_quantity=?, avg_sell_price=?, sell_quote=?, sell_fee=?, sell_fee_asset=?,
  net_quantity=?, realized_quote=?, close_reason=?, closed_at=NOW(6)
WHERE id=?`,
		a.sellQty.String(), decimalOrNull(avgSell), decimalOrNull(a.sellQuote), decimalOrNull(a.sellFee), a.sellFeeAsset,
		net.String(), realized.String(), reason, cycleID)
	return err
}

// ResolveCloseFromReconcile finalizes a NEEDS_RECONCILE cycle as CLOSED with full PnL
// accounting + symbol-lock release, using the operator-resolution transition (the
// operator-path analogue of the automatic close). It contacts NO exchange. The caller
// (the reconciliation tool) must have already recorded any operator-supplied fills onto
// the orders so the accounting reads correct cumulative quantities. Returns whether the
// lock was released. The cycle MUST currently be NEEDS_RECONCILE.
func ResolveCloseFromReconcile(ctx context.Context, tx *sql.Tx, cycleID int64, reason string) (bool, error) {
	if err := writeCloseAccounting(ctx, tx, cycleID, reason); err != nil {
		return false, err
	}
	cur, ver, err := readCycle(ctx, tx, cycleID)
	if err != nil {
		return false, err
	}
	if cur != state.CycleNeedsReconcile {
		return false, fmt.Errorf("orders: ResolveCloseFromReconcile expects NEEDS_RECONCILE, got %s (cycle %d)", cur, cycleID)
	}
	if _, err := state.ApplyCycleResolution(ctx, tx, state.CycleTransition{
		CycleID: cycleID, From: cur, To: state.CycleClosed, Version: ver,
		EventType: "operator_resolution", Reason: reason,
	}); err != nil {
		return false, err
	}
	if lock, ok, err := symbollock.ActiveByCycle(ctx, tx, cycleID); err != nil {
		return false, err
	} else if ok {
		return true, symbollock.Release(ctx, tx, lock.ID)
	}
	return false, nil
}

// ---- small query helpers ----

func sellOrderQty(ctx context.Context, tx *sql.Tx, orderID int64) decimal.Decimal {
	var s string
	_ = tx.QueryRowContext(ctx, "SELECT quantity FROM orders WHERE id=?", orderID).Scan(&s)
	return decimalOrZero(s)
}

func buyFilledQty(ctx context.Context, tx *sql.Tx, cycleID int64) decimal.Decimal {
	var s sql.NullString
	_ = tx.QueryRowContext(ctx, "SELECT filled_quantity FROM orders WHERE cycle_id=? AND role='entry_buy' LIMIT 1", cycleID).Scan(&s)
	if !s.Valid {
		return decimal.Zero
	}
	return decimalOrZero(s.String)
}

func sellSoldQty(ctx context.Context, tx *sql.Tx, cycleID int64) decimal.Decimal {
	var s sql.NullString
	_ = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(filled_quantity),0) FROM orders WHERE cycle_id=? AND role='exit_sell'", cycleID).Scan(&s)
	if !s.Valid {
		return decimal.Zero
	}
	return decimalOrZero(s.String)
}

func quoteOf(canonical string) string {
	if i := strings.IndexByte(canonical, '/'); i >= 0 {
		return canonical[i+1:]
	}
	return ""
}
