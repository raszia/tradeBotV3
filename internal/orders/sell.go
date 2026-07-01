package orders

import (
	"context"
	"database/sql"
	"encoding/json"
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
	if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderAcked, "sell_ack", "sell order acknowledged"); err != nil {
		return err
	}
	return advanceCycleTo(ctx, tx, p.CycleID, state.CycleSellSubmitted, "sell_submitted", "sell resting on the book")
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

	// Ambiguous status / fetch error -> NEEDS_RECONCILE, lock held, no guess.
	if p.StatusErr != nil || st.Status == execution.StateRejected || st.Status == execution.StateUnknown ||
		(st.Status == execution.StateFilled && (st.RemainingQty.IsPositive() || !st.FilledQty.IsPositive())) {
		out.Ambiguous = true
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderNeedsReconcile, "needs_reconcile", "ambiguous sell status"); err != nil {
			return out, err
		}
		if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleNeedsReconcile, "needs_reconcile", "ambiguous sell status"); err != nil {
			return out, err
		}
		return out, q.MarkSucceeded(ctx, tx, p.RequestID, p.RawResp)
	}

	// 1. Record this order's fills + accounting.
	requested := sellOrderQty(ctx, tx, p.OrderID)
	orderClass := ClassZero
	if st.FilledQty.IsPositive() && st.AvgPrice.IsPositive() {
		if !requested.IsZero() && st.FilledQty.GreaterThanOrEqual(requested) {
			orderClass = ClassFull
		} else {
			orderClass = ClassPartial
		}
	}
	if err := updateOrderAccounting(ctx, tx, p.OrderID, st, orderClass, requested); err != nil {
		return out, err
	}
	if st.FilledQty.IsPositive() && st.AvgPrice.IsPositive() {
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

// writeCloseAccounting computes exit/PnL accounting from the recorded buy + sell order
// fills and stamps it onto the cycle (sold qty, avg sell, sell quote/fee, net qty,
// realized_quote, close reason, closed_at). realized_quote nets ONLY quote-denominated
// fees (non-quote fees are stored raw). It writes no state transition and contacts no
// exchange — shared by the automatic close and the operator resolution close.
func writeCloseAccounting(ctx context.Context, tx *sql.Tx, cycleID int64, reason string) error {
	var (
		canonical                   string
		buyQty, buyQuote, buyFee    decimal.Decimal
		buyFeeAsset                 sql.NullString
		sellQty, sellQuote, sellFee decimal.Decimal
		sellFeeAsset                sql.NullString
	)
	// Buy side (entry_buy order).
	_ = tx.QueryRowContext(ctx, `
SELECT c.canonical_symbol, COALESCE(o.filled_quantity,0), COALESCE(o.quote_spent,0), COALESCE(o.fee_amount,0), o.fee_asset
FROM cycles c JOIN orders o ON o.cycle_id=c.id AND o.role='entry_buy'
WHERE c.id=? LIMIT 1`, cycleID).Scan(&canonical, &buyQty, &buyQuote, &buyFee, &buyFeeAsset)
	// Sell side (sum across exit_sell orders).
	_ = tx.QueryRowContext(ctx, `
SELECT COALESCE(SUM(filled_quantity),0), COALESCE(SUM(quote_spent),0), COALESCE(SUM(fee_amount),0), MAX(fee_asset)
FROM orders WHERE cycle_id=? AND role='exit_sell'`, cycleID).Scan(&sellQty, &sellQuote, &sellFee, &sellFeeAsset)

	quoteUnit := quoteOf(canonical)
	avgSell := decimal.Zero
	if sellQty.IsPositive() {
		avgSell = sellQuote.Div(sellQty)
	}
	realized := sellQuote.Sub(buyQuote)
	if buyFeeAsset.Valid && strings.EqualFold(buyFeeAsset.String, quoteUnit) {
		realized = realized.Sub(buyFee)
	}
	if sellFeeAsset.Valid && strings.EqualFold(sellFeeAsset.String, quoteUnit) {
		realized = realized.Sub(sellFee)
	}
	net := buyQty.Sub(sellQty)

	_, err := tx.ExecContext(ctx, `
UPDATE cycles SET sold_quantity=?, avg_sell_price=?, sell_quote=?, sell_fee=?, sell_fee_asset=?,
  net_quantity=?, realized_quote=?, close_reason=?, closed_at=NOW(6)
WHERE id=?`,
		sellQty.String(), decimalOrNull(avgSell), decimalOrNull(sellQuote), decimalOrNull(sellFee), sellFeeAsset,
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
