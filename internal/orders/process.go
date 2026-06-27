package orders

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
	"v3TradeBot/internal/symbollock"
)

// Follow-up request purposes (carried in the scheduled CANCEL/GET_ORDER payloads so
// the executor routes them correctly).
const (
	PurposeCancelRemainder = "simulated_ioc_cancel"
	PurposeFinalStatus     = "final_status"
)

// FollowupPayload is the payload of a scheduled CANCEL_ORDER / GET_ORDER request in
// the simulated-IOC flow.
type FollowupPayload struct {
	ExchangeOrderID    string `json:"exchange_order_id"`
	Purpose            string `json:"purpose"`
	LocalClientOrderID string `json:"local_client_order_id,omitempty"`
	CycleID            int64  `json:"cycle_id,omitempty"`
}

// PlaceAckParams is the input to OnPlaceAck.
type PlaceAckParams struct {
	RequestID  int64
	OrderID    int64
	CycleID    int64
	ExchangeID int64
	Symbol     string
	Ack        execution.OrderAck
	Intent     BuyIntentPayload
	RawResp    json.RawMessage
}

// OnPlaceAck records a successful PLACE_ORDER and schedules the simulated-IOC cancel,
// all in the caller's transaction (atomic with marking the request SUCCEEDED — rule
// #9). Steps: mark the PLACE request SUCCEEDED; stamp exchange_order_id +
// client_order_id_sent; advance the order QUEUED→SUBMITTED→ACKED and the cycle
// BUY_REQUEST_QUEUED→BUY_SUBMITTED via the state machine; enqueue a CANCEL_ORDER
// scheduled MakerWait into the future (queued wait — no worker sleeps).
func OnPlaceAck(ctx context.Context, tx *sql.Tx, q *queue.Queue, p PlaceAckParams) error {
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
	if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderSubmitted, "place_sent", "buy order sent"); err != nil {
		return err
	}
	if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderAcked, "place_ack", "buy order acknowledged by exchange"); err != nil {
		return err
	}
	if err := advanceCycleTo(ctx, tx, p.CycleID, state.CycleBuySubmitted, "buy_submitted", "buy submitted to exchange"); err != nil {
		return err
	}
	// Schedule the cancel of the remainder after the configured maker wait. Queued,
	// not slept. A duplicate (re-run) is harmless — treat it as already scheduled.
	payload, _ := json.Marshal(FollowupPayload{ExchangeOrderID: p.Ack.ExchangeOrderID, Purpose: PurposeCancelRemainder, LocalClientOrderID: p.Intent.LocalClientOrderID, CycleID: p.CycleID})
	_, err := q.EnqueueScheduled(ctx, tx, queue.Request{
		ExchangeID:     p.ExchangeID,
		Symbol:         p.Symbol,
		CycleID:        &p.CycleID,
		OrderID:        &p.OrderID,
		Type:           queue.TypeCancelOrder,
		Priority:       40,
		Payload:        payload,
		IdempotencyKey: fmt.Sprintf("cancel:c%d:buy", p.CycleID),
	}, p.Intent.MakerWait())
	if err != nil && !errors.Is(err, queue.ErrDuplicateIdempotencyKey) {
		return err
	}
	return nil
}

// CancelResultParams is the input to OnCancelResult.
type CancelResultParams struct {
	RequestID       int64
	OrderID         int64
	CycleID         int64
	ExchangeID      int64
	Symbol          string
	ExchangeOrderID string
	LocalClientID   string
	RawResp         json.RawMessage
	FinalCheckDelay time.Duration // small grace before reading the final status
}

// OnCancelResult records a successful CANCEL_ORDER and schedules the final
// GET_ORDER status check (atomic). The order moves ACKED→CANCEL_PENDING (cancel
// requested; the GET_ORDER confirms the real outcome — a cancel may race a fill).
func OnCancelResult(ctx context.Context, tx *sql.Tx, q *queue.Queue, p CancelResultParams) error {
	if err := q.MarkSucceeded(ctx, tx, p.RequestID, p.RawResp); err != nil {
		return err
	}
	if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderCancelPending, "cancel_requested", "cancel of remainder requested"); err != nil {
		return err
	}
	payload, _ := json.Marshal(FollowupPayload{ExchangeOrderID: p.ExchangeOrderID, Purpose: PurposeFinalStatus, LocalClientOrderID: p.LocalClientID, CycleID: p.CycleID})
	_, err := q.EnqueueScheduled(ctx, tx, queue.Request{
		ExchangeID:     p.ExchangeID,
		Symbol:         p.Symbol,
		CycleID:        &p.CycleID,
		OrderID:        &p.OrderID,
		Type:           queue.TypeGetOrder,
		Priority:       30,
		Payload:        payload,
		IdempotencyKey: fmt.Sprintf("final-status:c%d:buy", p.CycleID),
	}, p.FinalCheckDelay)
	if err != nil && !errors.Is(err, queue.ErrDuplicateIdempotencyKey) {
		return err
	}
	return nil
}

// FinalStatusParams is the input to ProcessFinalStatus.
type FinalStatusParams struct {
	RequestID int64
	OrderID   int64
	CycleID   int64
	Scope     string // exchange code — for the symbol lock
	Requested decimal.Decimal
	Status    execution.OrderStatus
	StatusErr error
	RawResp   json.RawMessage
}

// Outcome reports what ProcessFinalStatus did.
type Outcome struct {
	Class        Classification
	FilledQty    decimal.Decimal
	LockReleased bool
}

// ProcessFinalStatus is the heart of PR10: it classifies the buy order's final
// status, records the fill accounting (idempotently), advances the order + cycle
// through the state machine, releases the symbol lock only when there is positively
// no exposure, and marks the GET_ORDER request SUCCEEDED — all in the caller's
// transaction. Ambiguity (incl. a missing order with no fill proof) → NEEDS_RECONCILE
// with the lock HELD; it never guesses and never resends.
func ProcessFinalStatus(ctx context.Context, tx *sql.Tx, q *queue.Queue, p FinalStatusParams) (Outcome, error) {
	st := p.Status
	class := Classify(st, p.StatusErr)
	out := Outcome{Class: class, FilledQty: st.FilledQty}

	// The requested quantity is authoritative from the order row (used for the
	// remaining-qty accounting) when the caller did not supply it.
	requested := p.Requested
	if !requested.IsPositive() {
		var qs string
		if err := tx.QueryRowContext(ctx, "SELECT quantity FROM orders WHERE id=?", p.OrderID).Scan(&qs); err != nil {
			return out, err
		}
		requested = decimalOrZero(qs)
	}

	// 1. Fill accounting + evidence on the order (always — even zero/ambiguous, so the
	//    last observation is recorded). The per-fill row is written only when there is
	//    a usable fill, deduped by a deterministic key so repeated processing is
	//    idempotent.
	if err := updateOrderAccounting(ctx, tx, p.OrderID, st, class, requested); err != nil {
		return out, err
	}
	if st.FilledQty.IsPositive() && st.AvgPrice.IsPositive() {
		if err := upsertAggregateFill(ctx, tx, p.OrderID, p.CycleID, st); err != nil {
			return out, err
		}
	}

	// 2. State transitions (NEEDS_RECONCILE is the safe fallback for any illegal/
	//    ambiguous case).
	switch class {
	case ClassFull:
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderFilled, "buy_filled", "buy fully filled"); err != nil {
			return out, err
		}
		if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleBuyFilled, "buy_filled", "buy fully filled"); err != nil {
			return out, err
		}
	case ClassPartial:
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderPartiallyFilled, "buy_partial", "buy partially filled; remainder cancelled"); err != nil {
			return out, err
		}
		if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleBuyPartiallyFilled, "buy_partial", "buy partially filled; continue with filled qty"); err != nil {
			return out, err
		}
	case ClassZero:
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderCancelled, "buy_zero_fill", ReasonZeroFill); err != nil {
			return out, err
		}
		if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleCancelled, "buy_zero_fill", ReasonZeroFill); err != nil {
			return out, err
		}
	default: // ClassAmbiguous
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderNeedsReconcile, "needs_reconcile", "ambiguous final status"); err != nil {
			return out, err
		}
		if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleNeedsReconcile, "needs_reconcile", "ambiguous final status"); err != nil {
			return out, err
		}
	}

	// 3. Release the symbol lock ONLY when there is positively no exposure.
	if class.ReleasesLock() {
		if lock, ok, err := symbollock.ActiveByCycle(ctx, tx, p.CycleID); err != nil {
			return out, err
		} else if ok {
			if err := symbollock.Release(ctx, tx, lock.ID); err != nil {
				return out, err
			}
			out.LockReleased = true
		}
	}

	// 4. Mark the GET_ORDER request SUCCEEDED (atomic with all of the above).
	if err := q.MarkSucceeded(ctx, tx, p.RequestID, p.RawResp); err != nil {
		return out, err
	}
	return out, nil
}

// PlaceRejectedParams is the input to OnPlaceRejected.
type PlaceRejectedParams struct {
	RequestID int64
	OrderID   int64
	CycleID   int64
	Cause     string
	RawResp   json.RawMessage
}

// OnPlaceRejected cleanly resolves a DEFINITELY-rejected buy (the venue refused the
// order, so it was never placed and there is no exposure): the PLACE request is
// FAILED, the order and cycle go to FAILED, and the symbol lock is released — all in
// the caller's tx. This prevents a rejected place from orphaning the cycle/lock. A
// rejection is a real failure (FAILED), distinct from a clean zero-fill (CANCELLED).
func OnPlaceRejected(ctx context.Context, tx *sql.Tx, q *queue.Queue, p PlaceRejectedParams) error {
	if err := q.MarkFailed(ctx, tx, p.RequestID, p.Cause); err != nil {
		return err
	}
	if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderFailed, "place_rejected", p.Cause); err != nil {
		return err
	}
	if err := resolveCycleTo(ctx, tx, p.CycleID, state.CycleFailed, "place_rejected", p.Cause); err != nil {
		return err
	}
	if lock, ok, err := symbollock.ActiveByCycle(ctx, tx, p.CycleID); err != nil {
		return err
	} else if ok {
		return symbollock.Release(ctx, tx, lock.ID)
	}
	return nil
}

// MarkNeedsReconcile pushes the order (and its cycle, if given) to NEEDS_RECONCILE
// within the caller's tx — the safe response to an ambiguous cancel/send outcome.
// Terminal/already-reconcile rows are left untouched.
func MarkNeedsReconcile(ctx context.Context, tx *sql.Tx, orderID int64, cycleID *int64, reason string) error {
	if err := resolveOrderTo(ctx, tx, orderID, state.OrderNeedsReconcile, "needs_reconcile", reason); err != nil {
		return err
	}
	if cycleID != nil {
		if err := resolveCycleTo(ctx, tx, *cycleID, state.CycleNeedsReconcile, "needs_reconcile", reason); err != nil {
			return err
		}
	}
	return nil
}

// ---- accounting helpers ----

func updateOrderAccounting(ctx context.Context, tx *sql.Tx, orderID int64, st execution.OrderStatus, class Classification, requested decimal.Decimal) error {
	remaining := st.RemainingQty
	if !remaining.IsPositive() {
		remaining = requested.Sub(st.FilledQty)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}
	}
	quote := st.ExecutedQuote
	if !quote.IsPositive() && st.FilledQty.IsPositive() && st.AvgPrice.IsPositive() {
		quote = st.FilledQty.Mul(st.AvgPrice)
	}
	_, err := tx.ExecContext(ctx, `
UPDATE orders SET
  filled_quantity = ?, remaining_quantity = ?, avg_fill_price = ?, quote_spent = ?,
  fee_amount = ?, fee_asset = ?, actual_execution_mode = ?, fill_result = ?, last_normalized_status = ?,
  exchange_order_id = COALESCE(NULLIF(?,''), exchange_order_id)
WHERE id = ?`,
		st.FilledQty.String(), remaining.String(), decimalOrNull(st.AvgPrice), decimalOrNull(quote),
		decimalOrNull(st.Fee), nullStr(st.FeeAsset), actualExecutionMode(st.Liquidity), string(class), string(st.Status),
		st.ExchangeOrderID, orderID)
	return err
}

// upsertAggregateFill records (idempotently) one aggregate fill row for the final
// status. With only aggregate data from GET_ORDER we synthesize a deterministic
// exchange_fill_id so repeated processing does not duplicate the row
// (UNIQUE(order_id, exchange_fill_id)).
func upsertAggregateFill(ctx context.Context, tx *sql.Tx, orderID, cycleID int64, st execution.OrderStatus) error {
	fillID := syntheticFillID(st)
	quote := st.ExecutedQuote
	if !quote.IsPositive() {
		quote = st.FilledQty.Mul(st.AvgPrice)
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO fills (order_id, cycle_id, exchange_fill_id, quantity, price, quote_amount, fee_amount, fee_asset, filled_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
ON DUPLICATE KEY UPDATE quantity=VALUES(quantity), price=VALUES(price), quote_amount=VALUES(quote_amount),
  fee_amount=VALUES(fee_amount), fee_asset=VALUES(fee_asset)`,
		orderID, cycleID, fillID, st.FilledQty.String(), st.AvgPrice.String(), decimalOrNull(quote),
		decimalOrNull(st.Fee), nullStr(st.FeeAsset))
	return err
}

// ---- state-machine helpers ----

// advanceOrderTo applies a strict order transition (error on an illegal/unexpected
// state, so the executor's tx rolls back and the request is left for the sweeper).
// Re-running with the order already AT the target is a safe no-op.
func advanceOrderTo(ctx context.Context, tx *sql.Tx, orderID int64, target state.OrderState, eventType, reason string) error {
	cur, ver, err := readOrder(ctx, tx, orderID)
	if err != nil {
		return err
	}
	if cur == target {
		return nil
	}
	if state.ValidateOrderTransition(cur, target) != nil {
		return fmt.Errorf("orders: illegal order transition %s->%s (order %d)", cur, target, orderID)
	}
	_, err = state.ApplyOrderTransition(ctx, tx, state.OrderTransition{OrderID: orderID, From: cur, To: target, Version: ver, EventType: eventType, Reason: reason})
	return err
}

func advanceCycleTo(ctx context.Context, tx *sql.Tx, cycleID int64, target state.CycleState, eventType, reason string) error {
	cur, ver, err := readCycle(ctx, tx, cycleID)
	if err != nil {
		return err
	}
	if cur == target {
		return nil
	}
	if state.ValidateCycleTransition(cur, target) != nil {
		return fmt.Errorf("orders: illegal cycle transition %s->%s (cycle %d)", cur, target, cycleID)
	}
	_, err = state.ApplyCycleTransition(ctx, tx, state.CycleTransition{CycleID: cycleID, From: cur, To: target, Version: ver, EventType: eventType, Reason: reason})
	return err
}

// resolveOrderTo advances toward target, but DIVERTS to NEEDS_RECONCILE when the
// transition is illegal from the current state (never errors on an unexpected
// state — the safe default is always NEEDS_RECONCILE). A terminal/at-target order
// is left untouched (idempotent replay).
func resolveOrderTo(ctx context.Context, tx *sql.Tx, orderID int64, target state.OrderState, eventType, reason string) error {
	cur, ver, err := readOrder(ctx, tx, orderID)
	if err != nil {
		return err
	}
	if cur == target || state.IsTerminalOrder(cur) || cur == state.OrderNeedsReconcile {
		return nil
	}
	to := target
	et, rn := eventType, reason
	if state.ValidateOrderTransition(cur, target) != nil {
		to, et, rn = state.OrderNeedsReconcile, "needs_reconcile", "illegal transition "+string(cur)+"->"+string(target)
	}
	_, err = state.ApplyOrderTransition(ctx, tx, state.OrderTransition{OrderID: orderID, From: cur, To: to, Version: ver, EventType: et, Reason: rn})
	return err
}

func resolveCycleTo(ctx context.Context, tx *sql.Tx, cycleID int64, target state.CycleState, eventType, reason string) error {
	cur, ver, err := readCycle(ctx, tx, cycleID)
	if err != nil {
		return err
	}
	if cur == target || state.IsTerminalCycle(cur) || cur == state.CycleNeedsReconcile {
		return nil
	}
	to := target
	et, rn := eventType, reason
	if state.ValidateCycleTransition(cur, target) != nil {
		to, et, rn = state.CycleNeedsReconcile, "needs_reconcile", "illegal transition "+string(cur)+"->"+string(target)
	}
	_, err = state.ApplyCycleTransition(ctx, tx, state.CycleTransition{CycleID: cycleID, From: cur, To: to, Version: ver, EventType: et, Reason: rn})
	return err
}

func readOrder(ctx context.Context, tx *sql.Tx, id int64) (state.OrderState, int64, error) {
	var s string
	var v int64
	err := tx.QueryRowContext(ctx, "SELECT state, version FROM orders WHERE id=?", id).Scan(&s, &v)
	return state.OrderState(s), v, err
}

func readCycle(ctx context.Context, tx *sql.Tx, id int64) (state.CycleState, int64, error) {
	var s string
	var v int64
	err := tx.QueryRowContext(ctx, "SELECT state, version FROM cycles WHERE id=?", id).Scan(&s, &v)
	return state.CycleState(s), v, err
}

// syntheticFillID is a deterministic dedup key for the aggregate final fill when the
// venue gives no per-fill id (GET_ORDER returns aggregates). Derived from the order
// identity so repeated processing maps to the same fills row (idempotent). Prefer the
// exchange order id; fall back to the client order id.
func syntheticFillID(st execution.OrderStatus) string {
	base := st.ExchangeOrderID
	if base == "" {
		base = st.ClientOrderID
	}
	return "final:" + base
}

func decimalOrNull(d decimal.Decimal) any {
	if d.IsZero() {
		return nil
	}
	return d.String()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
