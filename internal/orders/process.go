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
	// Attempt bounds the ambiguous-cancel recovery loop (PR19 round 2 #3): each read-only probe
	// that still finds the order OPEN re-issues the cancel with Attempt+1; after a bounded number
	// of attempts the order goes to NEEDS_RECONCILE instead of looping forever.
	Attempt int `json:"attempt,omitempty"`
	// FirstProbeAt (unix millis) is stamped on the FIRST recovery probe and carried across
	// re-probes so the recovery window's TotalTimeout is measured from the start (PR19 round 4 #5).
	FirstProbeAt int64 `json:"first_probe_at,omitempty"`
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
	// PR10 #6: the place succeeded but the ack carries NO usable exchange_order_id. We must
	// NOT schedule a blind CANCEL_ORDER("")/GET_ORDER("") — the order exists on the venue but
	// is untrackable by us. Push order+cycle to NEEDS_RECONCILE and KEEP the symbol lock; the
	// reconciler determines whether it filled/exists. (Lookup/cancel by client_order_id is not
	// part of the current adapter contract, so we do not rely on an empty exchange id.)
	if p.Ack.ExchangeOrderID == "" {
		return MarkNeedsReconcile(ctx, tx, p.OrderID, &p.CycleID,
			"place ack without a usable exchange_order_id — cannot track/cancel; needs reconcile")
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
	// A fill row is written only with a USABLE cost basis (reported or derived from executed
	// quote) — never with a zero/invalid price (PR10 #7).
	if _, ok := usableAvgPrice(st); st.FilledQty.IsPositive() && ok {
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

// preSendCycleStates are the cycle states in which a buy has NOT yet been submitted to the
// venue, so no exchange exposure can exist for it.
var preSendCycleStates = map[state.CycleState]bool{
	state.CycleNew:              true,
	state.CycleSignalDetected:   true,
	state.CycleBuyRequestQueued: true,
}

// MutationKind selects the terminal disposition for a denied/not-sent mutating request.
type MutationKind int

const (
	KindEntryBuy MutationKind = iota // a clean fail + lock release is allowed (only with zero exposure)
	KindExitSell                     // inventory may exist → always NEEDS_RECONCILE + lock HELD
	KindCancel                       // a venue order may still be open → NEEDS_RECONCILE + lock HELD
	KindUnknown                      // role indeterminate (e.g. order-role read failed) → conservative
)

// DenialParams drives DisposeDeniedMutation.
type DenialParams struct {
	RequestID         int64
	OrderID           int64 // the request's claimed (authoritative) order id
	ClaimedCycleID    int64 // the request's claimed cycle id — VERIFIED, never used to mutate
	ClaimedExchangeID int64 // the request's claimed exchange id — VERIFIED
	Kind              MutationKind
	// RequestDead: when true the request is marked DEAD (exhaustion / inconsistency / cancel);
	// otherwise FAILED (a clean pre-send rejection).
	RequestDead bool
	// BroadTerminal: when true, the DEAD transition accepts a request in ANY non-terminal status
	// (QUEUED/RETRY_SCHEDULED/CLAIMED/IN_FLIGHT) via MarkDeadMalformed, instead of only
	// CLAIMED/IN_FLIGHT. Used by the malformed/inconsistent sweep, which finalizes rows that were
	// never validly dispatched (round 9 #1/#3). Denial/exhaustion callers leave it false (their
	// rows are always CLAIMED/IN_FLIGHT, and MarkDead's stricter guard is a useful safety check).
	BroadTerminal bool
	Cause         string
}

// DisposeDeniedMutation is the single AUTHORITATIVE terminal disposition for a mutating request
// that must not / did not execute (PR20 correction #1/#2/#4). It:
//   - loads the ORDER `FOR UPDATE` and derives the AUTHORITATIVE cycle from `order.cycle_id`
//     (NEVER the queue's claimed cycle_id — a request whose claimed cycle_id points at an
//     unrelated cycle can never mutate or unlock that cycle);
//   - proves the request↔order↔cycle↔exchange relationship is consistent;
//   - releases the symbol lock ONLY for an ENTRY BUY whose zero-exposure is proven (order
//     QUEUED, filled 0, exchange_order_id NULL, cycle pre-send) AND whose relationship is
//     consistent; otherwise HOLDS the lock and marks the ACTUAL order+cycle NEEDS_RECONCILE.
//
// Every state change happens in the caller's transaction, on rows read FOR UPDATE.
func DisposeDeniedMutation(ctx context.Context, tx *sql.Tx, q *queue.Queue, p DenialParams) error {
	var oState, filledS string
	var exOID sql.NullString
	var oExchangeID, oCycleID int64
	err := tx.QueryRowContext(ctx,
		"SELECT state, filled_quantity, exchange_order_id, exchange_id, cycle_id FROM orders WHERE id=? FOR UPDATE",
		p.OrderID).Scan(&oState, &filledS, &exOID, &oExchangeID, &oCycleID)
	if errors.Is(err, sql.ErrNoRows) {
		// No order row to reconcile — resolve the request only (nothing else to strand).
		return markRequestTerminal(ctx, tx, q, p.RequestID, p.RequestDead, p.BroadTerminal, p.Cause)
	}
	if err != nil {
		return err
	}
	// The request's claimed cycle/exchange MUST match the order's real ones. A mismatch is an
	// inconsistent (stale/tampered) request: force the conservative path and never touch the
	// unrelated claimed cycle.
	consistent := (p.ClaimedCycleID == 0 || p.ClaimedCycleID == oCycleID) &&
		(p.ClaimedExchangeID == 0 || p.ClaimedExchangeID == oExchangeID)

	var cState string
	if err := tx.QueryRowContext(ctx, "SELECT state FROM cycles WHERE id=? FOR UPDATE", oCycleID).Scan(&cState); err != nil {
		return err
	}
	filled, ferr := decimal.NewFromString(filledS)
	zeroExposure := ferr == nil &&
		oState == string(state.OrderQueued) &&
		filled.IsZero() &&
		!exOID.Valid &&
		preSendCycleStates[state.CycleState(cState)]

	if p.Kind == KindEntryBuy && consistent && zeroExposure {
		// Proven no exposure on a consistent entry buy → clean fail + release, on the ACTUAL cycle.
		if err := markRequestTerminal(ctx, tx, q, p.RequestID, p.RequestDead, p.BroadTerminal, p.Cause); err != nil {
			return err
		}
		if err := resolveOrderTo(ctx, tx, p.OrderID, state.OrderFailed, "place_rejected", p.Cause); err != nil {
			return err
		}
		if err := resolveCycleTo(ctx, tx, oCycleID, state.CycleFailed, "place_rejected", p.Cause); err != nil {
			return err
		}
		return releaseLockByCycle(ctx, tx, oCycleID)
	}
	// Conservative: request terminal (DEAD for inconsistency/uncertainty), ACTUAL order+cycle
	// NEEDS_RECONCILE, lock HELD. A relationship inconsistency always dead-letters the request.
	cause := p.Cause
	dead := p.RequestDead || !consistent
	if !consistent {
		cause = "inconsistent request/order/cycle/exchange relationship — " + cause
	}
	if err := markRequestTerminal(ctx, tx, q, p.RequestID, dead, p.BroadTerminal, cause); err != nil {
		return err
	}
	return MarkNeedsReconcile(ctx, tx, p.OrderID, &oCycleID, cause)
}

// DisposeMalformedMutation resolves a MUTATING request that has no usable order (order_id NULL,
// or an order that cannot be identified) — a malformed/historical row (PR20 correction #1/#4).
// The request is marked DEAD; if a cycle_id is known, that cycle is pushed to NEEDS_RECONCILE
// with its symbol lock HELD (we cannot prove zero exposure without an order, so we NEVER release
// the lock). All within the caller's transaction.
func DisposeMalformedMutation(ctx context.Context, tx *sql.Tx, q *queue.Queue, requestID int64, cycleID *int64, cause string) error {
	if cycleID != nil {
		if err := resolveCycleTo(ctx, tx, *cycleID, state.CycleNeedsReconcile, "needs_reconcile", cause); err != nil {
			return err
		}
		// Lock stays HELD (no release) — no order means no proof of zero exposure.
	}
	// A malformed row may be QUEUED/RETRY_SCHEDULED (never validly dispatched), so use the
	// broad-status terminal rather than MarkDead (which only accepts CLAIMED/IN_FLIGHT).
	return q.MarkDeadMalformed(ctx, tx, requestID, cause)
}

// markRequestTerminal marks the request FAILED or DEAD within the tx. When broad is true, the
// DEAD transition accepts any non-terminal status (MarkDeadMalformed) — used by the malformed/
// inconsistent sweep, whose rows may still be QUEUED. Otherwise DEAD requires CLAIMED/IN_FLIGHT.
func markRequestTerminal(ctx context.Context, tx *sql.Tx, q *queue.Queue, requestID int64, dead, broad bool, cause string) error {
	if dead {
		if broad {
			return q.MarkDeadMalformed(ctx, tx, requestID, cause)
		}
		return q.MarkDead(ctx, tx, requestID, cause)
	}
	return q.MarkFailed(ctx, tx, requestID, cause)
}

// releaseLockByCycle releases the ACTIVE symbol lock for a cycle (no-op when none is active).
func releaseLockByCycle(ctx context.Context, tx *sql.Tx, cycleID int64) error {
	if lock, ok, err := symbollock.ActiveByCycle(ctx, tx, cycleID); err != nil {
		return err
	} else if ok {
		return symbollock.Release(ctx, tx, lock.ID)
	}
	return nil
}

// OnBuyDenied is the entry-buy façade over DisposeDeniedMutation (PR20 correction #1/#2). The
// authoritative cycle is derived from the order — the caller's PlaceRejectedParams.CycleID is
// passed only as the CLAIMED cycle to be VERIFIED against it, never used to mutate/unlock.
func OnBuyDenied(ctx context.Context, tx *sql.Tx, q *queue.Queue, p PlaceRejectedParams) error {
	return DisposeDeniedMutation(ctx, tx, q, DenialParams{
		RequestID: p.RequestID, OrderID: p.OrderID, ClaimedCycleID: p.CycleID,
		Kind: KindEntryBuy, Cause: p.Cause,
	})
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
	// Effective avg price = reported, else derived from executed quote (PR10 #7). quote_spent
	// prefers the venue's ExecutedQuote, else filled_qty × effective avg.
	avg, _ := usableAvgPrice(st)
	quote := st.ExecutedQuote
	if !quote.IsPositive() && st.FilledQty.IsPositive() && avg.IsPositive() {
		quote = st.FilledQty.Mul(avg)
	}
	_, err := tx.ExecContext(ctx, `
UPDATE orders SET
  filled_quantity = ?, remaining_quantity = ?, avg_fill_price = ?, quote_spent = ?,
  fee_amount = ?, fee_asset = ?, actual_execution_mode = ?, fill_result = ?, last_normalized_status = ?,
  exchange_order_id = COALESCE(NULLIF(?,''), exchange_order_id)
WHERE id = ?`,
		st.FilledQty.String(), remaining.String(), decimalOrNull(avg), decimalOrNull(quote),
		decimalOrNull(st.Fee), nullStr(st.FeeAsset), actualExecutionMode(st.Liquidity), string(class), string(st.Status),
		st.ExchangeOrderID, orderID)
	return err
}

// upsertAggregateFill records (idempotently) one aggregate fill row for the final
// status. With only aggregate data from GET_ORDER we synthesize a deterministic
// exchange_fill_id so repeated processing does not duplicate the row
// (UNIQUE(order_id, exchange_fill_id)).
// upsertAggregateFill must be called only when usableAvgPrice(st) is ok (a positive cost
// basis exists), so a fill row never carries a zero/invalid price (PR10 #7).
func upsertAggregateFill(ctx context.Context, tx *sql.Tx, orderID, cycleID int64, st execution.OrderStatus) error {
	fillID := syntheticFillID(st)
	avg, _ := usableAvgPrice(st) // caller guarantees ok
	quote := st.ExecutedQuote
	if !quote.IsPositive() {
		quote = st.FilledQty.Mul(avg)
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO fills (order_id, cycle_id, exchange_fill_id, quantity, price, quote_amount, fee_amount, fee_asset, filled_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
ON DUPLICATE KEY UPDATE quantity=VALUES(quantity), price=VALUES(price), quote_amount=VALUES(quote_amount),
  fee_amount=VALUES(fee_amount), fee_asset=VALUES(fee_asset)`,
		orderID, cycleID, fillID, st.FilledQty.String(), avg.String(), decimalOrNull(quote),
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
