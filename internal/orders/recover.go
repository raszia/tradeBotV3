package orders

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
)

// PR19 round 2 — automatic read-only recovery of AMBIGUOUS mutation timeouts.
//
// A timed-out PlaceOrder/CancelOrder is an UNKNOWN outcome, never a success or failure and
// never blindly retried. Instead of dead-lettering straight to NEEDS_RECONCILE, the executor
// schedules a READ-ONLY probe (a GET_ORDER) that discovers the real exchange state and then
// continues the state machine:
//   - ambiguous PLACE  → look the order up by client_order_id (the exchange id was never
//     returned); found → resume the normal ack flow; provably-not-placed → resolve as a clean
//     rejection; still ambiguous after bounded attempts → NEEDS_RECONCILE.
//   - ambiguous CANCEL → look the order up by exchange_order_id; determine open / canceled /
//     partial / filled; record the ACTUAL filled quantity; still-open → re-cancel (bounded,
//     never blind — we PROVED it is open); bounded attempts exhausted → NEEDS_RECONCILE.
//
// The probes are persisted queue rows, so recovery survives a restart and is safe across
// multiple executor instances (the per-exchange claim lock serialises processing, and fill
// recording is idempotent).
const (
	PurposeAmbiguousPlaceProbe  = "ambiguous_place_probe"
	PurposeAmbiguousCancelProbe = "ambiguous_cancel_probe"
)

// ProbeParams is the input to the recovery-scheduling helpers.
type ProbeParams struct {
	ExchangeID         int64
	Symbol             string
	CycleID            int64
	OrderID            int64
	ExchangeOrderID    string // known for a cancel probe; empty for a place probe (never returned)
	LocalClientOrderID string // the recovery lookup key when the exchange id is unknown
	Attempt            int    // bounded-retry counter
	FirstProbeAt       int64  // unix millis of the first probe (carried for the TotalTimeout window)
	Delay              time.Duration
}

// SchedulePlaceProbe enqueues the read-only GET_ORDER that recovers an ambiguous PLACE. The
// exchange order id is unknown (the ack timed out), so the probe carries the local client
// order id and recovery looks the order up by it.
func SchedulePlaceProbe(ctx context.Context, tx *sql.Tx, q *queue.Queue, p ProbeParams) error {
	payload, _ := json.Marshal(FollowupPayload{
		Purpose: PurposeAmbiguousPlaceProbe, LocalClientOrderID: p.LocalClientOrderID, CycleID: p.CycleID, Attempt: p.Attempt, FirstProbeAt: p.FirstProbeAt,
	})
	// Attempt-scoped idempotency key so a bounded backoff re-probe enqueues a NEW row (a fixed
	// key would collide and silently drop the retry).
	return scheduleProbe(ctx, tx, q, p, queue.TypeGetOrder, 25, payload,
		fmt.Sprintf("place-probe:o%d:a%d", p.OrderID, p.Attempt))
}

// ScheduleCancelProbe enqueues the read-only GET_ORDER that recovers an ambiguous CANCEL. The
// exchange order id IS known (the cancel targeted an existing order).
func ScheduleCancelProbe(ctx context.Context, tx *sql.Tx, q *queue.Queue, p ProbeParams) error {
	payload, _ := json.Marshal(FollowupPayload{
		Purpose: PurposeAmbiguousCancelProbe, ExchangeOrderID: p.ExchangeOrderID,
		LocalClientOrderID: p.LocalClientOrderID, CycleID: p.CycleID, Attempt: p.Attempt, FirstProbeAt: p.FirstProbeAt,
	})
	return scheduleProbe(ctx, tx, q, p, queue.TypeGetOrder, 25, payload,
		fmt.Sprintf("cancel-probe:o%d:a%d", p.OrderID, p.Attempt))
}

// ScheduleReCancel re-issues a CANCEL after a read-only probe PROVED the order is still open
// (a proven re-cancel, never a blind retry). Bounded by Attempt. `sellReprice` routes it to the
// sell reprice-cancel handler; otherwise to the buy simulated-IOC cancel handler.
func ScheduleReCancel(ctx context.Context, tx *sql.Tx, q *queue.Queue, p ProbeParams, sellReprice bool) error {
	purpose := PurposeCancelRemainder
	if sellReprice {
		purpose = PurposeSellReprice
	}
	payload, _ := json.Marshal(FollowupPayload{
		ExchangeOrderID: p.ExchangeOrderID, Purpose: purpose,
		LocalClientOrderID: p.LocalClientOrderID, CycleID: p.CycleID, Attempt: p.Attempt, FirstProbeAt: p.FirstProbeAt,
	})
	return scheduleProbe(ctx, tx, q, p, queue.TypeCancelOrder, 40, payload,
		fmt.Sprintf("recancel:o%d:a%d", p.OrderID, p.Attempt))
}

func scheduleProbe(ctx context.Context, tx *sql.Tx, q *queue.Queue, p ProbeParams, typ queue.RequestType, priority int16, payload json.RawMessage, idem string) error {
	_, err := q.EnqueueScheduled(ctx, tx, queue.Request{
		ExchangeID:     p.ExchangeID,
		Symbol:         p.Symbol,
		CycleID:        &p.CycleID,
		OrderID:        &p.OrderID,
		Type:           typ,
		Priority:       priority,
		Payload:        payload,
		IdempotencyKey: idem,
	}, p.Delay)
	if err != nil && !errors.Is(err, queue.ErrDuplicateIdempotencyKey) {
		return err
	}
	return nil
}

// --- resolution of a FOUND recovery probe ---

// RecoverParams is the input to the probe-resolution helpers below.
type RecoverParams struct {
	RequestID          int64 // the probe request (marked SUCCEEDED / FAILED by the resolution)
	OrderID            int64
	CycleID            int64
	ExchangeID         int64
	Symbol             string
	Scope              string // exchange code (for the symbol lock)
	ClientOrderIDSent  string // the EXACT id we sent (stamped so future lookups use it)
	LocalClientOrderID string
}

// TerminalRecovered reports whether a probed order status is TERMINAL (settled on the venue), so
// recovery records fills DIRECTLY rather than continuing an open-order cancel flow. StateRejected
// is terminal too but is handled on its own clean-failure path.
func TerminalRecovered(s execution.NormalizedOrderState) bool {
	switch s {
	case execution.StateFilled, execution.StateCanceled, execution.StatePartiallyCanceled, execution.StateExpired:
		return true
	}
	return false
}

// ackFromStatus synthesizes the OrderAck the normal open-order ack flow needs from a probe.
func ackFromStatus(st execution.OrderStatus) execution.OrderAck {
	return execution.OrderAck{
		ExchangeOrderID: st.ExchangeOrderID, ClientOrderID: st.ClientOrderID, Symbol: st.Symbol,
		Side: st.Side, RequestedQty: st.IntendedQty, AvgPrice: st.AvgPrice, Status: execution.StateOpen,
	}
}

// stampExchangeIDs records the discovered exchange_order_id and the exact client_order_id_sent
// (never overwriting a non-empty value with empty).
func stampExchangeIDs(ctx context.Context, tx *sql.Tx, orderID int64, exoid, sentClientID string) error {
	_, err := tx.ExecContext(ctx,
		"UPDATE orders SET exchange_order_id = COALESCE(NULLIF(?,''), exchange_order_id), client_order_id_sent = COALESCE(NULLIF(?,''), client_order_id_sent) WHERE id = ?",
		exoid, sentClientID, orderID)
	return err
}

// RecoverBuyPlace resolves a FOUND ambiguous-buy-place probe. A rejected order fails cleanly
// (no exposure); a TERMINAL order records its fills DIRECTLY (no redundant cancel/GET_ORDER —
// PR19 round 3 #7); an OPEN order resumes the normal ack flow (which schedules the IOC cancel).
func RecoverBuyPlace(ctx context.Context, tx *sql.Tx, q *queue.Queue, p RecoverParams, st execution.OrderStatus) error {
	if err := stampExchangeIDs(ctx, tx, p.OrderID, st.ExchangeOrderID, p.ClientOrderIDSent); err != nil {
		return err
	}
	switch {
	case st.Status == execution.StateRejected:
		return OnPlaceRejected(ctx, tx, q, PlaceRejectedParams{
			RequestID: p.RequestID, OrderID: p.OrderID, CycleID: p.CycleID,
			Cause: "ambiguous place recovered: exchange REJECTED the order (no exposure)",
		})
	case TerminalRecovered(st.Status):
		if err := advanceCycleTo(ctx, tx, p.CycleID, state.CycleBuySubmitted, "place_recovered", "buy recovered (accepted; place response had timed out)"); err != nil {
			return err
		}
		if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderCancelPending, "place_recovered", "buy recovered to a terminal state via read-only probe"); err != nil {
			return err
		}
		_, err := ProcessFinalStatus(ctx, tx, q, FinalStatusParams{
			RequestID: p.RequestID, OrderID: p.OrderID, CycleID: p.CycleID, Scope: p.Scope,
			Status: st, RawResp: rawJSON(st),
		})
		return err
	default: // open / new / partially-filled-open → normal ack flow, which schedules the IOC cancel
		return OnPlaceAck(ctx, tx, q, PlaceAckParams{
			RequestID: p.RequestID, OrderID: p.OrderID, CycleID: p.CycleID, ExchangeID: p.ExchangeID,
			Symbol: p.Symbol, Ack: ackFromStatus(st), Intent: BuyIntentPayload{LocalClientOrderID: p.LocalClientOrderID},
			RawResp: rawJSON(st),
		})
	}
}

// RecoverSellPlace resolves a FOUND ambiguous-sell-place probe. A rejected/terminal sell holds
// buy-leg inventory, so a rejection goes to NEEDS_RECONCILE (lock held), never a clean fail.
func RecoverSellPlace(ctx context.Context, tx *sql.Tx, q *queue.Queue, p RecoverParams, st execution.OrderStatus) error {
	if err := stampExchangeIDs(ctx, tx, p.OrderID, st.ExchangeOrderID, p.ClientOrderIDSent); err != nil {
		return err
	}
	switch {
	case st.Status == execution.StateRejected:
		return OnSellPlaceRejected(ctx, tx, q, PlaceRejectedParams{
			RequestID: p.RequestID, OrderID: p.OrderID, CycleID: p.CycleID,
			Cause: "ambiguous sell place recovered: exchange rejected — inventory held; needs reconcile",
		})
	case TerminalRecovered(st.Status):
		if err := advanceCycleTo(ctx, tx, p.CycleID, state.CycleSellSubmitted, "sell_place_recovered", "sell recovered (accepted; place response had timed out)"); err != nil {
			return err
		}
		if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderSubmitted, "sell_place_recovered", "sell recovered via read-only probe"); err != nil {
			return err
		}
		if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderAcked, "sell_place_recovered", "sell recovered via read-only probe"); err != nil {
			return err
		}
		_, err := ProcessSellStatus(ctx, tx, q, SellStatusParams{
			RequestID: p.RequestID, OrderID: p.OrderID, CycleID: p.CycleID, Scope: p.Scope,
			Symbol: p.Symbol, Status: st, RawResp: rawJSON(st),
		})
		return err
	default: // open → rest normally
		return OnSellPlaceAck(ctx, tx, q, SellPlaceAckParams{
			RequestID: p.RequestID, OrderID: p.OrderID, CycleID: p.CycleID, Ack: ackFromStatus(st), RawResp: rawJSON(st),
		})
	}
}

// RecoverBuyCancel resolves a FOUND, TERMINAL ambiguous-buy-cancel probe by recording the actual
// fills DIRECTLY (no second GET_ORDER — PR19 round 3 #7). The caller has already confirmed the
// order is NOT still open.
func RecoverBuyCancel(ctx context.Context, tx *sql.Tx, q *queue.Queue, p RecoverParams, st execution.OrderStatus) error {
	if err := advanceOrderTo(ctx, tx, p.OrderID, state.OrderCancelPending, "cancel_recovered", "ambiguous cancel: real state discovered via read-only probe"); err != nil {
		return err
	}
	_, err := ProcessFinalStatus(ctx, tx, q, FinalStatusParams{
		RequestID: p.RequestID, OrderID: p.OrderID, CycleID: p.CycleID, Scope: p.Scope,
		Status: st, RawResp: rawJSON(st),
	})
	return err
}

// RecoverSellCancel resolves a FOUND, TERMINAL ambiguous-sell-cancel probe directly.
func RecoverSellCancel(ctx context.Context, tx *sql.Tx, q *queue.Queue, p RecoverParams, st execution.OrderStatus) error {
	_, err := ProcessSellStatus(ctx, tx, q, SellStatusParams{
		RequestID: p.RequestID, OrderID: p.OrderID, CycleID: p.CycleID, Scope: p.Scope,
		Symbol: p.Symbol, Status: st, RawResp: rawJSON(st),
	})
	return err
}

func rawJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return b
}
