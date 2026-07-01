package buyflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
	"v3TradeBot/internal/symbollock"
)

// ErrSymbolLocked is re-exported so engine callers can treat "scope already has an
// active cycle" as a normal no-op (not a failure).
var ErrSymbolLocked = symbollock.ErrSymbolLocked

// buyPriority is the queue priority for entry-buy PLACE_ORDER requests (lower =
// more urgent). Buys are time-sensitive, so they outrank routine GET_BALANCE polls.
const buyPriority int16 = 50

// SignalContext is the per-comparison context stamped onto the cycle/order and the
// request payload for audit. Prices are already in the Iranian quote unit.
type SignalContext struct {
	ConfigVersion  int64
	BinancePrice   decimal.Decimal // Binance reference, converted into the Iranian quote
	IranianPrice   decimal.Decimal // the Iranian best ask (our buy price)
	SpreadBps      int
	FeeAdjustedBps int
	QuoteUnit      string
	ReferenceRate  *decimal.Decimal // nil for USDT-quoted markets
	BuyFeeBps      int
	SellFeeBps     int
	DryRun         bool // PR19: stamp the cycle as a dry-run (simulated) cycle
	// SignalID is the signals row that produced this intent. When non-zero it is linked
	// to the created/refreshed cycle (signals.cycle_id) IN THE SAME transaction, so the
	// signal → cycle audit trail is complete. Zero means "do not link" (e.g. tests).
	SignalID int64
}

// CreateResult reports the rows created by CreateBuyCycle.
type CreateResult struct {
	CycleID   int64
	OrderID   int64
	LockID    int64
	RequestID int64
	Decision  Decision
	Quantity  decimal.Decimal
}

// The PLACE_ORDER request payload is the shared orders.BuyIntentPayload (defined in
// internal/orders so the order-executor/processor can parse the same contract).

// CreateBuyCycle creates the buy side of a new cycle in ONE transaction:
// cycle → symbol lock → buy order → state-machine transitions → PLACE_ORDER enqueue.
// It returns ErrSymbolLocked (and creates nothing) when the scope already has an
// active cycle. It NEVER calls an exchange and NEVER sends the order.
func CreateBuyCycle(ctx context.Context, store *db.Store, q *queue.Queue, m configstore.MarketConfig, ask decimal.Decimal, sig SignalContext, leaseSeconds int) (CreateResult, error) {
	var out CreateResult
	if !ask.IsPositive() {
		return out, errors.New("buyflow: non-positive ask")
	}
	err := store.WithTx(ctx, func(tx *sql.Tx) error {
		// 1. Continue the per-scope maker/taker attempt counter from the most recent
		//    cycle within the rolling window (resets when the window has expired). The
		//    symbol lock guarantees one creator per scope at a time, so this read
		//    cannot race for a scope.
		prior, windowStart, err := priorAttemptAndWindow(ctx, tx, m.ExchangeMarketID, m.Maker.MakerSignalWindowSeconds)
		if err != nil {
			return err
		}

		// 2. Pure maker/taker decision + resolve the base quantity. Reject a non-positive
		//    price or quantity here so a bad intent is NEVER persisted/enqueued (it could
		//    otherwise be sent blindly). Final market-rule snapping (tick/step/min) is the
		//    executor's job before send (see §2a / the market-rule boundary note).
		dec := Decide(m.Maker, ask, prior)
		if !dec.LimitPrice.IsPositive() {
			return fmt.Errorf("buyflow: non-positive limit price %s (ask=%s offset=%dbps)", dec.LimitPrice, ask, dec.OffsetBps)
		}
		qty := resolveQuantity(m.BuySize, m.BuySizeUnit, dec.LimitPrice)
		if !qty.IsPositive() {
			return fmt.Errorf("buyflow: non-positive quantity (buy_size=%s unit=%s)", m.BuySize, m.BuySizeUnit)
		}
		out.Decision, out.Quantity = dec, qty

		// 3. Insert the cycle (NEW) with config + signal + execution-mode context.
		cycleID, err := insertCycle(ctx, tx, m, sig, dec, qty, windowStart)
		if err != nil {
			return err
		}
		out.CycleID = cycleID

		// 3b. Link the originating signal to this cycle (audit trail) in the same tx.
		if err := linkSignalToCycle(ctx, tx, sig.SignalID, cycleID); err != nil {
			return err
		}

		// 4. Acquire the symbol lock — THE one-active-intent gate. Duplicate scope ⇒
		//    ErrSymbolLocked ⇒ the whole tx rolls back (no orphan cycle).
		lockID, err := symbollock.Acquire(ctx, tx, m.ExchangeCode, m.CanonicalSymbol, cycleID, leaseSeconds)
		if err != nil {
			return err
		}
		out.LockID = lockID

		// 5. Insert the buy order (NEW). TIF stays NULL — native IOC is never forced.
		localCOID := fmt.Sprintf("c%d-buy", cycleID)
		orderID, err := insertOrder(ctx, tx, m, cycleID, dec, qty, ask, localCOID)
		if err != nil {
			return err
		}
		out.OrderID = orderID

		// 6. Advance states via the state machine (events written in the same tx).
		if err := advanceCycleToQueued(ctx, tx, cycleID); err != nil {
			return err
		}
		if err := advanceOrderToQueued(ctx, tx, orderID); err != nil {
			return err
		}

		// 7. Enqueue the PLACE_ORDER request (status QUEUED) with the full payload.
		payload := buildPayload(m, dec, qty, ask, sig, localCOID)
		reqID, err := q.Enqueue(ctx, tx, queue.Request{
			ExchangeID:     m.ExchangeID,
			Symbol:         m.CanonicalSymbol,
			CycleID:        &cycleID,
			OrderID:        &orderID,
			Type:           queue.TypePlaceOrder,
			Priority:       buyPriority,
			Payload:        payload,
			TimeoutMS:      m.OrderTimeoutMs,
			MaxRetries:     m.MaxRetries,
			IdempotencyKey: fmt.Sprintf("place-order:c%d:buy", cycleID),
		})
		if err != nil {
			return err
		}
		out.RequestID = reqID
		return nil
	})
	if err != nil {
		return CreateResult{}, err
	}
	return out, nil
}

// RefreshActiveCycleBuy supersedes the active cycle's still-QUEUED (unsent) buy
// request with a newer valid signal WITHOUT creating a duplicate. A refresh counts
// as another opportunity in the window (§2a): it ADVANCES the maker attempt counter,
// re-runs the maker/taker decision, and so can escalate the SAME request maker →
// taker once maker_attempts_before_taker is reached. If the window has expired the
// counter resets to maker-first. Returns true iff a QUEUED request was refreshed.
// CLAIMED/IN_FLIGHT/sent requests are never touched, and a cycle-tied request is
// never deleted.
func RefreshActiveCycleBuy(ctx context.Context, store *db.Store, m configstore.MarketConfig, ask decimal.Decimal, sig SignalContext) (bool, error) {
	if !ask.IsPositive() {
		return false, nil
	}
	window := m.Maker.MakerSignalWindowSeconds
	if window <= 0 {
		window = 60
	}
	var refreshed bool
	err := store.WithTx(ctx, func(tx *sql.Tx) error {
		var (
			reqID, orderID, cycleID int64
			curAttempt              int
			localCOID               string
			expired                 sql.NullBool
		)
		// Select + FOR UPDATE lock the active cycle's buy intent ONLY when the WHOLE
		// execution state is still queued and active — lock ACTIVE, cycle
		// BUY_REQUEST_QUEUED, order QUEUED, request QUEUED. If ANY has moved on (request
		// CLAIMED/IN_FLIGHT, order/cycle advanced, lock released) no row matches and we do
		// NOT refresh (a claimer may already be sending it). FOR UPDATE locks the joined
		// rows so nothing changes between here and the guarded UPDATEs below.
		err := tx.QueryRowContext(ctx, `
SELECT er.id, o.id, c.id, c.maker_attempt_number, o.local_client_order_id,
       (c.opportunity_window_started_at < NOW(6) - INTERVAL ? SECOND)
FROM symbol_locks sl
JOIN cycles c  ON c.id = sl.cycle_id
JOIN orders o  ON o.cycle_id = c.id AND o.role = 'entry_buy'
JOIN exchange_requests er ON er.order_id = o.id AND er.request_type = 'PLACE_ORDER'
WHERE sl.state = 'ACTIVE' AND sl.scope = ? AND sl.canonical_symbol = ?
  AND c.state = 'BUY_REQUEST_QUEUED' AND o.state = 'QUEUED' AND er.status = 'QUEUED'
ORDER BY er.id DESC
LIMIT 1
FOR UPDATE`, window, m.ExchangeCode, m.CanonicalSymbol).Scan(&reqID, &orderID, &cycleID, &curAttempt, &localCOID, &expired)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // nothing fully-queued to refresh (guarded out)
		}
		if err != nil {
			return err
		}

		// Each refresh is another opportunity: advance the counter (reset on window
		// expiry) and re-decide. priorAttempts feeds Decide, which computes the new
		// attempt number = priorAttempts + 1.
		windowExpired := expired.Valid && expired.Bool
		prior := curAttempt
		if windowExpired {
			prior = 0
		}
		dec := Decide(m.Maker, ask, prior)
		qty := resolveQuantity(m.BuySize, m.BuySizeUnit, dec.LimitPrice)
		// Never refresh into a non-positive price/quantity (misconfig) — leave the prior
		// valid intent in place rather than persist something unsendable.
		if !dec.LimitPrice.IsPositive() || !qty.IsPositive() {
			return nil
		}

		// The cycle, order, and request must describe the SAME current intent, so all three
		// are refreshed together (one tx). Each UPDATE is guarded on its expected state and
		// its RowsAffected verified — a 0-row update means the state changed under us and
		// rolls the whole refresh back.
		//
		// (a) Cycle: refresh the FULL signal snapshot (not just mode/attempt) so the cycle
		//     audit matches the latest signal that superseded the intent.
		res, err := tx.ExecContext(ctx, `
UPDATE cycles SET
  signal_time = NOW(6),
  binance_price_at_signal = ?, iranian_price_at_signal = ?,
  spread_bps = ?, fee_adjusted_spread_bps = ?, buy_size = ?, config_version = ?,
  intended_execution_mode = ?, maker_attempt_number = ?,
  opportunity_window_started_at = CASE WHEN ? THEN NOW(6) ELSE opportunity_window_started_at END
WHERE id = ? AND state = 'BUY_REQUEST_QUEUED'`,
			sig.BinancePrice.String(), sig.IranianPrice.String(),
			sig.SpreadBps, sig.FeeAdjustedBps, qty.String(), nullID(sig.ConfigVersion),
			string(dec.Mode), dec.AttemptNumber, windowExpired, cycleID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("buyflow: cycle %d not BUY_REQUEST_QUEUED at refresh (rows=%d)", cycleID, n)
		}

		// (b) Order: refresh price/qty/mode (not a state change), guarded on QUEUED.
		res, err = tx.ExecContext(ctx,
			"UPDATE orders SET limit_price = ?, quantity = ?, ask_price_at_decision = ?, intended_execution_mode = ?, maker_attempt_number = ?, maker_offset_bps = ? WHERE id = ? AND state = 'QUEUED'",
			dec.LimitPrice.String(), qty.String(), ask.String(), string(dec.Mode), dec.AttemptNumber, dec.OffsetBps, orderID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("buyflow: order %d not QUEUED at refresh (rows=%d)", orderID, n)
		}

		// (c) Request payload, guarded on status='QUEUED'.
		payload := buildPayload(m, dec, qty, ask, sig, localCOID)
		res, err = tx.ExecContext(ctx,
			"UPDATE exchange_requests SET payload = ? WHERE id = ? AND status = 'QUEUED'", payload, reqID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("buyflow: request %d not QUEUED at refresh (rows=%d)", reqID, n)
		}

		// (d) Link the superseding signal to this same cycle (audit).
		if err := linkSignalToCycle(ctx, tx, sig.SignalID, cycleID); err != nil {
			return err
		}
		refreshed = true
		return nil
	})
	return refreshed, err
}

// linkSignalToCycle stamps signals.cycle_id for the originating signal (best-effort audit
// link) within the caller's tx. A zero signalID is a no-op. It only fills a NULL cycle_id so
// a re-run never repoints an already-linked signal.
func linkSignalToCycle(ctx context.Context, tx *sql.Tx, signalID, cycleID int64) error {
	if signalID == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		"UPDATE signals SET cycle_id = ? WHERE id = ? AND cycle_id IS NULL", cycleID, signalID)
	return err
}

// ---- helpers ----

// priorAttemptAndWindow returns the maker/taker attempt count to continue from for a
// new cycle on this scope, plus the window-start to carry. It looks at the most
// recent cycle for the scope: if its opportunity window is still open, the new cycle
// continues the count (carrying the window start); if the window has expired (or
// there is no prior cycle) the count resets to 0 and the window restarts at NOW.
// This shares one counter with RefreshActiveCycleBuy so escalation is continuous
// whether repeated signals hit a still-QUEUED cycle (refresh) or a fresh cycle
// created after a prior attempt released the lock.
func priorAttemptAndWindow(ctx context.Context, tx *sql.Tx, exchangeMarketID int64, windowSeconds int) (int, sql.NullTime, error) {
	if windowSeconds <= 0 {
		windowSeconds = 60
	}
	var (
		attempt     sql.NullInt64
		windowStart sql.NullTime
		expired     sql.NullBool
	)
	err := tx.QueryRowContext(ctx, `
SELECT maker_attempt_number, opportunity_window_started_at,
       (opportunity_window_started_at < NOW(6) - INTERVAL ? SECOND)
FROM cycles
WHERE exchange_market_id = ?
ORDER BY id DESC
LIMIT 1`, windowSeconds, exchangeMarketID).Scan(&attempt, &windowStart, &expired)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, sql.NullTime{}, nil // first attempt ever for this scope
	}
	if err != nil {
		return 0, sql.NullTime{}, err
	}
	if !windowStart.Valid || (expired.Valid && expired.Bool) {
		return 0, sql.NullTime{}, nil // window expired -> reset to maker-first
	}
	return int(attempt.Int64), windowStart, nil
}

func insertCycle(ctx context.Context, tx *sql.Tx, m configstore.MarketConfig, sig SignalContext, dec Decision, qty decimal.Decimal, windowStart sql.NullTime) (int64, error) {
	// windowStart is NULL for the first attempt -> anchor the window at NOW(6).
	res, err := tx.ExecContext(ctx, `
INSERT INTO cycles
  (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run, config_version,
   signal_time, binance_price_at_signal, iranian_price_at_signal, spread_bps, fee_adjusted_spread_bps, buy_size,
   intended_execution_mode, maker_attempt_number, opportunity_window_started_at)
VALUES (?, ?, ?, 'NEW', ?, ?, NOW(6), ?, ?, ?, ?, ?, ?, ?, COALESCE(?, NOW(6)))`,
		m.ExchangeMarketID, m.ExchangeID, m.CanonicalSymbol, b2iCycle(sig.DryRun), nullID(sig.ConfigVersion),
		sig.BinancePrice.String(), sig.IranianPrice.String(), sig.SpreadBps, sig.FeeAdjustedBps, qty.String(),
		string(dec.Mode), dec.AttemptNumber, windowStart)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func b2iCycle(b bool) int {
	if b {
		return 1
	}
	return 0
}

func insertOrder(ctx context.Context, tx *sql.Tx, m configstore.MarketConfig, cycleID int64, dec Decision, qty, ask decimal.Decimal, localCOID string) (int64, error) {
	res, err := tx.ExecContext(ctx, `
INSERT INTO orders
  (cycle_id, exchange_id, exchange_market_id, side, role, intended_execution_mode, maker_attempt_number, maker_offset_bps, ask_price_at_decision,
   local_client_order_id, state, order_type, time_in_force, limit_price, quantity)
VALUES (?, ?, ?, 'buy', 'entry_buy', ?, ?, ?, ?, ?, 'NEW', 'limit', NULL, ?, ?)`,
		cycleID, m.ExchangeID, m.ExchangeMarketID, string(dec.Mode), dec.AttemptNumber, dec.OffsetBps, ask.String(),
		localCOID, dec.LimitPrice.String(), qty.String())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func advanceCycleToQueued(ctx context.Context, tx *sql.Tx, cycleID int64) error {
	if _, err := state.ApplyCycleTransition(ctx, tx, state.CycleTransition{
		CycleID: cycleID, From: state.CycleNew, To: state.CycleSignalDetected, Version: 0, Reason: "signal accepted",
	}); err != nil {
		return err
	}
	_, err := state.ApplyCycleTransition(ctx, tx, state.CycleTransition{
		CycleID: cycleID, From: state.CycleSignalDetected, To: state.CycleBuyRequestQueued, Version: 1, Reason: "buy request queued",
	})
	return err
}

func advanceOrderToQueued(ctx context.Context, tx *sql.Tx, orderID int64) error {
	if _, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{
		OrderID: orderID, From: state.OrderNew, To: state.OrderRegistered, Version: 0, Reason: "order registered",
	}); err != nil {
		return err
	}
	_, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{
		OrderID: orderID, From: state.OrderRegistered, To: state.OrderQueued, Version: 1, Reason: "order queued",
	})
	return err
}

func buildPayload(m configstore.MarketConfig, dec Decision, qty, ask decimal.Decimal, sig SignalContext, localCOID string) json.RawMessage {
	p := orders.BuyIntentPayload{
		ExecutionMode:            string(dec.Mode),
		Side:                     "buy",
		OrderType:                "limit",
		SimulatedIOC:             true,
		IntendedPrice:            dec.LimitPrice.String(),
		IntendedQuantity:         qty.String(),
		BuySizeUnit:              m.BuySizeUnit,
		MakerAttemptNumber:       dec.AttemptNumber,
		MakerAttemptsBeforeTaker: m.Maker.MakerAttemptsBeforeTaker,
		MakerOffsetBps:           dec.OffsetBps,
		MakerWaitBeforeCancelMs:  m.Maker.MakerWaitBeforeCancelMs,
		CancelAfterWait:          true,
		FinalStatusCheckRequired: true,
		TakerPriceMode:           m.Maker.TakerPriceMode,
		MaxTakerSlippageBps:      m.Maker.MaxTakerSlippageBps,
		AskPriceAtDecision:       ask.String(),
		SignalBinancePrice:       sig.BinancePrice.String(),
		SignalIranianPrice:       sig.IranianPrice.String(),
		QuoteUnit:                sig.QuoteUnit,
		BuyFeeBps:                sig.BuyFeeBps,
		SellFeeBps:               sig.SellFeeBps,
		ConfigVersion:            sig.ConfigVersion,
		LocalClientOrderID:       localCOID,
	}
	if sig.ReferenceRate != nil {
		p.ReferenceRate = sig.ReferenceRate.String()
	}
	b, _ := json.Marshal(p)
	return b
}

// resolveQuantity converts the configured buy size into a base-asset quantity.
// "base" sizes are used directly; "quote" sizes are divided by the limit price.
func resolveQuantity(size decimal.Decimal, unit string, price decimal.Decimal) decimal.Decimal {
	if strings.EqualFold(unit, "quote") {
		if !price.IsPositive() {
			return decimal.Zero
		}
		return size.Div(price)
	}
	return size
}

func nullID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}
