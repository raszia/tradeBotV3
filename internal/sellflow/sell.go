package sellflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
)

// Benign "nothing to do" results (the manager treats these as skip, not failure).
var (
	ErrSellExists     = errors.New("sellflow: an active sell order already exists")
	ErrNotSellable    = errors.New("sellflow: cycle not in a sellable state")
	ErrNothingToSell  = errors.New("sellflow: no remaining inventory to sell")
	ErrBelowMinimum   = errors.New("sellflow: sell quantity/price below venue minimum")
	ErrNotRepriceable = errors.New("sellflow: cycle not in a repriceable state")
	ErrRepriceTooSoon = errors.New("sellflow: reprice interval not elapsed")
	ErrSellOpInFlight = errors.New("sellflow: a sell place/cancel is already in flight")
	ErrNoRestingSell  = errors.New("sellflow: no resting sell order to reprice")
)

const sellPriority int16 = 60

// CreateParams is the input to CreateSell. BinanceRef is the Binance reference price
// already converted into the Iranian quote unit.
type CreateParams struct {
	CycleID       int64
	BinanceRef    decimal.Decimal
	QuoteUnit     string
	ReferenceRate string // for audit (empty for USDT markets)
	ConfigVersion int64
}

// CreateResult reports the created sell.
type CreateResult struct {
	OrderID   int64
	RequestID int64
	Price     decimal.Decimal
	Quantity  decimal.Decimal
}

// CreateSell creates a resting exit sell for the cycle's REMAINING filled inventory
// (bought − already sold), priced below the Binance reference, in ONE transaction:
// insert sell order → cycle →SELL_REQUEST_QUEUED + order NEW→REGISTERED→QUEUED →
// enqueue sell PLACE_ORDER → commit. It NEVER calls an exchange. No-duplicate: it
// refuses when an active sell order already exists for the cycle.
func CreateSell(ctx context.Context, store *db.Store, q *queue.Queue, m configstore.MarketConfig, p CreateParams) (CreateResult, error) {
	if !p.BinanceRef.IsPositive() {
		return CreateResult{}, ErrNotSellable
	}
	var out CreateResult
	err := store.WithTx(ctx, func(tx *sql.Tx) error {
		cyState, cyVer, err := readCycle(ctx, tx, p.CycleID)
		if err != nil {
			return err
		}
		if !sellableState(cyState) {
			return ErrNotSellable
		}
		if n, err := activeSellOrders(ctx, tx, p.CycleID); err != nil {
			return err
		} else if n > 0 {
			return ErrSellExists
		}
		bought := buyFilled(ctx, tx, p.CycleID)
		sold := soldSoFar(ctx, tx, p.CycleID)
		remaining := bought.Sub(sold)
		if !remaining.IsPositive() {
			return ErrNothingToSell
		}
		qty := SnapQtyToStep(remaining, m.StepSize)
		price := SellPrice(p.BinanceRef, m.SellOffsetBps, m.TickSize)
		if !MeetsMinimums(qty, price, m.MinOrderQuantity, m.MinOrderAmount) {
			return ErrBelowMinimum
		}
		out.Price, out.Quantity = price, qty

		seq, err := sellOrderSeq(ctx, tx, p.CycleID)
		if err != nil {
			return err
		}
		seq++
		local := fmt.Sprintf("c%d-sell-%d", p.CycleID, seq)
		res, err := tx.ExecContext(ctx, `
INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, time_in_force, limit_price, quantity)
VALUES (?, ?, ?, 'sell', 'exit_sell', ?, 'NEW', 'limit', NULL, ?, ?)`,
			p.CycleID, m.ExchangeID, m.ExchangeMarketID, local, price.String(), qty.String())
		if err != nil {
			return err
		}
		orderID, _ := res.LastInsertId()
		out.OrderID = orderID

		// cycle -> SELL_REQUEST_QUEUED ; order NEW->REGISTERED->QUEUED.
		if _, err := state.ApplyCycleTransition(ctx, tx, state.CycleTransition{CycleID: p.CycleID, From: cyState, To: state.CycleSellRequestQueued, Version: cyVer, Reason: "exit sell queued"}); err != nil {
			return err
		}
		if _, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{OrderID: orderID, From: state.OrderNew, To: state.OrderRegistered, Version: 0, Reason: "sell registered"}); err != nil {
			return err
		}
		if _, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{OrderID: orderID, From: state.OrderRegistered, To: state.OrderQueued, Version: 1, Reason: "sell queued"}); err != nil {
			return err
		}

		payload, _ := json.Marshal(orders.SellIntentPayload{
			Side: "sell", OrderType: "limit", Price: price.String(), Quantity: qty.String(),
			LocalClientOrderID: local, SellOffsetBps: m.SellOffsetBps, BinanceRef: p.BinanceRef.String(),
			QuoteUnit: p.QuoteUnit, ConfigVersion: p.ConfigVersion,
		})
		reqID, err := q.Enqueue(ctx, tx, queue.Request{
			ExchangeID: m.ExchangeID, Symbol: m.CanonicalSymbol, CycleID: &p.CycleID, OrderID: &orderID,
			Type: queue.TypePlaceOrder, Priority: sellPriority, Payload: payload, TimeoutMS: m.OrderTimeoutMs, MaxRetries: m.MaxRetries,
			IdempotencyKey: fmt.Sprintf("place-sell:c%d:s%d", p.CycleID, seq),
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

// RepriceSell cancels the resting sell (to replace it at a fresh price) when the
// reprice interval has elapsed and no sell place/cancel is already in flight. It only
// CANCELs here; the replacement sell is created by the manager once the cancel +
// final status resolve (so fills before the cancel are accounted first). One tx.
func RepriceSell(ctx context.Context, store *db.Store, q *queue.Queue, m configstore.MarketConfig, cycleID int64, finalCheckDelayMs int) error {
	return store.WithTx(ctx, func(tx *sql.Tx) error {
		cyState, cyVer, err := readCycle(ctx, tx, cycleID)
		if err != nil {
			return err
		}
		if cyState != state.CycleSellSubmitted && cyState != state.CycleSellPartiallyFilled {
			return ErrNotRepriceable
		}
		// Reprice-interval gate.
		var elapsed bool
		interval := m.RepriceIntervalSeconds
		if interval <= 0 {
			interval = 5
		}
		if err := tx.QueryRowContext(ctx,
			"SELECT (last_reprice_at IS NULL OR last_reprice_at < NOW(6) - INTERVAL ? SECOND) FROM cycles WHERE id=?",
			interval, cycleID).Scan(&elapsed); err != nil {
			return err
		}
		if !elapsed {
			return ErrRepriceTooSoon
		}
		// Don't reprice while a sell place/cancel is CLAIMED/IN_FLIGHT.
		if busy, err := sellOpInFlight(ctx, tx, cycleID); err != nil {
			return err
		} else if busy {
			return ErrSellOpInFlight
		}
		// The resting sell order (ACKED/SUBMITTED/PARTIALLY_FILLED).
		ordID, ordState, ordVer, exoid, ok, err := restingSell(ctx, tx, cycleID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNoRestingSell
		}
		// PR11 #7: a resting sell with no usable exchange_order_id cannot be cancelled — never
		// enqueue CancelOrder(""). Push order+cycle to NEEDS_RECONCILE and KEEP the lock; the
		// reconciler determines the real exchange state. (No blind cancel.)
		if exoid == "" {
			return orders.MarkNeedsReconcile(ctx, tx, ordID, &cycleID,
				"resting sell has no exchange_order_id — cannot reprice/cancel; needs reconcile")
		}

		if _, err := state.ApplyCycleTransition(ctx, tx, state.CycleTransition{CycleID: cycleID, From: cyState, To: state.CycleSellRepricePending, Version: cyVer, Reason: "repricing exit sell"}); err != nil {
			return err
		}
		if _, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{OrderID: ordID, From: ordState, To: state.OrderCancelPending, Version: ordVer, Reason: "cancel for reprice"}); err != nil {
			return err
		}
		payload, _ := json.Marshal(orders.FollowupPayload{ExchangeOrderID: exoid, Purpose: orders.PurposeSellReprice, CycleID: cycleID})
		if _, err := q.Enqueue(ctx, tx, queue.Request{
			ExchangeID: m.ExchangeID, Symbol: m.CanonicalSymbol, CycleID: &cycleID, OrderID: &ordID,
			Type: queue.TypeCancelOrder, Priority: sellPriority, Payload: payload, TimeoutMS: m.OrderTimeoutMs, MaxRetries: m.MaxRetries,
			IdempotencyKey: fmt.Sprintf("cancel-sell:o%d", ordID),
		}); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE cycles SET last_reprice_at=NOW(6) WHERE id=?", cycleID)
		return err
	})
}

// ---- query helpers ----

func sellableState(s state.CycleState) bool {
	return s == state.CycleBuyFilled || s == state.CycleBuyPartiallyFilled || s == state.CycleSellRepricePending
}

func readCycle(ctx context.Context, tx *sql.Tx, id int64) (state.CycleState, int64, error) {
	var s string
	var v int64
	err := tx.QueryRowContext(ctx, "SELECT state, version FROM cycles WHERE id=?", id).Scan(&s, &v)
	return state.CycleState(s), v, err
}

func buyFilled(ctx context.Context, tx *sql.Tx, cycleID int64) decimal.Decimal {
	var s sql.NullString
	_ = tx.QueryRowContext(ctx, "SELECT filled_quantity FROM orders WHERE cycle_id=? AND role='entry_buy' LIMIT 1", cycleID).Scan(&s)
	return decOrZero(s)
}

func soldSoFar(ctx context.Context, tx *sql.Tx, cycleID int64) decimal.Decimal {
	var s sql.NullString
	_ = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(filled_quantity),0) FROM orders WHERE cycle_id=? AND role='exit_sell'", cycleID).Scan(&s)
	return decOrZero(s)
}

// activeSellOrders counts exit_sell orders that are not in a terminal state.
func activeSellOrders(ctx context.Context, tx *sql.Tx, cycleID int64) (int, error) {
	var n int
	err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM orders WHERE cycle_id=? AND role='exit_sell' AND state NOT IN ('FILLED','CANCELLED','REJECTED','EXPIRED','FAILED','NEEDS_RECONCILE')",
		cycleID).Scan(&n)
	return n, err
}

func sellOrderSeq(ctx context.Context, tx *sql.Tx, cycleID int64) (int, error) {
	var n int
	err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders WHERE cycle_id=? AND role='exit_sell'", cycleID).Scan(&n)
	return n, err
}

// sellOpInFlight reports whether a sell PLACE/CANCEL is CLAIMED or IN_FLIGHT.
func sellOpInFlight(ctx context.Context, tx *sql.Tx, cycleID int64) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_requests WHERE cycle_id=? AND request_type IN ('PLACE_ORDER','CANCEL_ORDER') AND status IN ('CLAIMED','IN_FLIGHT')",
		cycleID).Scan(&n)
	return n > 0, err
}

// restingSell returns the active resting sell order to reprice.
func restingSell(ctx context.Context, tx *sql.Tx, cycleID int64) (id int64, st state.OrderState, ver int64, exoid string, ok bool, err error) {
	var s string
	var e sql.NullString
	row := tx.QueryRowContext(ctx,
		"SELECT id, state, version, COALESCE(exchange_order_id,'') FROM orders WHERE cycle_id=? AND role='exit_sell' AND state IN ('SUBMITTED','ACKED','PARTIALLY_FILLED') ORDER BY id DESC LIMIT 1 FOR UPDATE",
		cycleID)
	if err = row.Scan(&id, &s, &ver, &e); errors.Is(err, sql.ErrNoRows) {
		return 0, "", 0, "", false, nil
	}
	if err != nil {
		return 0, "", 0, "", false, err
	}
	return id, state.OrderState(s), ver, e.String, true, nil
}

func decOrZero(s sql.NullString) decimal.Decimal {
	if !s.Valid {
		return decimal.Zero
	}
	d, err := decimal.NewFromString(s.String)
	if err != nil {
		return decimal.Zero
	}
	return d
}
