package sellflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
)

// RefPrice supplies the Binance reference price for a market, already converted into
// the Iranian quote unit (the same conversion the buy signal uses). ok is false when
// the price is missing/stale (the manager then skips price-dependent actions).
type RefPrice func(m configstore.MarketConfig) (binanceRef decimal.Decimal, quoteUnit, referenceRate string, ok bool)

// Manager drives sell-side management: it scans open sell cycles and, per cycle,
// creates the exit sell, polls its status, and reprices it — all by writing DB rows
// and queue requests. It NEVER calls an exchange (the executor sends; internal/orders
// processes results).
type Manager struct {
	store    *db.Store
	q        *queue.Queue
	cache    *configstore.Cache
	refPrice RefPrice
	clk      clock.Clock
	log      *slog.Logger
}

// NewManager builds a sell Manager.
func NewManager(store *db.Store, q *queue.Queue, cache *configstore.Cache, refPrice RefPrice, clk clock.Clock, log *slog.Logger) *Manager {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Manager{store: store, q: q, cache: cache, refPrice: refPrice, clk: clk, log: log}
}

// openSellStates are the cycle states the manager acts on.
var openSellStates = []string{"BUY_FILLED", "BUY_PARTIALLY_FILLED", "SELL_SUBMITTED", "SELL_PARTIALLY_FILLED", "SELL_REPRICE_PENDING"}

// Pass runs one management sweep over all open sell cycles. A single cycle's failure
// never stops the sweep.
func (mgr *Manager) Pass(ctx context.Context) error {
	rows, err := mgr.store.DB().QueryContext(ctx,
		"SELECT id, exchange_market_id, state FROM cycles WHERE state IN ('BUY_FILLED','BUY_PARTIALLY_FILLED','SELL_SUBMITTED','SELL_PARTIALLY_FILLED','SELL_REPRICE_PENDING')")
	if err != nil {
		return err
	}
	type cyc struct {
		id    int64
		emID  int64
		state string
	}
	var cycles []cyc
	for rows.Next() {
		var c cyc
		if err := rows.Scan(&c.id, &c.emID, &c.state); err != nil {
			rows.Close()
			return err
		}
		cycles = append(cycles, c)
	}
	rows.Close()

	snap := mgr.cache.Snapshot()
	for _, c := range cycles {
		m, ok := snap.Market(c.emID)
		if !ok || !m.EnabledForSellManage {
			continue // sell-manage disabled or unknown market: leave it alone
		}
		if err := mgr.manageCycle(ctx, m, c.id, c.state); err != nil {
			mgr.log.Warn("manage sell cycle failed", "cycle", c.id, "state", c.state, "err", err)
		}
	}
	return nil
}

func (mgr *Manager) manageCycle(ctx context.Context, m configstore.MarketConfig, cycleID int64, st string) error {
	switch st {
	case "BUY_FILLED", "BUY_PARTIALLY_FILLED":
		return mgr.create(ctx, m, cycleID)
	case "SELL_REPRICE_PENDING":
		// If the reprice cancel has resolved (no active sell order) create the
		// replacement for the remaining inventory; otherwise wait for the executor.
		n, err := activeSellOrdersDB(ctx, mgr.store.DB(), cycleID)
		if err != nil {
			return err
		}
		if n == 0 {
			return mgr.create(ctx, m, cycleID)
		}
		return nil
	case "SELL_SUBMITTED", "SELL_PARTIALLY_FILLED":
		if err := mgr.ensurePoll(ctx, cycleID); err != nil {
			return err
		}
		return mgr.reprice(ctx, m, cycleID)
	}
	return nil
}

// create makes (or replaces) the exit sell using the current reference price.
func (mgr *Manager) create(ctx context.Context, m configstore.MarketConfig, cycleID int64) error {
	ref, quote, rate, ok := mgr.refPrice(m)
	if !ok {
		return nil // no fresh price -> can't price the sell; try again next pass
	}
	_, err := CreateSell(ctx, mgr.store, mgr.q, m, CreateParams{
		CycleID: cycleID, BinanceRef: ref, QuoteUnit: quote, ReferenceRate: rate, ConfigVersion: m.SymbolConfigVersion,
	})
	if isBenign(err) {
		return nil
	}
	return err
}

// reprice attempts an interval-gated reprice (cancel of the resting sell).
func (mgr *Manager) reprice(ctx context.Context, m configstore.MarketConfig, cycleID int64) error {
	if _, _, _, ok := mgr.refPrice(m); !ok {
		return nil
	}
	err := RepriceSell(ctx, mgr.store, mgr.q, m, cycleID, 500)
	if isBenign(err) {
		return nil
	}
	return err
}

// ensurePoll enqueues a single outstanding sell-status GET_ORDER for the resting
// sell, so its fills are observed. It is gated to at most one in-flight poll.
func (mgr *Manager) ensurePoll(ctx context.Context, cycleID int64) error {
	var n int
	if err := mgr.store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_requests WHERE cycle_id=? AND request_type='GET_ORDER' AND status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT')",
		cycleID).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil // a poll is already pending
	}
	// Find the resting sell order to poll.
	var orderID, exID int64
	var exoid string
	var symbol string
	err := mgr.store.DB().QueryRowContext(ctx,
		"SELECT o.id, o.exchange_id, COALESCE(o.exchange_order_id,''), o.exchange_market_id FROM orders o WHERE o.cycle_id=? AND o.role='exit_sell' AND o.state IN ('SUBMITTED','ACKED','PARTIALLY_FILLED') ORDER BY o.id DESC LIMIT 1",
		cycleID).Scan(&orderID, &exID, &exoid, new(int64))
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	// PR11: never poll with an empty exchange_order_id (no blind GetOrder("")). A resting sell
	// with no usable id is untrackable → NEEDS_RECONCILE, lock held (OnSellPlaceAck normally
	// prevents this reaching a resting state; this is defense-in-depth).
	if exoid == "" {
		return mgr.store.WithTx(ctx, func(tx *sql.Tx) error {
			return orders.MarkNeedsReconcile(ctx, tx, orderID, &cycleID,
				"resting sell has no exchange_order_id — cannot poll status; needs reconcile")
		})
	}
	_ = mgr.store.DB().QueryRowContext(ctx, "SELECT canonical_symbol FROM cycles WHERE id=?", cycleID).Scan(&symbol)
	payload, _ := json.Marshal(orders.FollowupPayload{ExchangeOrderID: exoid, Purpose: orders.PurposeSellStatus, CycleID: cycleID})
	return mgr.store.WithTx(ctx, func(tx *sql.Tx) error {
		_, e := mgr.q.Enqueue(ctx, tx, queue.Request{
			ExchangeID: exID, Symbol: symbol, CycleID: &cycleID, OrderID: &orderID,
			Type: queue.TypeGetOrder, Priority: 70, Payload: payload,
			IdempotencyKey: fmt.Sprintf("sell-poll:o%d:%d", orderID, mgr.clk.Now().Unix()),
		})
		if errors.Is(e, queue.ErrDuplicateIdempotencyKey) {
			return nil
		}
		return e
	})
}

func activeSellOrdersDB(ctx context.Context, db *sql.DB, cycleID int64) (int, error) {
	var n int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM orders WHERE cycle_id=? AND role='exit_sell' AND state NOT IN ('FILLED','CANCELLED','REJECTED','EXPIRED','FAILED','NEEDS_RECONCILE')",
		cycleID).Scan(&n)
	return n, err
}

// isBenign reports whether a sellflow error is an expected "nothing to do" outcome.
func isBenign(err error) bool {
	switch {
	case err == nil,
		errors.Is(err, ErrSellExists), errors.Is(err, ErrNotSellable), errors.Is(err, ErrNothingToSell),
		errors.Is(err, ErrBelowMinimum), errors.Is(err, ErrNotRepriceable), errors.Is(err, ErrRepriceTooSoon),
		errors.Is(err, ErrSellOpInFlight), errors.Is(err, ErrNoRestingSell):
		return true
	}
	return false
}
