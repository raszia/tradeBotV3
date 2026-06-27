package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
)

// §2a — one active pending buy-intent per (exchange-market / symbol / strategy
// scope). The scope key here is exchange_market_id (which encodes exchange +
// canonical symbol; there is a single strategy today). Signal spam must never
// create competing buy requests, so when a newer signal arrives the engine
// UPDATES the existing not-yet-claimed QUEUED buy request rather than enqueueing a
// duplicate, and REMOVES it (before it is sent) when the signal is no longer
// valid.
//
// IMPORTANT (PR8 boundary): the engine does NOT create buy requests — that is PR9
// (cycle + order + request creation, transactional, under the symbol lock). These
// helpers only ever touch a request whose status is still QUEUED. A CLAIMED /
// IN_FLIGHT / already-sent request is never modified here (the SELECT ... FOR
// UPDATE + the `status='QUEUED'` guard make that safe even against a concurrent
// claimer) — those are left to the executor/reconciler.

// pendingBuySelectForUpdate locks and returns the newest not-yet-claimed QUEUED
// buy (PLACE_ORDER, entry_buy) request for an exchange-market scope. FOR UPDATE
// prevents a concurrent claimer from grabbing the row between SELECT and the
// UPDATE/DELETE below.
const pendingBuySelectForUpdate = `
SELECT er.id
FROM exchange_requests er
JOIN orders o ON o.id = er.order_id
WHERE o.exchange_market_id = ?
  AND er.request_type = 'PLACE_ORDER'
  AND o.role = 'entry_buy'
  AND er.status = 'QUEUED'
ORDER BY er.id DESC
LIMIT 1
FOR UPDATE`

// UpdatePendingBuyRequest refreshes the payload of an existing not-yet-claimed
// QUEUED buy request for the scope, so a newer valid signal supersedes the stale
// intent without creating a duplicate. Returns true iff a QUEUED request was
// updated. No-op (false, nil) when none exists — PR8 never creates one.
func UpdatePendingBuyRequest(ctx context.Context, store *db.Store, exchangeMarketID int64, payload []byte) (bool, error) {
	var updated bool
	err := store.WithTx(ctx, func(tx *sql.Tx) error {
		reqID, ok, err := lockPendingBuy(ctx, tx, exchangeMarketID)
		if err != nil || !ok {
			return err
		}
		// The status guard is redundant given FOR UPDATE above but kept as defence
		// in depth: never mutate a request that has left QUEUED.
		res, err := tx.ExecContext(ctx,
			"UPDATE exchange_requests SET payload = ? WHERE id = ? AND status = 'QUEUED'",
			payload, reqID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		updated = n > 0
		return nil
	})
	return updated, err
}

// RemovePendingBuyRequest deletes an existing not-yet-claimed QUEUED buy request
// for the scope (the signal that justified it is no longer valid, and it has not
// been sent, so there is no exchange exposure). Returns true iff one was removed.
// CLAIMED/IN_FLIGHT/sent requests are never removed.
func RemovePendingBuyRequest(ctx context.Context, store *db.Store, exchangeMarketID int64) (bool, error) {
	var removed bool
	err := store.WithTx(ctx, func(tx *sql.Tx) error {
		reqID, ok, err := lockPendingBuy(ctx, tx, exchangeMarketID)
		if err != nil || !ok {
			return err
		}
		res, err := tx.ExecContext(ctx,
			"DELETE FROM exchange_requests WHERE id = ? AND status = 'QUEUED'", reqID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		removed = n > 0
		return nil
	})
	return removed, err
}

// lockPendingBuy SELECT ... FOR UPDATE the scope's QUEUED buy request id.
func lockPendingBuy(ctx context.Context, tx *sql.Tx, exchangeMarketID int64) (int64, bool, error) {
	var reqID int64
	err := tx.QueryRowContext(ctx, pendingBuySelectForUpdate, exchangeMarketID).Scan(&reqID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return reqID, true, nil
}

// intentPayload is the refreshed buy-intent recorded on the QUEUED request's
// payload. It carries enough price/config context for later audit. The actual
// simulated-IOC execution parameters (wait/cancel) are added when PR9/PR10 build
// the full request; PR8 only refreshes the price/quantity/config snapshot.
type intentPayload struct {
	SignalID         int64  `json:"signal_id"`
	ExchangeMarketID int64  `json:"exchange_market_id"`
	CanonicalSymbol  string `json:"canonical_symbol"`
	Side             string `json:"side"`
	IntendedBuyPrice string `json:"intended_buy_price"`
	BuySize          string `json:"buy_size"`
	BuySizeUnit      string `json:"buy_size_unit"`
	ConfigVersion    int64  `json:"config_version"`
}

// newIntentPayload builds the refreshed-intent JSON for an UPDATE.
func newIntentPayload(m configstore.MarketConfig, price decimal.Decimal, configVersion, signalID int64) []byte {
	b, _ := json.Marshal(intentPayload{
		SignalID:         signalID,
		ExchangeMarketID: m.ExchangeMarketID,
		CanonicalSymbol:  m.CanonicalSymbol,
		Side:             "buy",
		IntendedBuyPrice: price.String(),
		BuySize:          m.BuySize.String(),
		BuySizeUnit:      m.BuySizeUnit,
		ConfigVersion:    configVersion,
	})
	return b
}
