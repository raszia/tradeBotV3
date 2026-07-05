// Package executor is the order-executor: the ONLY component that issues
// order-mutating exchange API calls. It is driven SOLELY by the database-backed
// queue (internal/queue) — there is intentionally no public method or CLI path
// that sends an order directly (rule #1). The flow is always:
//
//	DB request row → Claim → (MarkInFlight for mutating) → send via adapter →
//	record final queue status (+ order state, atomically where applicable).
//
// Safety guard (rule #2): mutating requests (PLACE/CANCEL) are processed only
// when Config.AllowLiveExecution is true. It defaults to false, so a development
// service can never accidentally place a real order by inserting a DB row — it
// simply does not claim mutating request types. Tests set AllowLiveExecution=true
// and inject FAKE PrivateClients (never a real exchange).
package executor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/live"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
)

// Config tunes the executor.
type Config struct {
	// Name identifies this executor instance (stored as claimed_by).
	Name string
	// AllowLiveExecution gates MUTATING request processing. Default false: the
	// executor only claims read-only request types until live execution is
	// explicitly enabled (and credentials/real clients are wired in a later PR).
	AllowLiveExecution bool
	// PollInterval is the claim cadence (default 250ms).
	PollInterval time.Duration
	// SweepEvery runs the stuck-IN_FLIGHT sweeper every N poll ticks (default 40).
	SweepEvery int
	// LimitFor returns the per-exchange concurrency limit for an exchange code.
	// Defaults to 1 if nil.
	LimitFor func(exchangeCode string) int
	// FinalStatusDelay is the grace before the simulated-IOC final GET_ORDER status
	// check is claimable after a successful cancel (default 500ms).
	FinalStatusDelay time.Duration
	// ExecutionMode is "off" | "dry_run" | "live". The live Guard is consulted ONLY in
	// "live" mode (dry-run uses simexec clients with no real exposure).
	ExecutionMode string
	// Guard is the limited-live safety gate (PR20). In live mode it is the FINAL check
	// before any real mutating send; nil disables the gate (non-live modes).
	Guard *live.Guard
	// faultAfterSend is a TEST-ONLY fault-injection hook (PR26). When non-nil it is invoked
	// at the START of every post-send completion transaction; returning an error forces that
	// transaction to roll back AFTER the exchange send has already happened — exercising the
	// crash/rollback recovery paths (request stays IN_FLIGHT → sweeper → DEAD + reconcile,
	// never re-sent). It is NEVER set in production (the field is unexported, so only
	// same-package tests can set it).
	faultAfterSend func() error
}

// Executor claims and processes exchange requests for a set of private clients.
type Executor struct {
	store   *db.Store
	q       *queue.Queue
	clients map[string]exchanges.PrivateClient // exchange code -> client
	exIDs   map[string]int64                   // exchange code -> id
	log     *slog.Logger
	cfg     Config
}

// New builds an Executor. clients maps exchange code -> PrivateClient (fakes in
// tests). With no clients it has nothing to do and idles.
func New(store *db.Store, q *queue.Queue, clients map[string]exchanges.PrivateClient, log *slog.Logger, cfg Config) *Executor {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.SweepEvery <= 0 {
		cfg.SweepEvery = 40
	}
	if cfg.LimitFor == nil {
		cfg.LimitFor = func(string) int { return 1 }
	}
	if cfg.FinalStatusDelay <= 0 {
		cfg.FinalStatusDelay = 500 * time.Millisecond
	}
	return &Executor{store: store, q: q, clients: clients, exIDs: map[string]int64{}, log: log, cfg: cfg}
}

// allowedTypes returns the request types this executor will claim. Mutating types
// are claimed only when live execution is enabled.
func (e *Executor) allowedTypes() []queue.RequestType {
	if e.cfg.AllowLiveExecution {
		return queue.AllTypes
	}
	return queue.ReadOnlyTypes
}

// Run resolves exchange ids, then claims + processes requests until ctx is
// cancelled.
func (e *Executor) Run(ctx context.Context) error {
	if err := e.resolveExchangeIDs(ctx); err != nil {
		return err
	}
	if e.log != nil {
		e.log.Info("order-executor starting", "exchanges", len(e.clients),
			"allow_live_execution", e.cfg.AllowLiveExecution)
	}
	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()
	tick := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.claimAndProcessAll(ctx)
			tick++
			if tick%e.cfg.SweepEvery == 0 {
				if _, err := e.q.SweepStuck(ctx, 5); err != nil && e.log != nil {
					e.log.Warn("sweep stuck failed", "err", err)
				}
			}
		}
	}
}

func (e *Executor) resolveExchangeIDs(ctx context.Context) error {
	for code := range e.clients {
		var id int64
		err := e.store.DB().QueryRowContext(ctx, "SELECT id FROM exchanges WHERE code = ?", code).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			continue // unknown exchange; skip (its requests simply won't be claimed)
		}
		if err != nil {
			return err
		}
		e.exIDs[code] = id
	}
	return nil
}

func (e *Executor) claimAndProcessAll(ctx context.Context) {
	allowed := e.allowedTypes()
	for code := range e.clients {
		exID, ok := e.exIDs[code]
		if !ok {
			continue
		}
		limit := e.cfg.LimitFor(code)
		claimed, err := e.q.Claim(ctx, exID, e.cfg.Name, limit, allowed)
		if err != nil {
			if e.log != nil {
				e.log.Warn("claim failed", "exchange", code, "err", err)
			}
			continue
		}
		for _, c := range claimed {
			e.process(ctx, c)
		}
	}
}

// process dispatches one claimed request to its handler.
func (e *Executor) process(ctx context.Context, c queue.Claimed) {
	client := e.clients[c.ExchangeCode]
	if client == nil {
		e.failTx(ctx, c.ID, "no client configured for exchange "+c.ExchangeCode)
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, time.Duration(c.Timeout())*time.Millisecond)
	defer cancel()

	switch c.Type {
	case queue.TypeGetBalance:
		bals, err := client.GetBalances(sendCtx)
		e.handleReadOnly(ctx, c, mustJSON(bals), err)
	case queue.TypeGetOrder:
		var fp orders.FollowupPayload
		_ = json.Unmarshal(c.Payload, &fp)
		// Order-tied GET_ORDERs drive fill processing (buy final status / sell status);
		// any other GET_ORDER is read-only.
		if c.OrderID != nil && c.CycleID != nil {
			switch fp.Purpose {
			case orders.PurposeFinalStatus:
				e.handleFinalStatus(ctx, sendCtx, c, fp, client)
				return
			case orders.PurposeSellStatus:
				e.handleSellStatus(ctx, sendCtx, c, fp, client)
				return
			}
		}
		st, err := client.GetOrder(sendCtx, fp.ExchangeOrderID)
		e.handleReadOnly(ctx, c, mustJSON(st), err)
	case queue.TypeGetOpenOrders:
		var p struct {
			Symbol string `json:"symbol"`
		}
		_ = json.Unmarshal(c.Payload, &p)
		list, err := client.GetOpenOrders(sendCtx, p.Symbol)
		e.handleReadOnly(ctx, c, mustJSON(list), err)
	case queue.TypePlaceOrder:
		e.dispatchPlace(ctx, sendCtx, c, client)
	case queue.TypeCancelOrder:
		var fp orders.FollowupPayload
		_ = json.Unmarshal(c.Payload, &fp)
		if fp.Purpose == orders.PurposeSellReprice {
			e.handleSellCancel(ctx, sendCtx, c, fp, client)
		} else {
			e.handleCancel(ctx, sendCtx, c, client)
		}
	default:
		e.failTx(ctx, c.ID, "unknown request type "+string(c.Type))
	}
}

// handleReadOnly records a read-only request's outcome. Read-only requests are
// idempotent, so a retryable error simply reschedules.
func (e *Executor) handleReadOnly(ctx context.Context, c queue.Claimed, resp json.RawMessage, sendErr error) {
	if sendErr == nil {
		_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
			return e.q.MarkSucceeded(ctx, tx, c.ID, resp)
		})
		return
	}
	if isRetryable(sendErr) {
		if _, err := e.q.ScheduleRetry(ctx, c.ID, sendErr.Error()); err != nil && e.log != nil {
			e.log.Warn("schedule retry failed", "id", c.ID, "err", err)
		}
		return
	}
	e.failTx(ctx, c.ID, sendErr.Error())
}

// dispatchPlace routes a PLACE_ORDER to the buy or sell handler using the DATABASE ORDER as
// the source of truth (its role), NEVER the untrusted payload.side text (PR10 #2). A payload
// that lies about its side therefore cannot bypass the intended handler/validation: a buy DB
// order always goes to the buy handler, whose Validate() then rejects `side != buy`.
func (e *Executor) dispatchPlace(ctx, sendCtx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	if c.OrderID == nil || c.CycleID == nil {
		// No DB order to consult or resolve — request-only failure.
		e.failTx(ctx, c.ID, "PLACE_ORDER missing order/cycle context")
		return
	}
	role, err := e.orderRole(ctx, *c.OrderID)
	if err != nil {
		// Cannot read the DB order — do NOT guess a handler. Fail the request (a missing
		// order row cannot be transitioned; the reconciler/operator investigates).
		e.failTx(ctx, c.ID, "PLACE_ORDER: cannot read order role: "+err.Error())
		return
	}
	switch role {
	case "entry_buy":
		e.handlePlace(ctx, sendCtx, c, client)
	case "exit_sell":
		e.handleSellPlace(ctx, sendCtx, c, client)
	default:
		e.failTx(ctx, c.ID, "PLACE_ORDER: unroutable order role "+role)
	}
}

// orderRole reads the trusted role (entry_buy | exit_sell) of the DB order.
func (e *Executor) orderRole(ctx context.Context, orderID int64) (string, error) {
	var role string
	err := e.store.DB().QueryRowContext(ctx, "SELECT role FROM orders WHERE id=?", orderID).Scan(&role)
	return role, err
}

// rejectBuyCleanly resolves a buy PLACE_ORDER that must not be sent (undecodable/invalid
// payload) through the official rejection path: request FAILED, order + cycle FAILED, symbol
// lock released — no direct state-table writes, and never leaves order/cycle/lock stranded.
func (e *Executor) rejectBuyCleanly(ctx context.Context, c queue.Claimed, cause string) {
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		return orders.OnPlaceRejected(ctx, tx, e.q, orders.PlaceRejectedParams{
			RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, Cause: cause,
		})
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("clean buy rejection tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
}

// rejectSellToReconcile handles a sell PLACE_ORDER that must not be sent (undecodable/invalid
// payload). A sell has existing inventory, so unlike a buy it is NOT failed+released: the
// request is FAILED and the order + cycle go to NEEDS_RECONCILE with the lock HELD (via the
// official state path), for an operator/reconciler to resolve the inventory. Nothing is sent.
func (e *Executor) rejectSellToReconcile(ctx context.Context, c queue.Claimed, cause string) {
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if err := e.q.MarkFailed(ctx, tx, c.ID, cause); err != nil {
			return err
		}
		return orders.MarkNeedsReconcile(ctx, tx, *c.OrderID, c.CycleID, cause)
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("sell reject-to-reconcile tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
}

// handlePlace processes a buy PLACE_ORDER. It marks IN_FLIGHT (committed) BEFORE
// sending so a crash is recoverable, then records the outcome conservatively and
// (on success) schedules the simulated-IOC cancel via internal/orders.
func (e *Executor) handlePlace(ctx, sendCtx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	// The dispatcher guarantees order/cycle context (it routed here by the DB order role),
	// but re-guard defensively: without it we can only fail the request.
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, "PLACE_ORDER missing order/cycle context")
		return
	}
	// PR10 #1/#5: a payload that cannot be DECODED, or an intent that fails validation
	// (bad/zero price/qty, wrong side/type/non-IOC, empty client id), must NEVER be sent
	// AND must not strand the order/cycle/lock. Both resolve cleanly via OnPlaceRejected
	// (request FAILED, order + cycle FAILED, lock released) — nothing was placed, so there
	// is no exposure. This is also what defends a wrong-side payload on a buy order: the DB
	// role routed it here, and Validate() rejects `side != buy`.
	intent, err := orders.ParseBuyIntent(c.Payload)
	if err != nil {
		e.rejectBuyCleanly(ctx, c, "undecodable buy PLACE_ORDER payload: "+err.Error())
		return
	}
	if verr := intent.Validate(); verr != nil {
		e.rejectBuyCleanly(ctx, c, "invalid buy payload (not sent): "+verr.Error())
		return
	}
	// FINAL live gate (PR20): before any real buy send, the guard must allow it.
	price := decimalOrZero(intent.IntendedPrice)
	qty := decimalOrZero(intent.IntendedQuantity)
	if !e.liveGatePlace(ctx, c, "buy", price.Mul(qty), qty) {
		return // denied + audited + request failed by the gate
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		if e.log != nil {
			e.log.Warn("mark in-flight failed; not sending", "id", c.ID, "err", err)
		}
		return
	}

	ack, err := client.PlaceOrder(sendCtx, intent.OrderRequest(c.Symbol))
	if err == nil {
		e.completePlaceSuccess(ctx, c, ack, intent)
		return
	}
	// Conservative outcome classification (rule #6): a definite rejection means
	// the order was NOT placed (no exposure) — fail the request AND cleanly resolve
	// the order/cycle + release the lock. Anything ambiguous (timeout, network, 5xx,
	// unknown) must NOT be re-sent — DEAD + order/cycle NEEDS_RECONCILE.
	if isDefiniteRejection(err) {
		txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
			return orders.OnPlaceRejected(ctx, tx, e.q, orders.PlaceRejectedParams{
				RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, Cause: "place rejected (not placed): " + err.Error(),
			})
		})
		if txErr != nil && e.log != nil {
			e.log.Warn("place-rejected tx failed (rolled back)", "id", c.ID, "err", txErr)
		}
		return
	}
	e.deadReconcile(ctx, c, "place ambiguous outcome: "+err.Error())
}

// completionFault returns the test-only post-send fault (nil in production). Placed at the
// start of every post-send completion tx so a forced error rolls the tx back AFTER the send.
func (e *Executor) completionFault() error {
	if e.cfg.faultAfterSend != nil {
		return e.cfg.faultAfterSend()
	}
	return nil
}

// completePlaceSuccess records the PLACE result and schedules the cancel of the
// remainder (simulated IOC) in ONE transaction (rule #9), via orders.OnPlaceAck.
func (e *Executor) completePlaceSuccess(ctx context.Context, c queue.Claimed, ack execution.OrderAck, intent orders.BuyIntentPayload) {
	err := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.completionFault(); ferr != nil {
			return ferr
		}
		return orders.OnPlaceAck(ctx, tx, e.q, orders.PlaceAckParams{
			RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, ExchangeID: c.ExchangeID,
			Symbol: c.Symbol, Ack: ack, Intent: intent, RawResp: mustJSON(ack),
		})
	})
	if err != nil && e.log != nil {
		// The whole tx rolled back; the request stays IN_FLIGHT and will be picked
		// up by the sweeper rather than wrongly marked succeeded.
		e.log.Warn("place completion tx failed (rolled back)", "id", c.ID, "err", err)
	}
}

// handleCancel processes the simulated-IOC CANCEL_ORDER. On a clean (or definite)
// cancel it schedules the final GET_ORDER status check; an AMBIGUOUS cancel
// (timeout/network) goes to NEEDS_RECONCILE (never guess whether it took).
func (e *Executor) handleCancel(ctx, sendCtx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	var fp orders.FollowupPayload
	if err := json.Unmarshal(c.Payload, &fp); err != nil {
		e.failTx(ctx, c.ID, "bad CANCEL_ORDER payload: "+err.Error())
		return
	}
	// FINAL SAFETY BOUNDARY (PR11): never call CancelOrder(""). With order/cycle context →
	// DEAD + NEEDS_RECONCILE (lock held); otherwise a request-only failure. Not sent.
	if fp.ExchangeOrderID == "" {
		e.deadReconcile(ctx, c, "CANCEL_ORDER has empty exchange_order_id — not sent; needs reconcile")
		return
	}
	if !e.liveGateCancel(ctx, c) {
		return
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		return
	}
	err := client.CancelOrder(sendCtx, fp.ExchangeOrderID)
	// A clean cancel OR a definite rejection (e.g. order already gone/filled) both
	// resolve via the final GET_ORDER — the cancel is best-effort, the status is
	// authoritative. Only an ambiguous cancel (we cannot tell if it took) is unsafe.
	if err == nil || isDefiniteRejection(err) {
		if c.OrderID == nil || c.CycleID == nil {
			_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
				return e.q.MarkSucceeded(ctx, tx, c.ID, mustJSON(map[string]string{"cancelled": fp.ExchangeOrderID}))
			})
			return
		}
		txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
			if ferr := e.completionFault(); ferr != nil {
				return ferr
			}
			return orders.OnCancelResult(ctx, tx, e.q, orders.CancelResultParams{
				RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, ExchangeID: c.ExchangeID,
				Symbol: c.Symbol, ExchangeOrderID: fp.ExchangeOrderID, LocalClientID: fp.LocalClientOrderID,
				RawResp: mustJSON(map[string]any{"cancelled": fp.ExchangeOrderID, "rejected": err != nil}), FinalCheckDelay: e.cfg.FinalStatusDelay,
			})
		})
		if txErr != nil && e.log != nil {
			e.log.Warn("cancel completion tx failed (rolled back)", "id", c.ID, "err", txErr)
		}
		return
	}
	e.deadReconcile(ctx, c, "cancel ambiguous outcome: "+err.Error())
}

// handleFinalStatus fetches the order's final status and processes the fills +
// classification via internal/orders. A transient (retryable) fetch error simply
// reschedules the read; ErrOrderUnknown / a missing order is NOT proof of zero fill
// — it is processed as ambiguous → NEEDS_RECONCILE.
func (e *Executor) handleFinalStatus(ctx, sendCtx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	// FINAL SAFETY BOUNDARY (PR11): never call GetOrder("") — not fetched → DEAD +
	// NEEDS_RECONCILE (lock held).
	if fp.ExchangeOrderID == "" {
		e.deadReconcile(ctx, c, "GET_ORDER has empty exchange_order_id — not fetched; needs reconcile")
		return
	}
	st, err := client.GetOrder(sendCtx, fp.ExchangeOrderID)
	if err != nil && !errors.Is(err, execution.ErrOrderUnknown) && isRetryable(err) {
		if _, sErr := e.q.ScheduleRetry(ctx, c.ID, err.Error()); sErr != nil && e.log != nil {
			e.log.Warn("schedule final-status retry failed", "id", c.ID, "err", sErr)
		}
		return
	}
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		_, perr := orders.ProcessFinalStatus(ctx, tx, e.q, orders.FinalStatusParams{
			RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, Scope: c.ExchangeCode,
			Status: st, StatusErr: err, RawResp: mustJSON(st),
		})
		return perr
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("final-status processing tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
}

// handleSellPlace sends a resting exit sell (no auto-cancel scheduled) and records
// the ack via orders.OnSellPlaceAck. Same conservative classification as the buy:
// definite rejection → clean fail; ambiguous → DEAD + NEEDS_RECONCILE, never re-sent.
func (e *Executor) handleSellPlace(ctx, sendCtx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, "sell PLACE_ORDER missing order/cycle context")
		return
	}
	// PR10 #2: a sell payload that cannot be decoded or fails validation (bad side/type/
	// zero price/qty/empty client id) must NEVER be sent. Unlike a buy rejection, a sell has
	// existing inventory to protect, so it is NOT cleanly failed+released — the request is
	// FAILED and the order/cycle go to NEEDS_RECONCILE with the lock HELD (an operator/
	// reconciler resolves the inventory). This also defends a wrong-side payload on a sell order.
	intent, err := orders.ParseSellIntent(c.Payload)
	if err != nil {
		e.rejectSellToReconcile(ctx, c, "undecodable sell PLACE_ORDER payload: "+err.Error())
		return
	}
	if verr := intent.Validate(); verr != nil {
		e.rejectSellToReconcile(ctx, c, "invalid sell payload (not sent): "+verr.Error())
		return
	}
	price := decimalOrZero(intent.Price)
	qty := decimalOrZero(intent.Quantity)
	if !e.liveGatePlace(ctx, c, "sell", price.Mul(qty), qty) {
		return // denied + audited + request failed by the gate
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		return
	}
	ack, err := client.PlaceOrder(sendCtx, intent.OrderRequest(c.Symbol))
	if err == nil {
		txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
			return orders.OnSellPlaceAck(ctx, tx, e.q, orders.SellPlaceAckParams{
				RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, Ack: ack, RawResp: mustJSON(ack),
			})
		})
		if txErr != nil && e.log != nil {
			e.log.Warn("sell place completion tx failed (rolled back)", "id", c.ID, "err", txErr)
		}
		return
	}
	if isDefiniteRejection(err) {
		// A definite SELL rejection is NOT a buy rejection: we still hold inventory from the
		// buy leg, so DO NOT release the lock / FAIL the cycle. Request FAILED, order + cycle
		// NEEDS_RECONCILE, lock HELD (OnSellPlaceRejected). Reconciliation decides the next step.
		_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
			return orders.OnSellPlaceRejected(ctx, tx, e.q, orders.PlaceRejectedParams{
				RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, Cause: "sell place rejected: " + err.Error(),
			})
		})
		return
	}
	e.deadReconcile(ctx, c, "sell place ambiguous outcome: "+err.Error())
}

// handleSellCancel sends a reprice CANCEL and records it via orders.OnSellCancelResult
// (which schedules the final sell-status read). Ambiguous cancel → NEEDS_RECONCILE.
func (e *Executor) handleSellCancel(ctx, sendCtx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, "sell CANCEL_ORDER missing order/cycle context")
		return
	}
	// FINAL SAFETY BOUNDARY (PR11): never call CancelOrder("") — an empty exchange_order_id is
	// unusable. Do not send, do not MarkInFlight: request FAILED, order+cycle NEEDS_RECONCILE,
	// lock HELD (inventory). Reconciliation determines the real exchange state.
	if fp.ExchangeOrderID == "" {
		e.rejectSellToReconcile(ctx, c, "sell CANCEL_ORDER has empty exchange_order_id — not sent; needs reconcile")
		return
	}
	if !e.liveGateCancel(ctx, c) {
		return
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		return
	}
	err := client.CancelOrder(sendCtx, fp.ExchangeOrderID)
	if err == nil || isDefiniteRejection(err) {
		txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
			if ferr := e.completionFault(); ferr != nil {
				return ferr
			}
			return orders.OnSellCancelResult(ctx, tx, e.q, orders.SellCancelParams{
				RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, ExchangeID: c.ExchangeID,
				Symbol: c.Symbol, ExchangeOrderID: fp.ExchangeOrderID,
				RawResp: mustJSON(map[string]any{"cancelled": fp.ExchangeOrderID, "rejected": err != nil}), FinalCheckDelay: e.cfg.FinalStatusDelay,
			})
		})
		if txErr != nil && e.log != nil {
			e.log.Warn("sell cancel completion tx failed (rolled back)", "id", c.ID, "err", txErr)
		}
		return
	}
	e.deadReconcile(ctx, c, "sell cancel ambiguous outcome: "+err.Error())
}

// handleSellStatus reads a resting/cancelled sell's status and processes its fills via
// orders.ProcessSellStatus. Transient errors reschedule; a missing order / definitive
// error is processed as ambiguous → NEEDS_RECONCILE.
func (e *Executor) handleSellStatus(ctx, sendCtx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, "sell GET_ORDER missing order/cycle context")
		return
	}
	// FINAL SAFETY BOUNDARY (PR11): never call GetOrder("") — an empty exchange_order_id is
	// unusable. Do not send: request FAILED, order+cycle NEEDS_RECONCILE, lock HELD.
	if fp.ExchangeOrderID == "" {
		e.rejectSellToReconcile(ctx, c, "sell GET_ORDER has empty exchange_order_id — not sent; needs reconcile")
		return
	}
	st, err := client.GetOrder(sendCtx, fp.ExchangeOrderID)
	if err != nil && !errors.Is(err, execution.ErrOrderUnknown) && isRetryable(err) {
		if _, sErr := e.q.ScheduleRetry(ctx, c.ID, err.Error()); sErr != nil && e.log != nil {
			e.log.Warn("schedule sell-status retry failed", "id", c.ID, "err", sErr)
		}
		return
	}
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		_, perr := orders.ProcessSellStatus(ctx, tx, e.q, orders.SellStatusParams{
			RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, Scope: c.ExchangeCode,
			Symbol: c.Symbol, Status: st, StatusErr: err, RawResp: mustJSON(st),
		})
		return perr
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("sell-status processing tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
}

// deadReconcile marks a mutating request DEAD and pushes its order AND cycle to
// NEEDS_RECONCILE in one transaction. Used for ambiguous send/cancel outcomes.
func (e *Executor) deadReconcile(ctx context.Context, c queue.Claimed, cause string) {
	_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if c.OrderID != nil {
			if err := orders.MarkNeedsReconcile(ctx, tx, *c.OrderID, c.CycleID, cause); err != nil {
				return err
			}
		}
		return e.q.MarkDead(ctx, tx, c.ID, cause)
	})
}

func (e *Executor) failTx(ctx context.Context, id int64, cause string) {
	_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
		return e.q.MarkFailed(ctx, tx, id, cause)
	})
}

// --- error classification ---

// isRetryable reports whether a read-only error warrants a retry. context
// cancellation/deadline are retryable so a request interrupted by shutdown
// reschedules (read-only requests are idempotent) instead of being lost.
func isRetryable(err error) bool {
	if errors.Is(err, execution.ErrRateLimited) || errors.Is(err, execution.ErrAckTimeout) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var apiErr *exchanges.NormalizedAPIError
	if errors.As(err, &apiErr) {
		return apiErr.Retryable || apiErr.Category == exchanges.CatRateLimit ||
			apiErr.Category == exchanges.CatServer || apiErr.Category == exchanges.CatTimeout ||
			apiErr.Category == exchanges.CatNetwork
	}
	return false
}

// isDefiniteRejection reports whether a mutating-send error means the order was
// DEFINITELY not placed (so the request can safely be FAILED rather than treated
// as ambiguous). Conservative: only clear client-side rejections qualify.
func isDefiniteRejection(err error) bool {
	if errors.Is(err, execution.ErrInsufficientBalance) || errors.Is(err, execution.ErrAuthFailed) {
		return true
	}
	var apiErr *exchanges.NormalizedAPIError
	if errors.As(err, &apiErr) {
		switch apiErr.Category {
		case exchanges.CatBadRequest, exchanges.CatAuth, exchanges.CatInsufficientBalance:
			return true
		}
	}
	return false
}

// liveGatePlace is the FINAL live safety gate before a real PLACE send. In non-live
// modes (off/dry_run) it is a no-op (returns true). In live mode it consults the Guard
// (caps/kill-switch/live-flags/credentials/dry-run/state); a denial fails the request
// (audited) and returns false so the send is skipped.
func (e *Executor) liveGatePlace(ctx context.Context, c queue.Claimed, side string, notional, baseQty decimal.Decimal) bool {
	if e.cfg.ExecutionMode != "live" || e.cfg.Guard == nil {
		return true
	}
	pc := live.PlaceCheck{
		ExchangeID: c.ExchangeID, ExchangeMarketID: e.orderMarket(ctx, c.OrderID), ExchangeCode: c.ExchangeCode,
		Symbol: c.Symbol, CycleID: derefID(c.CycleID), OrderID: derefID(c.OrderID), RequestID: c.ID,
		Side: side, Notional: notional, BaseQty: baseQty, DryRun: e.cycleDryRun(ctx, c.CycleID),
	}
	d := e.cfg.Guard.CheckPlace(ctx, pc)
	if !d.Allow {
		e.failTx(ctx, c.ID, "live guard denied: "+d.Reason)
		return false
	}
	// PR24: just before the first real BUY of the canary session is sent, record the final
	// pre-send checklist snapshot (idempotent; no secrets). Best-effort — never blocks.
	if side == "buy" {
		e.cfg.Guard.RecordFirstOrderChecklist(ctx, pc)
	}
	return true
}

// liveGateCancel gates a real CANCEL send (cancels reduce risk; permitted under the
// kill switch but still requires live mode + credentials + not dry-run).
func (e *Executor) liveGateCancel(ctx context.Context, c queue.Claimed) bool {
	if e.cfg.ExecutionMode != "live" || e.cfg.Guard == nil {
		return true
	}
	d := e.cfg.Guard.CheckCancel(ctx, live.PlaceCheck{
		ExchangeID: c.ExchangeID, CycleID: derefID(c.CycleID), OrderID: derefID(c.OrderID), RequestID: c.ID,
		DryRun: e.cycleDryRun(ctx, c.CycleID),
	})
	if !d.Allow {
		e.failTx(ctx, c.ID, "live guard denied cancel: "+d.Reason)
		return false
	}
	return true
}

func (e *Executor) orderMarket(ctx context.Context, orderID *int64) int64 {
	if orderID == nil {
		return 0
	}
	var em int64
	e.store.DB().QueryRowContext(ctx, "SELECT exchange_market_id FROM orders WHERE id=?", *orderID).Scan(&em)
	return em
}

func (e *Executor) cycleDryRun(ctx context.Context, cycleID *int64) bool {
	if cycleID == nil {
		return false
	}
	var dry int
	e.store.DB().QueryRowContext(ctx, "SELECT dry_run FROM cycles WHERE id=?", *cycleID).Scan(&dry)
	return dry != 0
}

func derefID(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func decimalOrZero(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero
	}
	return d
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(fmt.Sprintf("{\"marshal_error\":%q}", err.Error()))
	}
	return b
}
