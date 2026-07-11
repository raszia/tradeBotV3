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
	"math/rand/v2"
	"strings"
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
	// cycleModeFault is a TEST-ONLY hook (PR19 round 2) that forces the cycle-mode lookup
	// (cycleDryRun) to fail, so the fail-closed final live guard can be exercised: an executor
	// that cannot confirm a cycle's mode must send NOTHING and leave the request recoverable.
	// Unexported — only same-package tests can set it.
	cycleModeFault func() error
	// Recovery is the default ambiguous-mutation recovery window (bounded read-only probes with
	// exponential backoff + jitter). RecoveryPerExchange overrides it per exchange code (venues
	// with slower order visibility need a longer window). Zero values are defaulted in New().
	Recovery            RecoveryConfig
	RecoveryPerExchange map[string]RecoveryConfig
}

// RecoveryConfig bounds the read-only recovery of an ambiguous mutation timeout. Recovery keeps
// probing (never resending the mutation) until it resolves the order OR the window is exhausted by
// EITHER MaxAttempts or TotalTimeout — after which the order goes to NEEDS_RECONCILE with the lock
// HELD. Backoff is exponential from InitialDelay, capped at MaxDelay, with jitter.
type RecoveryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	TotalTimeout time.Duration
}

func (rc RecoveryConfig) withDefaults() RecoveryConfig {
	if rc.MaxAttempts <= 0 {
		rc.MaxAttempts = 6
	}
	if rc.InitialDelay <= 0 {
		rc.InitialDelay = 1 * time.Second
	}
	if rc.MaxDelay <= 0 {
		rc.MaxDelay = 30 * time.Second
	}
	if rc.TotalTimeout <= 0 {
		rc.TotalTimeout = 5 * time.Minute
	}
	return rc
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
	cfg.Recovery = cfg.Recovery.withDefaults()
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
				// PR19 round 3 #3: FIRST turn any stale mutating IN_FLIGHT (crash after the exchange
				// accepted the send, before the response was handled) into a persisted READ-ONLY
				// recovery probe — never a blind resend. Then the queue sweeper handles the rest
				// (stale CLAIMED requeue, read-only reschedule, and any mutating row we could not
				// convert falls back to its conservative DEAD + NEEDS_RECONCILE).
				e.recoverStaleMutating(ctx, 5)
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
		claimed, err := e.q.Claim(ctx, exID, e.cfg.Name, limit, allowed, e.claimDryRunFilter())
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
			case orders.PurposeAmbiguousPlaceProbe:
				e.handleAmbiguousPlaceProbe(ctx, sendCtx, c, fp, client)
				return
			case orders.PurposeAmbiguousCancelProbe:
				e.handleAmbiguousCancelProbe(ctx, sendCtx, c, fp, client)
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
	if e.abortOnModeMismatch(ctx, c) {
		return
	}
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
	// PR19 round 3 #4: persist the EXACT client id we are about to send (adapter-normalized) and
	// COMMIT it BEFORE the network call, so a timeout/crash recovery looks the order up by exactly
	// what the exchange received. If we cannot persist it, we do NOT send (fail closed).
	sentCOID := e.clientOrderIDForSend(client, intent.LocalClientOrderID)
	if err := e.persistClientOrderIDSent(ctx, *c.OrderID, sentCOID); err != nil {
		if e.log != nil {
			e.log.Warn("could not persist client_order_id_sent; not sending", "id", c.ID, "err", err)
		}
		return
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		if e.log != nil {
			e.log.Warn("mark in-flight failed; not sending", "id", c.ID, "err", err)
		}
		return
	}

	req := intent.OrderRequest(c.Symbol)
	req.ClientOrderID = sentCOID // send EXACTLY the persisted id
	ack, err := client.PlaceOrder(sendCtx, req)
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
	// AMBIGUOUS (timeout/network/5xx/unknown): the order may or may not have been placed. Never
	// blindly re-send. Schedule a READ-ONLY recovery probe that looks the order up by
	// client_order_id (the exchange id was never returned) and continues the state machine.
	e.recoverAmbiguousPlace(ctx, c, intent.LocalClientOrderID, err)
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
	if e.abortOnModeMismatch(ctx, c) {
		return
	}
	// PR19 round 3 #6: a mutating request without a cycle + order cannot be classified dry-run vs
	// live nor recovered — FAIL CLOSED before any send (no exchange/simexec call).
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, "CANCEL_ORDER missing cycle/order context — not sent (fail closed)")
		return
	}
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
	// AMBIGUOUS cancel: we cannot tell whether it took. Never re-send blindly. Schedule a
	// READ-ONLY recovery probe (GET_ORDER by the known exchange_order_id) that determines the
	// real state (open/canceled/partial/filled), records the ACTUAL filled qty, and continues.
	e.recoverAmbiguousCancel(ctx, c, fp, err)
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
	if e.abortOnModeMismatch(ctx, c) {
		return
	}
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
	// PR19 round 3 #4: persist the EXACT sent client id before the network call (see handlePlace).
	sentCOID := e.clientOrderIDForSend(client, intent.LocalClientOrderID)
	if err := e.persistClientOrderIDSent(ctx, *c.OrderID, sentCOID); err != nil {
		if e.log != nil {
			e.log.Warn("could not persist sell client_order_id_sent; not sending", "id", c.ID, "err", err)
		}
		return
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		return
	}
	sreq := intent.OrderRequest(c.Symbol)
	sreq.ClientOrderID = sentCOID
	ack, err := client.PlaceOrder(sendCtx, sreq)
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
	// AMBIGUOUS sell place: same read-only recovery. If the probe proves the sell was NOT placed,
	// it resolves to NEEDS_RECONCILE with the lock HELD (inventory), never a clean fail+release.
	e.recoverAmbiguousPlace(ctx, c, intent.LocalClientOrderID, err)
}

// handleSellCancel sends a reprice CANCEL and records it via orders.OnSellCancelResult
// (which schedules the final sell-status read). Ambiguous cancel → NEEDS_RECONCILE.
func (e *Executor) handleSellCancel(ctx, sendCtx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	if e.abortOnModeMismatch(ctx, c) {
		return
	}
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
	// AMBIGUOUS sell reprice-cancel: same read-only recovery via ProcessSellStatus.
	e.recoverAmbiguousCancel(ctx, c, fp, err)
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

// --- PR19 round 2/3/4: automatic read-only recovery of ambiguous mutation timeouts ---

// nowMillis is the wall clock in unix millis (recovery-window bookkeeping; not determinism-critical).
func (e *Executor) nowMillis() int64 { return time.Now().UnixMilli() }

// firstOrNow keeps an existing first-probe timestamp, or stamps `now` when none was set yet.
func firstOrNow(existing, now int64) int64 {
	if existing > 0 {
		return existing
	}
	return now
}

// recoverAmbiguousPlace turns an ambiguous PLACE timeout into a read-only recovery: it
// schedules a GET_ORDER probe (looked up by client_order_id, since the exchange id was never
// returned) and DEAD-letters the place request (consumed — never blindly re-sent). The order
// stays QUEUED for the probe to resolve. If scheduling fails, it falls back to NEEDS_RECONCILE.
func (e *Executor) recoverAmbiguousPlace(ctx context.Context, c queue.Claimed, localClientOrderID string, cause error) {
	if c.OrderID == nil || c.CycleID == nil {
		e.deadReconcile(ctx, c, "ambiguous place (no order/cycle context): "+cause.Error())
		return
	}
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if err := orders.SchedulePlaceProbe(ctx, tx, e.q, orders.ProbeParams{
			ExchangeID: c.ExchangeID, Symbol: c.Symbol, CycleID: *c.CycleID, OrderID: *c.OrderID,
			LocalClientOrderID: localClientOrderID, FirstProbeAt: e.nowMillis(), Delay: e.cfg.FinalStatusDelay,
		}); err != nil {
			return err
		}
		return e.q.MarkDead(ctx, tx, c.ID, "place ambiguous (timeout) — read-only recovery probe scheduled: "+cause.Error())
	})
	if txErr != nil {
		if e.log != nil {
			e.log.Warn("scheduling place recovery probe failed; reconciling", "id", c.ID, "err", txErr)
		}
		e.deadReconcile(ctx, c, "ambiguous place; probe scheduling failed: "+cause.Error())
	}
}

// recoverAmbiguousCancel turns an ambiguous CANCEL timeout into a read-only recovery: it
// schedules a GET_ORDER probe (by the known exchange_order_id) and DEAD-letters the cancel
// request. The probe determines the real outcome and continues the state machine.
func (e *Executor) recoverAmbiguousCancel(ctx context.Context, c queue.Claimed, fp orders.FollowupPayload, cause error) {
	if c.OrderID == nil || c.CycleID == nil {
		e.deadReconcile(ctx, c, "ambiguous cancel (no order/cycle context): "+cause.Error())
		return
	}
	if fp.ExchangeOrderID == "" {
		e.deadReconcile(ctx, c, "ambiguous cancel with empty exchange_order_id — cannot probe; needs reconcile")
		return
	}
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if err := orders.ScheduleCancelProbe(ctx, tx, e.q, orders.ProbeParams{
			ExchangeID: c.ExchangeID, Symbol: c.Symbol, CycleID: *c.CycleID, OrderID: *c.OrderID,
			ExchangeOrderID: fp.ExchangeOrderID, LocalClientOrderID: fp.LocalClientOrderID,
			Attempt: fp.Attempt, FirstProbeAt: firstOrNow(fp.FirstProbeAt, e.nowMillis()), Delay: e.cfg.FinalStatusDelay,
		}); err != nil {
			return err
		}
		return e.q.MarkDead(ctx, tx, c.ID, "cancel ambiguous (timeout) — read-only recovery probe scheduled: "+cause.Error())
	})
	if txErr != nil {
		if e.log != nil {
			e.log.Warn("scheduling cancel recovery probe failed; reconciling", "id", c.ID, "err", txErr)
		}
		e.deadReconcile(ctx, c, "ambiguous cancel; probe scheduling failed: "+cause.Error())
	}
}

// handleAmbiguousPlaceProbe is the read-only GET_ORDER that recovers an ambiguous place. It
// discovers the real state (looked up by client_order_id when the exchange id is unknown) and:
// FOUND → resumes the normal ack flow (OnPlaceAck/OnSellPlaceAck, which drives the cancel →
// final-status path that records fills); PROVABLY NOT PLACED (ErrOrderUnknown) → resolves as a
// clean rejection (buy: FAILED + lock released; sell: NEEDS_RECONCILE + lock held); a transient
// error → bounded read-only retry, then NEEDS_RECONCILE.
func (e *Executor) handleAmbiguousPlaceProbe(ctx, sendCtx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, "place probe missing order/cycle context")
		return
	}
	info, ierr := e.orderRecoveryInfo(ctx, *c.OrderID)
	if ierr != nil {
		e.deadReconcile(ctx, c, "place probe: cannot read order: "+ierr.Error())
		return
	}
	st, err, canProbe := e.probeLookup(sendCtx, client, info)
	if !canProbe {
		// Cannot verify (no exchange id AND the venue offers no RELIABLE client-id lookup). NEVER
		// resend the place; keep the lock; hand to manual reconcile.
		e.deadReconcile(ctx, c, "place probe: cannot verify (no exchange id; venue has no client-id lookup) — needs reconcile")
		return
	}

	// UNRESOLVED (not found OR a transient error). A first "not found" is NEVER proof the order
	// was not accepted (eventual consistency) — bounded read-only retries with exponential backoff;
	// the lock stays HELD and the cycle is NOT failed while the recovery window is open.
	if errors.Is(err, execution.ErrOrderUnknown) || isRetryable(err) {
		next := fp.Attempt + 1
		if !e.probeWindowExhausted(c.ExchangeCode, next, fp.FirstProbeAt) {
			txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
				if serr := orders.SchedulePlaceProbe(ctx, tx, e.q, orders.ProbeParams{
					ExchangeID: c.ExchangeID, Symbol: c.Symbol, CycleID: *c.CycleID, OrderID: *c.OrderID,
					LocalClientOrderID: fp.LocalClientOrderID, Attempt: next, FirstProbeAt: fp.FirstProbeAt, Delay: e.probeBackoff(c.ExchangeCode, next),
				}); serr != nil {
					return serr
				}
				return e.q.MarkSucceeded(ctx, tx, c.ID, mustJSON(map[string]any{"unresolved": true, "probe_attempt": next}))
			})
			if txErr != nil && e.log != nil {
				e.log.Warn("place probe re-schedule failed (rolled back)", "id", c.ID, "err", txErr)
			}
			return
		}
		// Recovery window exhausted, still unresolved.
		if errors.Is(err, execution.ErrOrderUnknown) && client.Capabilities().ReliableNotFound {
			// ONLY a venue with a truly reliable negative lets us conclude "provably not placed".
			e.resolvePlaceProvablyNotPlaced(ctx, c, info.role)
			return
		}
		// Still ambiguous → manual reconcile, lock HELD, cycle NOT failed.
		e.deadReconcile(ctx, c, "place probe: unresolved after the recovery window — needs reconcile")
		return
	}
	if err != nil {
		// Non-retryable, non-unknown (e.g. auth) — cannot safely recover; never re-place blindly.
		e.deadReconcile(ctx, c, "place probe failed (non-recoverable): "+err.Error())
		return
	}
	// FOUND — verify the recovered order matches our immutable fields before attaching it.
	if !recoveredMatches(info, c.Symbol, st) {
		e.deadReconcile(ctx, c, "place probe: recovered order fields do not match this order — needs reconcile")
		return
	}
	p := orders.RecoverParams{
		RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, ExchangeID: c.ExchangeID,
		Symbol: c.Symbol, Scope: c.ExchangeCode, ClientOrderIDSent: info.clientLookupID(), LocalClientOrderID: info.localClientOrderID,
	}
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if info.role == "exit_sell" {
			return orders.RecoverSellPlace(ctx, tx, e.q, p, st)
		}
		return orders.RecoverBuyPlace(ctx, tx, e.q, p, st)
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("place probe resume tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
}

// resolvePlaceProvablyNotPlaced cleanly resolves a place whose read-only probe, after bounded
// retries on a venue with a RELIABLE negative, proved it was never placed. A buy has no exposure
// → clean fail + lock release; a sell holds buy-leg inventory → NEEDS_RECONCILE, lock held.
func (e *Executor) resolvePlaceProvablyNotPlaced(ctx context.Context, c queue.Claimed, role string) {
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		p := orders.PlaceRejectedParams{RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID,
			Cause: "ambiguous place: order provably not placed (reliable negative after bounded probes)"}
		if role == "exit_sell" {
			return orders.OnSellPlaceRejected(ctx, tx, e.q, p)
		}
		return orders.OnPlaceRejected(ctx, tx, e.q, p)
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("place probe provably-not-placed resolution tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
}

// handleAmbiguousCancelProbe is the read-only GET_ORDER that recovers an ambiguous cancel. It
// reads the order by the known exchange_order_id and: STILL OPEN (cancel didn't take) → re-issues
// the cancel, bounded (a proven re-cancel, never blind), then NEEDS_RECONCILE once exhausted;
// TERMINAL (canceled/partial/filled) → treats the cancel as resolved and reads the authoritative
// final status (records the ACTUAL filled qty and continues); ErrOrderUnknown / other error →
// NEEDS_RECONCILE (not proof of any outcome). EVERY unresolved outcome — a transient lookup
// failure (timeout/network/rate-limit/5xx/context) exactly like a proven still-open — consumes an
// attempt of the SAME persisted per-exchange recovery window (RecoveryConfig: attempt counter +
// FirstProbeAt + bounded exponential backoff with jitter + TotalTimeout), never the generic queue
// retry policy; exhaustion produces DEAD probe + NEEDS_RECONCILE atomically, lock held.
func (e *Executor) handleAmbiguousCancelProbe(ctx, sendCtx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, "cancel probe missing order/cycle context")
		return
	}
	if fp.ExchangeOrderID == "" {
		e.deadReconcile(ctx, c, "cancel probe: empty exchange_order_id — cannot verify; needs reconcile")
		return
	}
	role, rerr := e.orderRole(ctx, *c.OrderID)
	if rerr != nil {
		e.deadReconcile(ctx, c, "cancel probe: cannot read order role: "+rerr.Error())
		return
	}
	sellReprice := role == "exit_sell"

	st, err := client.GetOrder(sendCtx, fp.ExchangeOrderID)
	if err != nil && !errors.Is(err, execution.ErrOrderUnknown) && isRetryable(err) {
		// TRANSIENT lookup failure (timeout / network / rate limit / retryable 5xx / context
		// deadline or cancellation): the SAME persisted recovery window as every other unresolved
		// probe outcome — the per-exchange RecoveryConfig with its attempt counter, FirstProbeAt
		// wall-clock, bounded exponential backoff + jitter — NEVER the generic queue retry policy
		// (PR19 round 4 correction #2). On exhaustion the probe goes DEAD and the order/cycle to
		// NEEDS_RECONCILE in ONE transaction (deadReconcile — correction #3), lock held.
		next := fp.Attempt + 1
		if e.probeWindowExhausted(c.ExchangeCode, next, fp.FirstProbeAt) {
			e.deadReconcile(ctx, c, fmt.Sprintf("cancel probe: transient lookup failures exhausted the recovery window (%d attempts): %s — needs reconcile", fp.Attempt, err.Error()))
			return
		}
		txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
			if serr := orders.ScheduleCancelProbe(ctx, tx, e.q, orders.ProbeParams{
				ExchangeID: c.ExchangeID, Symbol: c.Symbol, CycleID: *c.CycleID, OrderID: *c.OrderID,
				ExchangeOrderID: fp.ExchangeOrderID, LocalClientOrderID: fp.LocalClientOrderID,
				Attempt: next, FirstProbeAt: firstOrNow(fp.FirstProbeAt, e.nowMillis()), Delay: e.probeBackoff(c.ExchangeCode, next),
			}); serr != nil {
				return serr
			}
			return e.q.MarkSucceeded(ctx, tx, c.ID, mustJSON(map[string]any{"transient": err.Error(), "probe_attempt": next}))
		})
		if txErr != nil && e.log != nil {
			e.log.Warn("cancel probe transient re-schedule failed (rolled back)", "id", c.ID, "err", txErr)
		}
		return
	}
	if errors.Is(err, execution.ErrOrderUnknown) {
		e.deadReconcile(ctx, c, "cancel probe: order unknown to exchange — not proof of outcome; needs reconcile")
		return
	}
	if err != nil {
		e.deadReconcile(ctx, c, "cancel probe failed (non-recoverable): "+err.Error())
		return
	}
	if recoveryStillOpen(st) {
		// PROVEN still open: re-issue the cancel (bounded by the recovery window), never blindly.
		next := fp.Attempt + 1
		if e.probeWindowExhausted(c.ExchangeCode, next, fp.FirstProbeAt) {
			e.deadReconcile(ctx, c, fmt.Sprintf("cancel probe: order still open after the recovery window (%d attempts) — needs reconcile", fp.Attempt))
			return
		}
		txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
			if rerr := orders.ScheduleReCancel(ctx, tx, e.q, orders.ProbeParams{
				ExchangeID: c.ExchangeID, Symbol: c.Symbol, CycleID: *c.CycleID, OrderID: *c.OrderID,
				ExchangeOrderID: fp.ExchangeOrderID, LocalClientOrderID: fp.LocalClientOrderID, Attempt: next, FirstProbeAt: fp.FirstProbeAt, Delay: e.probeBackoff(c.ExchangeCode, next),
			}, sellReprice); rerr != nil {
				return rerr
			}
			return e.q.MarkSucceeded(ctx, tx, c.ID, mustJSON(map[string]any{"still_open": true, "recancel_attempt": next}))
		})
		if txErr != nil && e.log != nil {
			e.log.Warn("cancel probe re-cancel scheduling failed (rolled back)", "id", c.ID, "err", txErr)
		}
		return
	}
	// TERMINAL (canceled / partially-canceled / filled): the probe ALREADY has the authoritative
	// state, so record the actual fills DIRECTLY — no redundant second GET_ORDER or cancel
	// (PR19 round 3 #7).
	p := orders.RecoverParams{
		RequestID: c.ID, OrderID: *c.OrderID, CycleID: *c.CycleID, ExchangeID: c.ExchangeID,
		Symbol: c.Symbol, Scope: c.ExchangeCode, LocalClientOrderID: fp.LocalClientOrderID,
	}
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.completionFault(); ferr != nil {
			return ferr
		}
		if sellReprice {
			return orders.RecoverSellCancel(ctx, tx, e.q, p, st)
		}
		return orders.RecoverBuyCancel(ctx, tx, e.q, p, st)
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("cancel probe completion tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
}

// recoveryStillOpen reports whether a probed order is still working on the venue (the cancel did
// not take). A partial fill that is still open counts as open (its remainder is re-cancelled).
func recoveryStillOpen(st execution.OrderStatus) bool {
	switch st.Status {
	case execution.StateOpen, execution.StateNew, execution.StatePartiallyFilled:
		return true
	}
	return false
}

// recoverStaleMutating converts stale IN_FLIGHT PLACE/CANCEL requests — those left behind when
// the process CRASHED after the exchange accepted the send but before the response was handled
// (so no ErrAckTimeout recovery probe was ever persisted) — into persisted READ-ONLY recovery
// probes, then DEAD-letters the original mutation. It NEVER re-sends. Scoped to this executor's
// exchanges AND mode (a dry-run executor never recovers a live cycle's request, and vice-versa),
// so recovery respects the dry/live separation. Best-effort: any row it cannot convert is left
// for the queue sweeper's conservative DEAD + NEEDS_RECONCILE.
func (e *Executor) recoverStaleMutating(ctx context.Context, graceSeconds int) {
	dry := e.claimDryRunFilter()
	for _, exID := range e.exIDs {
		e.recoverStaleMutatingForExchange(ctx, exID, dry, graceSeconds)
	}
}

type staleMutation struct {
	id      int64
	typ     queue.RequestType
	cycleID sql.NullInt64
	orderID sql.NullInt64
	symbol  string
	payload []byte
}

func (e *Executor) recoverStaleMutatingForExchange(ctx context.Context, exID int64, dry *bool, graceSeconds int) {
	q := `
		SELECT er.id, er.request_type, er.cycle_id, er.order_id, COALESCE(er.symbol,''), er.payload
		FROM exchange_requests er
		WHERE er.status='IN_FLIGHT' AND er.request_type IN ('PLACE_ORDER','CANCEL_ORDER')
		  AND er.inflight_at IS NOT NULL
		  AND er.inflight_at < (NOW(6) - INTERVAL (er.timeout_ms/1000 + ?) SECOND)
		  AND er.exchange_id = ?`
	args := []any{graceSeconds, exID}
	if dry != nil {
		// Mode-scope: only this executor's mode. A mutating request always has a cycle (DB CHECK),
		// so the cycle's dry_run must match.
		q += ` AND EXISTS (SELECT 1 FROM cycles c WHERE c.id = er.cycle_id AND c.dry_run = ?)`
		d := 0
		if *dry {
			d = 1
		}
		args = append(args, d)
	}
	rows, err := e.store.DB().QueryContext(ctx, q, args...)
	if err != nil {
		if e.log != nil {
			e.log.Warn("recover stale mutating: query failed", "exchange", exID, "err", err)
		}
		return
	}
	var stale []staleMutation
	for rows.Next() {
		var s staleMutation
		var t string
		if err := rows.Scan(&s.id, &t, &s.cycleID, &s.orderID, &s.symbol, &s.payload); err != nil {
			rows.Close()
			return
		}
		s.typ = queue.RequestType(t)
		stale = append(stale, s)
	}
	rows.Close()

	for _, s := range stale {
		e.recoverOneStaleMutation(ctx, exID, s)
	}
}

func (e *Executor) recoverOneStaleMutation(ctx context.Context, exID int64, s staleMutation) {
	if !s.cycleID.Valid || !s.orderID.Valid {
		return // a mutating request always has a cycle+order (runtime guard); nothing to recover
	}
	info, err := e.orderRecoveryInfo(ctx, s.orderID.Int64)
	if err != nil {
		return
	}
	var fp orders.FollowupPayload
	_ = json.Unmarshal(s.payload, &fp)
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		// ATOMIC CLAIM (PR19 round 4 #1): lock the row and re-confirm it is STILL IN_FLIGHT. FOR
		// UPDATE SKIP LOCKED means if another recoverer already holds it we get ErrNoRows and skip
		// — so exactly one instance converts each stale mutation, and the generic sweeper (which no
		// longer touches mutating IN_FLIGHT) can never race it into a premature reconcile.
		var gotID int64
		if err := tx.QueryRowContext(ctx,
			"SELECT id FROM exchange_requests WHERE id=? AND status='IN_FLIGHT' FOR UPDATE SKIP LOCKED", s.id).Scan(&gotID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil // already claimed/handled by another instance — leave it
			}
			return err
		}
		p := orders.ProbeParams{
			ExchangeID: exID, Symbol: s.symbol, CycleID: s.cycleID.Int64, OrderID: s.orderID.Int64,
			LocalClientOrderID: info.localClientOrderID, FirstProbeAt: e.nowMillis(), Delay: e.cfg.FinalStatusDelay,
		}
		if s.typ == queue.TypePlaceOrder {
			// The place's ack was never handled — look the order up by client_order_id (never resend).
			if err := orders.SchedulePlaceProbe(ctx, tx, e.q, p); err != nil {
				return err
			}
		} else {
			// A cancel whose outcome is unknown — read-only probe by the known exchange order id.
			p.ExchangeOrderID = firstNonEmpty(fp.ExchangeOrderID, info.exchangeOrderID)
			p.Attempt = fp.Attempt
			p.FirstProbeAt = firstOrNow(fp.FirstProbeAt, e.nowMillis())
			if p.ExchangeOrderID == "" {
				// Cannot probe a cancel without an order id — mark DEAD + NEEDS_RECONCILE atomically.
				if err := orders.MarkNeedsReconcile(ctx, tx, s.orderID.Int64, &s.cycleID.Int64, "stale cancel with no exchange id — needs reconcile"); err != nil {
					return err
				}
				return e.q.MarkDead(ctx, tx, s.id, "crash recovery: stale cancel with no exchange id — needs reconcile")
			}
			if err := orders.ScheduleCancelProbe(ctx, tx, e.q, p); err != nil {
				return err
			}
		}
		return e.q.MarkDead(ctx, tx, s.id, "crash recovery: stale IN_FLIGHT mutation → read-only recovery probe scheduled")
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("recover stale mutating: schedule failed", "id", s.id, "err", txErr)
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// recoveryInfo is the DB order state a recovery probe needs: the trusted role, the identifiers
// to look the order up by (exchange id, the EXACT client id sent, the local id), and the
// immutable fields to VERIFY a recovered order really is this order before attaching it.
type recoveryInfo struct {
	role               string
	exchangeOrderID    string
	clientOrderIDSent  string
	localClientOrderID string
	side               string
	quantity           decimal.Decimal
}

// clientLookupID is the id to query the venue by when the exchange id is unknown: the EXACT id
// we sent (adapter-normalized, persisted before the send), falling back to the local id.
func (r recoveryInfo) clientLookupID() string {
	if r.clientOrderIDSent != "" {
		return r.clientOrderIDSent
	}
	return r.localClientOrderID
}

// clientOrderIDForSend returns the EXACT client id the adapter will transmit for a local id: the
// adapter's own normalization if it declares one, else the local id verbatim.
func (e *Executor) clientOrderIDForSend(client exchanges.PrivateClient, local string) string {
	if n, ok := client.(exchanges.ClientOrderIDNormalizer); ok {
		return n.ClientOrderIDForSend(local)
	}
	return local
}

// persistClientOrderIDSent commits the exact id we are about to send BEFORE the network call
// (autocommit), so recovery can look the order up by exactly what the exchange received. It
// verifies EXACTLY ONE row was updated (PR19 round 4): zero or multiple → fail closed (the caller
// must not send).
func (e *Executor) persistClientOrderIDSent(ctx context.Context, orderID int64, sent string) error {
	res, err := e.store.DB().ExecContext(ctx,
		"UPDATE orders SET client_order_id_sent = ? WHERE id = ?", sent, orderID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 1 {
		return nil
	}
	// RowsAffected reports CHANGED rows, so 0 can mean "value already equal" (a re-run) rather than
	// "no such row". Confirm exactly one order row now holds the value; anything else fails closed.
	var cnt int
	if err := e.store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM orders WHERE id = ? AND client_order_id_sent = ?", orderID, sent).Scan(&cnt); err != nil {
		return err
	}
	if cnt != 1 {
		return fmt.Errorf("persist client_order_id_sent for order %d matched %d rows, want exactly 1", orderID, cnt)
	}
	return nil
}

func (e *Executor) orderRecoveryInfo(ctx context.Context, orderID int64) (recoveryInfo, error) {
	var (
		info  recoveryInfo
		exoid sql.NullString
		sent  sql.NullString
		qtyS  string
	)
	err := e.store.DB().QueryRowContext(ctx,
		"SELECT role, COALESCE(exchange_order_id,''), COALESCE(client_order_id_sent,''), local_client_order_id, side, quantity FROM orders WHERE id=?",
		orderID).Scan(&info.role, &exoid, &sent, &info.localClientOrderID, &info.side, &qtyS)
	if err != nil {
		return recoveryInfo{}, err
	}
	info.exchangeOrderID = exoid.String
	info.clientOrderIDSent = sent.String
	info.quantity = decimalOrZero(qtyS)
	return info, nil
}

// recoveredMatches reports whether a probed order status genuinely corresponds to THIS order —
// every immutable field the venue reports must agree (side, quantity, symbol). A field the venue
// omits (empty/zero) is not held against it, but a REPORTED field that disagrees means it is a
// different order and must never be attached to the cycle (PR19 round 3 #4).
// recoveredMatches reports whether a probed order genuinely corresponds to THIS order. The order
// was looked up BY a reliable identifier (client_order_id_sent or exchange_order_id), so identity
// is already established; recovery additionally verifies the venue-reported SYMBOL and SIDE agree
// (a cheap guard against a wrong attachment). Quantity is NOT an exact-identity requirement —
// venues normalize/round the submitted quantity and partial fills report a smaller filled amount —
// so it is deliberately not checked here (PR19 round 4 #4).
func recoveredMatches(info recoveryInfo, symbol string, st execution.OrderStatus) bool {
	if st.Side != "" && !strings.EqualFold(st.Side, info.side) {
		return false
	}
	if st.Symbol != "" && symbol != "" && st.Symbol != symbol {
		return false
	}
	return true
}

// randInt63n returns a non-negative jitter in [0,n) (n<=0 → 0).
func randInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return rand.Int64N(n)
}

// probeLookup performs the read-only order lookup for a recovery probe: by exchange_order_id when
// known, else by the EXACT client id we sent — but ONLY through a real ClientOrderLookup client
// (never GetOrder, which takes an exchange id). Returns canProbe=false when neither is possible.
func (e *Executor) probeLookup(ctx context.Context, client exchanges.PrivateClient, info recoveryInfo) (execution.OrderStatus, error, bool) {
	if info.exchangeOrderID != "" {
		st, err := client.GetOrder(ctx, info.exchangeOrderID)
		return st, err, true
	}
	cid := info.clientLookupID()
	lk, ok := client.(exchanges.ClientOrderLookup)
	if cid == "" || !ok || !client.Capabilities().LookupByClientOrderID {
		return execution.OrderStatus{}, nil, false
	}
	st, err := lk.GetOrderByClientOrderID(ctx, cid)
	return st, err, true
}

// recoveryConfig resolves the recovery window for an exchange: the per-exchange override when
// present, with every UNSET (zero) field inheriting the CONFIGURED global window (e.cfg.Recovery,
// already defaulted in New) — never hard-coded defaults (PR19 round 4 correction #1). So a
// partial override like {TotalTimeout: 15m} keeps the configured global attempts/backoff.
func (e *Executor) recoveryConfig(exchangeCode string) RecoveryConfig {
	base := e.cfg.Recovery
	rc, ok := e.cfg.RecoveryPerExchange[exchangeCode]
	if !ok {
		return base
	}
	if rc.MaxAttempts <= 0 {
		rc.MaxAttempts = base.MaxAttempts
	}
	if rc.InitialDelay <= 0 {
		rc.InitialDelay = base.InitialDelay
	}
	if rc.MaxDelay <= 0 {
		rc.MaxDelay = base.MaxDelay
	}
	if rc.TotalTimeout <= 0 {
		rc.TotalTimeout = base.TotalTimeout
	}
	return rc
}

// probeBackoff is the delay before the next read-only recovery probe: bounded EXPONENTIAL backoff
// (InitialDelay << (attempt-1), capped at MaxDelay) with full jitter in [d/2, d], so many
// instances recovering at once do not synchronize.
func (e *Executor) probeBackoff(exchangeCode string, attempt int) time.Duration {
	rc := e.recoveryConfig(exchangeCode)
	d := rc.InitialDelay
	for i := 1; i < attempt && d < rc.MaxDelay; i++ {
		d *= 2
	}
	if d > rc.MaxDelay {
		d = rc.MaxDelay
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(randInt63n(int64(half)+1))
}

// probeWindowExhausted reports whether the recovery window for an exchange is spent — by attempt
// count OR by wall-clock since the first probe (fp.FirstProbeAt, unix millis). Either bound ends
// the read-only recovery and hands the order to manual reconciliation.
func (e *Executor) probeWindowExhausted(exchangeCode string, nextAttempt int, firstProbeAtMs int64) bool {
	rc := e.recoveryConfig(exchangeCode)
	if nextAttempt > rc.MaxAttempts {
		return true
	}
	if firstProbeAtMs > 0 && time.Since(time.UnixMilli(firstProbeAtMs)) > rc.TotalTimeout {
		return true
	}
	return false
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
	// FAIL CLOSED: if the cycle's mode cannot be confirmed we must NOT send. (abortOnModeMismatch
	// already caught this at the top of the handler; this is the defensive belt-and-suspenders.)
	dry, derr := e.cycleDryRun(ctx, c.CycleID)
	if derr != nil {
		e.failTx(ctx, c.ID, "live gate: cannot confirm cycle mode (not sent): "+derr.Error())
		return false
	}
	pc := live.PlaceCheck{
		ExchangeID: c.ExchangeID, ExchangeMarketID: e.orderMarket(ctx, c.OrderID), ExchangeCode: c.ExchangeCode,
		Symbol: c.Symbol, CycleID: derefID(c.CycleID), OrderID: derefID(c.OrderID), RequestID: c.ID,
		Side: side, Notional: notional, BaseQty: baseQty, DryRun: dry,
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
	dry, derr := e.cycleDryRun(ctx, c.CycleID)
	if derr != nil {
		e.failTx(ctx, c.ID, "live gate: cannot confirm cycle mode (cancel not sent): "+derr.Error())
		return false
	}
	d := e.cfg.Guard.CheckCancel(ctx, live.PlaceCheck{
		ExchangeID: c.ExchangeID, CycleID: derefID(c.CycleID), OrderID: derefID(c.OrderID), RequestID: c.ID,
		DryRun: dry,
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

// cycleDryRun reports whether a cycle is a dry-run (simulated) cycle. It FAILS CLOSED (PR19
// round 2 #6): a DB error, a missing cycle, an invalid/NULL dry_run, or a nil cycle id all
// return a non-nil error so the caller treats the cycle's mode as UNCONFIRMED. The final live
// guard must never call the exchange when it cannot confirm the cycle mode — "cannot confirm
// → do not send".
func (e *Executor) cycleDryRun(ctx context.Context, cycleID *int64) (bool, error) {
	if e.cfg.cycleModeFault != nil {
		if err := e.cfg.cycleModeFault(); err != nil {
			return false, err
		}
	}
	if cycleID == nil {
		return false, fmt.Errorf("cannot confirm cycle mode: missing cycle id")
	}
	var dry sql.NullInt64
	err := e.store.DB().QueryRowContext(ctx, "SELECT dry_run FROM cycles WHERE id=?", *cycleID).Scan(&dry)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("cannot confirm cycle mode: cycle %d not found", *cycleID)
	}
	if err != nil {
		return false, fmt.Errorf("cannot confirm cycle mode for cycle %d: %w", *cycleID, err)
	}
	if !dry.Valid {
		return false, fmt.Errorf("cannot confirm cycle mode: cycle %d has NULL dry_run", *cycleID)
	}
	return dry.Int64 != 0, nil
}

// claimDryRunFilter is the dry_run filter passed to queue.Claim: a dry-run executor claims
// only dry_run=1 cycles' requests, a live executor only dry_run=0; off has no clients so it
// never claims. This is the FIRST separation guard (claim time).
func (e *Executor) claimDryRunFilter() *bool {
	switch e.cfg.ExecutionMode {
	case "dry_run":
		t := true
		return &t
	case "live":
		f := false
		return &f
	}
	return nil
}

// abortOnModeMismatch is the pre-send check applied at the top of every send handler. If
// the claimed request's owning cycle is in the wrong mode for this executor, it logs and
// returns true; the caller must then return WITHOUT sending or marking the request — it
// stays CLAIMED and the stale-claim sweeper reverts it so the correct-mode executor (whose
// claim filter matches) can pick it up. This never marks a mismatched request failed/sent.
func (e *Executor) abortOnModeMismatch(ctx context.Context, c queue.Claimed) bool {
	mismatch, err := e.modeMismatch(ctx, c.CycleID)
	if err != nil {
		// FAIL CLOSED (PR19 round 2 #6): the cycle's mode could not be confirmed (DB error,
		// missing cycle, invalid dry_run). Never call the exchange when we cannot confirm the
		// mode. Leave the request CLAIMED (untouched) — the stale-claim sweeper reverts it, so it
		// stays recoverable and is NEVER falsely marked sent/failed.
		if e.log != nil {
			var cyc int64
			if c.CycleID != nil {
				cyc = *c.CycleID
			}
			e.log.Error("cannot confirm cycle mode — refusing to send (request left untouched)",
				"request", c.ID, "cycle", cyc, "type", string(c.Type), "mode", e.cfg.ExecutionMode, "err", err)
		}
		return true
	}
	if !mismatch {
		return false
	}
	if e.log != nil {
		var cyc int64
		if c.CycleID != nil {
			cyc = *c.CycleID
		}
		e.log.Error("executor mode/cycle dry_run MISMATCH — refusing to send (request left untouched)",
			"request", c.ID, "cycle", cyc, "type", string(c.Type), "mode", e.cfg.ExecutionMode)
	}
	return true
}

// modeMismatch is the SECOND (final, pre-send) separation guard: it reports whether the
// owning cycle's dry_run flag does NOT match the executor's mode. A dry-run executor must
// never PlaceOrder/CancelOrder for a real cycle (and vice-versa) — even though the claim
// filter already prevents claiming one, this belt-and-suspenders check runs immediately
// before every send. A non-cycle request (no cycle_id) has no dry_run to match.
func (e *Executor) modeMismatch(ctx context.Context, cycleID *int64) (bool, error) {
	if cycleID == nil {
		// A non-cycle request (e.g. a bare GET_BALANCE / recovery cancel with no cycle) has no
		// dry_run to match. It is never a mode mismatch and needs no confirmation.
		return false, nil
	}
	dry, err := e.cycleDryRun(ctx, cycleID)
	if err != nil {
		return false, err // cannot confirm — the caller fails closed
	}
	switch e.cfg.ExecutionMode {
	case "dry_run":
		return !dry, nil
	case "live":
		return dry, nil
	}
	return false, nil
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
