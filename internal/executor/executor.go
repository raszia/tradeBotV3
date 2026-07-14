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
	"sync"
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
	// hookAfterMarkInFlight is a TEST-ONLY hook (PR20 correction #3). When non-nil it runs
	// immediately after MarkInFlight and before the sendCtx.Err() pre-send check, letting a
	// test cancel the context in that exact window to prove no adapter call happens. Unexported
	// → only same-package tests set it; never in production.
	hookAfterMarkInFlight func()
	// hookOrderRecoveryInfoErr is a TEST-ONLY hook (PR20 round 10 #2): when non-nil and it
	// returns a non-nil error, stale recovery's authoritative order re-read is treated as
	// failing with that error — simulating a persistently failing orders query so the
	// expired-window conservative path can be exercised. Unexported → tests only.
	hookOrderRecoveryInfoErr func(orderID int64) error
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
	// RateLimitFallbackCooldown is the reactive park duration used when a rate-limited venue
	// provided no usable wait (and the exchange has no configured retry_backoff_ms). Default 60s.
	RateLimitFallbackCooldown time.Duration
	// RateLimitMaxCooldown bounds any park deadline (venue-provided or fallback). Default 15m.
	RateLimitMaxCooldown time.Duration
	// CooldownPersistGrace is how long a rate-limit park may remain un-persisted before live
	// ENTRY BUYS for THAT exchange are disabled (fail closed — a restart would forget the
	// park). Proven exit sells and cancels stay available. Default 30s.
	CooldownPersistGrace time.Duration
	// ShutdownFlushTimeout bounds the graceful-shutdown flush of pending cooldown persistence
	// (PR20 correction #5). Default 5s.
	ShutdownFlushTimeout time.Duration
	// StartupLoad runs SYNCHRONOUSLY in Run before any request is claimed (PR20 correction
	// #4): main uses it to load + validate the DB-backed exchange tuning, so no request can
	// be paced/backed-off with uninitialized zero values. An error aborts Run — in live mode
	// that means no real mutation happens with unloaded config.
	StartupLoad func(ctx context.Context) error
	// RateLimitSink bridges throttle signals seen by the adapters on SUCCESSFUL responses
	// into this executor's reactive cooldown (PR20 correction #6). Optional; created by main
	// before the clients and bound to the executor by New.
	RateLimitSink *RateLimitSink
	// ExchangeTuningFor resolves per-exchange operational tuning from the DB-backed config
	// (PR20 #7 — wires the previously-dead exchange_configs fields): rate_limit_per_sec drives
	// the PROACTIVE pacer; retry_backoff_ms is the per-exchange REACTIVE fallback cooldown.
	// nil (tests / no configstore) disables pacing and uses the global fallback.
	ExchangeTuningFor func(exchangeCode string) (rateLimitPerSec int, retryBackoff time.Duration)
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
	// Per-exchange reactive cooldown + proactive pacer (PR20 #4/#7 — see cooldown.go).
	cooldowns *cooldowns
	// persistWake nudges the cooldown persistence worker; buffered(1) so the exchange
	// response path never blocks on it (PR20 correction #2).
	persistWake chan struct{}
	// durabilityFailed tracks, PER EXCHANGE, whether cooldown durability is currently failed
	// (used only for transition logging; the gate computes health on demand). PR20 #4.
	durMu            sync.Mutex
	durabilityFailed map[string]bool
	pacers           *pacer
	nowFn            func() time.Time
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
	e := &Executor{store: store, q: q, clients: clients, exIDs: map[string]int64{}, log: log, cfg: cfg,
		cooldowns: newCooldowns(), pacers: newPacer(), nowFn: time.Now, persistWake: make(chan struct{}, 1),
		durabilityFailed: map[string]bool{}}
	// Bind the late-bound rate-limit sink (created before the clients) to this executor, so
	// throttle headers on SUCCESSFUL adapter responses park the exchange (PR20 #6).
	if cfg.RateLimitSink != nil {
		cfg.RateLimitSink.bind(e)
	}
	return e
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
// Run drives the executor. STARTUP ORDER IS A SAFETY PROPERTY (PR20 correction #4): every
// input that a real send depends on is loaded and validated SYNCHRONOUSLY, before the first
// claim, and any failure aborts instead of degrading to defaults:
//
//	resolve exchange ids → StartupLoad (exchange tuning, loaded + validated)
//	  → load durable cooldowns → start the cooldown persister → only THEN claim/send
//
// Loading tuning in the background would let the first requests run with uninitialized zero
// values (rate_limit_per_sec = 0 → no pacing; retry_backoff_ms = 0 → the wrong fallback), and
// loading cooldowns late would let a restart send to a still-throttled venue. Periodic
// refresh starts afterwards and is allowed to fail (the cache keeps its last good snapshot).
func (e *Executor) Run(ctx context.Context) error {
	if err := e.resolveExchangeIDs(ctx); err != nil {
		return err
	}
	// SYNCHRONOUS tuning load + validation. In live mode a failure here means no real
	// mutation can occur with uninitialized fallback values — the binary exits instead.
	if e.cfg.StartupLoad != nil {
		if err := e.cfg.StartupLoad(ctx); err != nil {
			return fmt.Errorf("startup load (exchange tuning): %w", err)
		}
	}
	// Restore still-active rate-limit cooldowns BEFORE the first claim, so a restart never
	// resumes sending to a venue that is still throttled. A load failure is fatal to startup:
	// silently starting un-parked is exactly the bug this prevents.
	if err := e.loadCooldowns(ctx); err != nil {
		return fmt.Errorf("load exchange cooldowns: %w", err)
	}
	// STARTUP (round 11): a recovery probe committed as QUEUED before the last shutdown may now be
	// UNCLAIMABLE — the order's exchange was disabled, its credential removed, or this restart did
	// not construct its client. The claim loop will never pick it up and SweepStuck ignores QUEUED
	// GET_ORDERs, so it would sit stuck forever. Finalize such probes conservatively BEFORE the
	// first claim so no order/cycle/lock is silently stranded across a restart.
	e.sweepUnclaimableRecoveryProbes(ctx, 200)
	// The persistence worker owns ALL cooldown DB writes; no exchange response path ever
	// waits on one (PR20 correction #2). It is tracked so graceful shutdown can wait for it
	// and then flush any remaining pending parks (PR20 correction #5).
	persisterDone := make(chan struct{})
	go func() { defer close(persisterDone); e.runCooldownPersister(ctx) }()
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
			// Graceful shutdown (PR20 correction #5): claiming has stopped; wait for the
			// persistence worker to exit (no new persistence events), then flush any pending
			// cooldowns durably with a bounded, independent context so a restart does not
			// forget an active park.
			<-persisterDone
			e.flushCooldownsOnShutdown()
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
				// PR20 correction #1: finalize any malformed mutating rows (missing order/cycle)
				// that Claim now refuses, so they never sit non-terminal forever.
				e.sweepMalformedMutations(ctx, 50)
				if _, err := e.q.SweepStuck(ctx, 5); err != nil && e.log != nil {
					e.log.Warn("sweep stuck failed", "err", err)
				}
				// PR20 round 11: finalize any GET_ORDER recovery probe that has BECOME unclaimable
				// (its order's exchange was disabled or its client removed after the probe was
				// queued) — the creation-time capability check cannot cover a later loss.
				e.sweepUnclaimableRecoveryProbes(ctx, 200)
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
		// PR20 #4: a parked (rate-limited) exchange is skipped ENTIRELY — nothing is
		// claimed, so no request of any kind (order/cancel/status/balance) can reach it
		// until its cooldown expires. Other exchanges continue unaffected. Poll-driven —
		// the next tick re-checks; no busy loop.
		if e.parked(code) {
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
	// PR20 #4 (belt-and-suspenders inside a claimed batch): if an EARLIER request of this
	// batch just parked the exchange, defer this one BEFORE any network call. The request
	// was only CLAIMED (never sent), so releasing it back to QUEUED is always safe.
	if e.parked(c.ExchangeCode) {
		if err := e.q.Release(ctx, c.ID); err != nil && e.log != nil {
			e.log.Warn("release of deferred request failed", "id", c.ID, "err", err)
		}
		return
	}
	// PR20 corrections #3/#7: proactive pacing happens at each real send boundary, and the
	// exchange-call timeout is created ONLY AFTER pacing (sendContext), so the timeout measures
	// the venue call, not the internal pacing wait, and a cancellation during pacing sends
	// nothing.
	switch c.Type {
	case queue.TypeGetBalance:
		if e.paceSend(ctx, c.ExchangeCode) != nil {
			return // cancelled while pacing: nothing sent
		}
		sendCtx, cancel := e.sendContext(ctx, c)
		defer cancel()
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
				e.handleFinalStatus(ctx, c, fp, client)
				return
			case orders.PurposeSellStatus:
				e.handleSellStatus(ctx, c, fp, client)
				return
			case orders.PurposeAmbiguousPlaceProbe:
				e.handleAmbiguousPlaceProbe(ctx, c, fp, client)
				return
			case orders.PurposeAmbiguousCancelProbe:
				e.handleAmbiguousCancelProbe(ctx, c, fp, client)
				return
			}
		}
		if e.paceSend(ctx, c.ExchangeCode) != nil {
			return // cancelled while pacing: nothing sent, row stays CLAIMED for the sweeper
		}
		sendCtx, cancel := e.sendContext(ctx, c)
		defer cancel()
		st, err := client.GetOrder(sendCtx, fp.ExchangeOrderID)
		e.handleReadOnly(ctx, c, mustJSON(st), err)
	case queue.TypeGetOpenOrders:
		var p struct {
			Symbol string `json:"symbol"`
		}
		_ = json.Unmarshal(c.Payload, &p)
		if e.paceSend(ctx, c.ExchangeCode) != nil {
			return // cancelled while pacing: nothing sent
		}
		sendCtx, cancel := e.sendContext(ctx, c)
		defer cancel()
		list, err := client.GetOpenOrders(sendCtx, p.Symbol)
		e.handleReadOnly(ctx, c, mustJSON(list), err)
	case queue.TypePlaceOrder:
		e.dispatchPlace(ctx, c, client)
	case queue.TypeCancelOrder:
		var fp orders.FollowupPayload
		_ = json.Unmarshal(c.Payload, &fp)
		if fp.Purpose == orders.PurposeSellReprice {
			e.handleSellCancel(ctx, c, fp, client)
		} else {
			e.handleCancel(ctx, c, client)
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
	// PR20 #4: a rate-limited read parks the whole exchange (the reschedule below then
	// naturally lands after the cooldown, since a parked exchange claims nothing).
	e.noteRateLimit(c.ExchangeCode, sendErr)
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
func (e *Executor) dispatchPlace(ctx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	if c.OrderID == nil || c.CycleID == nil {
		// No DB order to consult or resolve — request-only failure.
		e.finalizeMalformedMutation(ctx, c, "PLACE_ORDER missing order/cycle context")
		return
	}
	role, err := e.orderRole(ctx, *c.OrderID)
	if err != nil {
		// PR20 correction #1: a pre-handler failure must not strand the order/cycle. The request
		// is still CLAIMED (never sent). A TEMPORARY DB error re-queues with backoff (recoverable);
		// a missing order row (sql.ErrNoRows) is permanent → conservative disposition (request
		// DEAD, actual order+cycle NEEDS_RECONCILE, lock HELD — via the authoritative cycle).
		if errors.Is(err, sql.ErrNoRows) {
			e.disposeDenied(ctx, c, orders.KindUnknown, true, "PLACE_ORDER: order row missing")
		} else {
			e.requeueUnsent(ctx, c, orders.KindUnknown, execution.NotSent(fmt.Errorf("cannot read order role: %w", err)), true)
		}
		return
	}
	switch role {
	case "entry_buy":
		e.handlePlace(ctx, c, client)
	case "exit_sell":
		e.handleSellPlace(ctx, c, client)
	default:
		// Unroutable role: a permanent inconsistency — never send, never strand.
		e.disposeDenied(ctx, c, orders.KindUnknown, true, "PLACE_ORDER: unroutable order role "+role)
	}
}

// orderRole reads the trusted role (entry_buy | exit_sell) of the DB order.
func (e *Executor) orderRole(ctx context.Context, orderID int64) (string, error) {
	var role string
	err := e.store.DB().QueryRowContext(ctx, "SELECT role FROM orders WHERE id=?", orderID).Scan(&role)
	return role, err
}

// rejectBuyCleanly resolves a buy PLACE_ORDER that must not be sent (undecodable/invalid
// payload, or a live-guard denial). It routes through orders.OnBuyDenied, which releases the
// symbol lock ONLY when it can atomically PROVE zero exchange exposure (order still QUEUED,
// filled_quantity 0, exchange_order_id NULL, cycle pre-send) and otherwise holds the lock and
// marks order+cycle NEEDS_RECONCILE (PR20 correction #1). So even a denial that races a
// recovery/reconcile which advanced the order can never wrongly release the lock.
func (e *Executor) rejectBuyCleanly(ctx context.Context, c queue.Claimed, cause string) {
	e.disposeDenied(ctx, c, orders.KindEntryBuy, false, cause)
}

// disposeDenied applies the AUTHORITATIVE terminal disposition for a denied/permanently-failed
// mutating request (PR20 correction #1/#2): the cycle is derived from the ORDER (never the
// queue's claimed cycle_id), the request↔order↔cycle↔exchange relationship is proven, and the
// lock is released only for an entry buy with proven zero exposure; otherwise order+cycle go
// NEEDS_RECONCILE with the lock HELD. `dead` marks the request DEAD (else FAILED).
func (e *Executor) disposeDenied(ctx context.Context, c queue.Claimed, kind orders.MutationKind, dead bool, cause string) {
	if c.OrderID == nil {
		e.failTx(ctx, c.ID, cause) // no order to reconcile — resolve the request only
		return
	}
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		return orders.DisposeDeniedMutation(ctx, tx, e.q, orders.DenialParams{
			RequestID: c.ID, OrderID: *c.OrderID, ClaimedCycleID: derefID(c.CycleID),
			ClaimedExchangeID: c.ExchangeID, Kind: kind, RequestDead: dead, Cause: cause,
		})
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("mutation disposition tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
}

// rejectSellToReconcile handles a sell PLACE_ORDER that must not be sent (undecodable/invalid
// payload). A sell has existing inventory, so unlike a buy it is NOT failed+released: the
// request is FAILED and the order + cycle go to NEEDS_RECONCILE with the lock HELD (via the
// official state path), for an operator/reconciler to resolve the inventory. Nothing is sent.
func (e *Executor) rejectSellToReconcile(ctx context.Context, c queue.Claimed, cause string) {
	// An exit sell never releases the lock (inventory) — DisposeDeniedMutation(KindExitSell)
	// always takes the conservative NEEDS_RECONCILE + lock-HELD path, on the ACTUAL cycle.
	e.disposeDenied(ctx, c, orders.KindExitSell, false, cause)
}

// handlePlace processes a buy PLACE_ORDER. It marks IN_FLIGHT (committed) BEFORE
// sending so a crash is recoverable, then records the outcome conservatively and
// (on success) schedules the simulated-IOC cancel via internal/orders.
func (e *Executor) handlePlace(ctx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	if e.abortOnModeMismatch(ctx, c) {
		return
	}
	// The dispatcher guarantees order/cycle context (it routed here by the DB order role),
	// but re-guard defensively: without it we can only fail the request.
	if c.OrderID == nil || c.CycleID == nil {
		e.finalizeMalformedMutation(ctx, c, "PLACE_ORDER missing order/cycle context")
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
	// Build the venue request and prepare the EXACT value to send BEFORE any guard (PR20
	// correction #6): normalize the client id and verify it is non-empty. No transformation
	// happens after this point, so the guard proves precisely what will be sent.
	req := intent.OrderRequest(c.Symbol)
	sentCOID := e.clientOrderIDForSend(client, intent.LocalClientOrderID)
	if sentCOID == "" {
		e.rejectBuyCleanly(ctx, c, "client order id normalized to empty — not sent (fail closed)")
		return
	}
	req.ClientOrderID = sentCOID
	// EARLY guard (PR20 correction #2): a cheap pre-pacing pre-filter so a locally-invalid
	// request never consumes a pacing slot. No audit (that is the final guard's job). No
	// sent-id yet — the final guard proves it once persisted.
	if !e.earlyGatePlace(ctx, c, buildPlacePayload(req, intent.LocalClientOrderID, "")) {
		return // denied + request resolved (no-stranding disposition)
	}
	// Persist the EXACT sent id BEFORE the final guard (PR20 correction #4/#6), so the guard
	// proves the persisted value and a timeout/crash recovery finds the order by exactly what
	// the exchange received. Fail closed if we cannot persist it.
	if err := e.persistClientOrderIDSent(ctx, *c.OrderID, sentCOID); err != nil {
		if e.log != nil {
			e.log.Warn("could not persist client_order_id_sent; not sending", "id", c.ID, "err", err)
		}
		return
	}
	// PREPARE stage (PR20 correction #2): if the adapter supports it, do ALL pre-send work
	// (credentials/token/auth, symbol normalization, payload AND the final http.Request) NOW —
	// BEFORE MarkInFlight. A crash during preparation therefore leaves the row CLAIMED (never
	// sent). A preparation failure is definitely-not-sent while still CLAIMED (claimed=true
	// disposition). Round 8 #3: the adapter paces ONLY the network calls it actually makes during
	// preparation via the hook in prepareCtx (a cached-token or in-memory-credential prepare
	// reserves no slot); the order send is paced separately below.
	send := func(sctx context.Context) (execution.OrderAck, error) { return client.PlaceOrder(sctx, req) }
	if pm, ok := client.(exchanges.MutationPreparer); ok {
		prepared, perr := pm.PreparePlace(e.prepareCtx(ctx, c.ExchangeCode), req)
		if perr != nil {
			e.notSentBeforePlace(ctx, c, false, perr, true) // pre-MarkInFlight → CLAIMED disposition
			return
		}
		send = prepared.Send
	}
	// Proactive pacing of the ORDER send — BEFORE MarkInFlight and BEFORE the exchange timeout
	// (PR20 correction #3): a crash/cancellation here leaves the row CLAIMED (swept back to
	// QUEUED), never IN_FLIGHT. This reserves the slot for the actual order/cancel HTTP call.
	if e.paceSend(ctx, c.ExchangeCode) != nil {
		return // cancelled while pacing: do NOT MarkInFlight, do NOT send
	}
	// FINAL guard immediately before MarkInFlight (PR20 correction #2): the initial guard may
	// have gone stale while pacing/preparing. It writes the durable allow-audit and proves the
	// persisted sent-id.
	if !e.finalGatePlace(ctx, c, buildPlacePayload(req, intent.LocalClientOrderID, sentCOID)) {
		return // denied + audited + request resolved (no-stranding disposition)
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		if e.log != nil {
			e.log.Warn("mark in-flight failed; not sending", "id", c.ID, "err", err)
		}
		return
	}
	if e.cfg.hookAfterMarkInFlight != nil {
		e.cfg.hookAfterMarkInFlight()
	}
	// Exchange timeout starts HERE (PR20 correction #3) — at the real mutation network boundary.
	sendCtx, cancel := e.sendContext(ctx, c)
	defer cancel()
	if cerr := sendCtx.Err(); cerr != nil {
		e.notSentBeforePlace(ctx, c, false, execution.NotSent(cerr), false)
		return
	}
	ack, err := send(sendCtx) // the ONLY order-endpoint network call; req.ClientOrderID = sentCOID
	if err == nil {
		e.completePlaceSuccess(ctx, c, ack, intent)
		// PR24/PR20 #2: record the first-order checklist AFTER the send + persistence — off the
		// final-guard→MarkInFlight critical window. Best-effort; never affects the send.
		e.recordFirstOrderChecklist(ctx, c)
		return
	}
	// PR20 correction #3: a pre-network (definitely-not-sent) failure is NEVER ambiguous.
	if execution.IsNotSent(err) {
		e.notSentBeforePlace(ctx, c, false, err, false)
		return
	}
	// PR20 #5: a rate-limit signal parks the exchange (reactive cooldown) FIRST. Then:
	// a venue-PROVEN pre-execution rejection (DefiniteRejection — e.g. Nobitex's documented
	// TooManyRequests envelope) is safe to re-queue after the cooldown (never blind); any
	// other rate-limit-looking response — including HTTP 200 with a throttle body — is
	// AMBIGUOUS: never retried, never marked successful, resolved by a read-only probe.
	if rl := e.noteRateLimit(c.ExchangeCode, err); rl != nil {
		if rl.DefiniteRejection {
			e.requeueUnsent(ctx, c, orders.KindEntryBuy, err, false)
			return
		}
		e.recoverAmbiguousPlace(ctx, c, intent.LocalClientOrderID, err)
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
func (e *Executor) handleCancel(ctx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	if e.abortOnModeMismatch(ctx, c) {
		return
	}
	// PR19 round 3 #6: a mutating request without a cycle + order cannot be classified dry-run vs
	// live nor recovered — FAIL CLOSED before any send (no exchange/simexec call).
	if c.OrderID == nil || c.CycleID == nil {
		e.finalizeMalformedMutation(ctx, c, "CANCEL_ORDER missing cycle/order context — not sent (fail closed)")
		return
	}
	var fp orders.FollowupPayload
	if err := json.Unmarshal(c.Payload, &fp); err != nil {
		// PR20 correction #1: a malformed CANCEL payload is a permanent local failure — the
		// order may still be open on the venue, so NEEDS_RECONCILE + lock HELD (never a bare
		// queue-row failure), via the authoritative cycle.
		e.disposeDenied(ctx, c, orders.KindCancel, true, "bad CANCEL_ORDER payload: "+err.Error())
		return
	}
	// FINAL SAFETY BOUNDARY (PR11): never call CancelOrder(""). With order/cycle context →
	// DEAD + NEEDS_RECONCILE (lock held); otherwise a request-only failure. Not sent.
	if fp.ExchangeOrderID == "" {
		e.deadReconcile(ctx, c, "CANCEL_ORDER has empty exchange_order_id — not sent; needs reconcile")
		return
	}
	// EARLY guard (pre-pacing pre-filter).
	if !e.earlyGateCancel(ctx, c, fp.ExchangeOrderID) {
		return
	}
	// PREPARE stage (PR20 correction #2): token/auth + final http.Request built before MarkInFlight
	// when supported. Round 8 #3: the adapter paces only the preparation network calls it actually
	// makes via prepareCtx; the cancel send is paced separately below.
	send := func(sctx context.Context) error { return client.CancelOrder(sctx, fp.ExchangeOrderID) }
	if pm, ok := client.(exchanges.MutationPreparer); ok {
		prepared, perr := pm.PrepareCancel(e.prepareCtx(ctx, c.ExchangeCode), fp.ExchangeOrderID)
		if perr != nil {
			e.notSentBeforeCancel(ctx, c, fp, perr, true) // pre-MarkInFlight → CLAIMED disposition (lock held)
			return
		}
		send = func(sctx context.Context) error { _, e := prepared.Send(sctx); return e }
	}
	// Pacing of the cancel send before MarkInFlight and before the exchange timeout.
	if e.paceSend(ctx, c.ExchangeCode) != nil {
		return // cancelled while pacing: do NOT MarkInFlight, do NOT send
	}
	// FINAL guard immediately before MarkInFlight (PR20 correction #2): order/cycle state may
	// have changed while pacing.
	if !e.finalGateCancel(ctx, c, fp.ExchangeOrderID) {
		return
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		return
	}
	sendCtx, cancel := e.sendContext(ctx, c)
	defer cancel()
	if cerr := sendCtx.Err(); cerr != nil {
		e.notSentBeforeCancel(ctx, c, fp, execution.NotSent(cerr), false)
		return
	}
	err := send(sendCtx)
	if execution.IsNotSent(err) {
		e.notSentBeforeCancel(ctx, c, fp, err, false)
		return
	}
	// PR20 #5: a rate-limited cancel is NOT a resolution — the cancel may or may not have
	// executed. Park the exchange; a venue-PROVEN pre-execution rejection re-queues the
	// cancel after the cooldown (we still want the remainder cancelled); anything else is
	// AMBIGUOUS and goes to the read-only recovery probe.
	if rl := e.noteRateLimit(c.ExchangeCode, err); rl != nil {
		if rl.DefiniteRejection {
			e.requeueUnsent(ctx, c, orders.KindCancel, err, false)
			return
		}
		e.recoverAmbiguousCancel(ctx, c, fp, err)
		return
	}
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
func (e *Executor) handleFinalStatus(ctx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	// FINAL SAFETY BOUNDARY (PR11): never call GetOrder("") — not fetched → DEAD +
	// NEEDS_RECONCILE (lock held).
	if fp.ExchangeOrderID == "" {
		e.deadReconcile(ctx, c, "GET_ORDER has empty exchange_order_id — not fetched; needs reconcile")
		return
	}
	if e.paceSend(ctx, c.ExchangeCode) != nil {
		return // cancelled while pacing: no read sent, request stays claimable
	}
	sendCtx, cancel := e.sendContext(ctx, c)
	defer cancel()
	st, err := client.GetOrder(sendCtx, fp.ExchangeOrderID)
	e.noteRateLimit(c.ExchangeCode, err) // PR20 #4: park the exchange on a throttled read
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
func (e *Executor) handleSellPlace(ctx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	if e.abortOnModeMismatch(ctx, c) {
		return
	}
	if c.OrderID == nil || c.CycleID == nil {
		e.finalizeMalformedMutation(ctx, c, "sell PLACE_ORDER missing order/cycle context")
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
	// Prepare the exact sent id before any guard (PR20 correction #6). A proven exit sell is
	// NOT gated on cooldown durability (risk-reducing), only entry buys are (PR20 correction #4).
	sreq := intent.OrderRequest(c.Symbol)
	sentCOID := e.clientOrderIDForSend(client, intent.LocalClientOrderID)
	if sentCOID == "" {
		e.rejectSellToReconcile(ctx, c, "sell client order id normalized to empty — not sent (fail closed)")
		return
	}
	sreq.ClientOrderID = sentCOID
	// EARLY guard (pre-pacing pre-filter).
	if !e.earlyGatePlace(ctx, c, buildPlacePayload(sreq, intent.LocalClientOrderID, "")) {
		return // denied + request resolved (keeps the lock — see denyPlace for sells)
	}
	// Persist the exact sent id BEFORE the final guard.
	if err := e.persistClientOrderIDSent(ctx, *c.OrderID, sentCOID); err != nil {
		if e.log != nil {
			e.log.Warn("could not persist sell client_order_id_sent; not sending", "id", c.ID, "err", err)
		}
		return
	}
	// PREPARE stage (PR20 correction #2): all pre-send work (incl. the final http.Request) before
	// MarkInFlight when supported. Round 8 #3: preparation network calls are paced by the adapter
	// via prepareCtx; the order send is paced separately below.
	send := func(sctx context.Context) (execution.OrderAck, error) { return client.PlaceOrder(sctx, sreq) }
	if pm, ok := client.(exchanges.MutationPreparer); ok {
		prepared, perr := pm.PreparePlace(e.prepareCtx(ctx, c.ExchangeCode), sreq)
		if perr != nil {
			e.notSentBeforePlace(ctx, c, true, perr, true) // pre-MarkInFlight → CLAIMED disposition (keeps the lock)
			return
		}
		send = prepared.Send
	}
	// Pacing of the ORDER send before MarkInFlight and before the exchange timeout.
	if e.paceSend(ctx, c.ExchangeCode) != nil {
		return // cancelled while pacing: do NOT MarkInFlight, do NOT send
	}
	// FINAL guard immediately before MarkInFlight (PR20 correction #2).
	if !e.finalGatePlace(ctx, c, buildPlacePayload(sreq, intent.LocalClientOrderID, sentCOID)) {
		return // denied + audited + request resolved (keeps the lock)
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		return
	}
	// Exchange timeout starts at the real mutation network boundary (PR20 correction #3).
	sendCtx, cancel := e.sendContext(ctx, c)
	defer cancel()
	if cerr := sendCtx.Err(); cerr != nil {
		e.notSentBeforePlace(ctx, c, true, execution.NotSent(cerr), false)
		return
	}
	ack, err := send(sendCtx) // the ONLY order-endpoint network call; sreq.ClientOrderID = sentCOID
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
	// PR20 #5: rate-limit handling identical to the buy — proven pre-execution rejection
	// re-queues after the cooldown; anything else rate-limit-looking is ambiguous.
	if execution.IsNotSent(err) {
		e.notSentBeforePlace(ctx, c, true, err, false)
		return
	}
	if rl := e.noteRateLimit(c.ExchangeCode, err); rl != nil {
		if rl.DefiniteRejection {
			e.requeueUnsent(ctx, c, orders.KindExitSell, err, false)
			return
		}
		e.recoverAmbiguousPlace(ctx, c, intent.LocalClientOrderID, err)
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
func (e *Executor) handleSellCancel(ctx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	if e.abortOnModeMismatch(ctx, c) {
		return
	}
	if c.OrderID == nil || c.CycleID == nil {
		e.finalizeMalformedMutation(ctx, c, "sell CANCEL_ORDER missing order/cycle context")
		return
	}
	// FINAL SAFETY BOUNDARY (PR11): never call CancelOrder("") — an empty exchange_order_id is
	// unusable. Do not send, do not MarkInFlight: request FAILED, order+cycle NEEDS_RECONCILE,
	// lock HELD (inventory). Reconciliation determines the real exchange state.
	if fp.ExchangeOrderID == "" {
		e.rejectSellToReconcile(ctx, c, "sell CANCEL_ORDER has empty exchange_order_id — not sent; needs reconcile")
		return
	}
	// EARLY guard (pre-pacing pre-filter).
	if !e.earlyGateCancel(ctx, c, fp.ExchangeOrderID) {
		return
	}
	// PREPARE stage (PR20 correction #2): token/auth + final http.Request built before
	// MarkInFlight when supported. Round 8 #3: preparation network calls are paced by the adapter
	// via prepareCtx; the cancel send is paced separately below.
	send := func(sctx context.Context) error { return client.CancelOrder(sctx, fp.ExchangeOrderID) }
	if pm, ok := client.(exchanges.MutationPreparer); ok {
		prepared, perr := pm.PrepareCancel(e.prepareCtx(ctx, c.ExchangeCode), fp.ExchangeOrderID)
		if perr != nil {
			e.notSentBeforeCancel(ctx, c, fp, perr, true) // pre-MarkInFlight → CLAIMED disposition (lock held)
			return
		}
		send = func(sctx context.Context) error { _, e := prepared.Send(sctx); return e }
	}
	// Pacing of the cancel send before MarkInFlight and before the exchange timeout.
	if e.paceSend(ctx, c.ExchangeCode) != nil {
		return // cancelled while pacing: do NOT MarkInFlight, do NOT send
	}
	// FINAL guard immediately before MarkInFlight (PR20 correction #2).
	if !e.finalGateCancel(ctx, c, fp.ExchangeOrderID) {
		return
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		return
	}
	sendCtx, cancel := e.sendContext(ctx, c)
	defer cancel()
	if cerr := sendCtx.Err(); cerr != nil {
		e.notSentBeforeCancel(ctx, c, fp, execution.NotSent(cerr), false)
		return
	}
	err := send(sendCtx)
	if execution.IsNotSent(err) {
		e.notSentBeforeCancel(ctx, c, fp, err, false)
		return
	}
	// PR20 #5: same rate-limit policy as the buy-side cancel (park; proven → re-queue;
	// otherwise ambiguous probe).
	if rl := e.noteRateLimit(c.ExchangeCode, err); rl != nil {
		if rl.DefiniteRejection {
			e.requeueUnsent(ctx, c, orders.KindCancel, err, false)
			return
		}
		e.recoverAmbiguousCancel(ctx, c, fp, err)
		return
	}
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
func (e *Executor) handleSellStatus(ctx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
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
	if e.paceSend(ctx, c.ExchangeCode) != nil {
		return // cancelled while pacing: no read sent, request stays claimable
	}
	sendCtx, cancel := e.sendContext(ctx, c)
	defer cancel()
	st, err := client.GetOrder(sendCtx, fp.ExchangeOrderID)
	e.noteRateLimit(c.ExchangeCode, err) // PR20 #4: park the exchange on a throttled read
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
	// A cancel (or an ambiguous mutation) never releases the lock — DisposeDeniedMutation
	// (KindCancel) marks the request DEAD and the ACTUAL order+cycle NEEDS_RECONCILE, lock HELD.
	e.disposeDenied(ctx, c, orders.KindCancel, true, cause)
}

func (e *Executor) failTx(ctx context.Context, id int64, cause string) {
	_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
		return e.q.MarkFailed(ctx, tx, id, cause)
	})
}

// finalizeMalformedMutation resolves a malformed mutating request (missing order/cycle) WITHOUT
// stranding a cycle (PR20 correction #1): request DEAD, and if a cycle is known that cycle →
// NEEDS_RECONCILE with its lock HELD. Claiming already refuses these rows, so this is a
// defensive/sweep path — but it must never leave the cycle open.
func (e *Executor) finalizeMalformedMutation(ctx context.Context, c queue.Claimed, cause string) {
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		return orders.DisposeMalformedMutation(ctx, tx, e.q, c.ID, c.CycleID, cause)
	})
	if txErr != nil && e.log != nil {
		e.log.Warn("finalize malformed mutation tx failed (rolled back)", "id", c.ID, "err", txErr)
	}
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
	// Do NOT create a probe the claim loop could never pick up (round 11): if the exchange lost its
	// usable read-only recovery path (disabled or unwired) during the send, reconcile instead —
	// deadReconcile derives the ACTUAL cycle from the order, marks it NEEDS_RECONCILE, lock HELD.
	if !e.usableRecoveryClientByCode(ctx, c.ExchangeCode, c.ExchangeID) {
		e.deadReconcile(ctx, c, "ambiguous place but no usable read-only recovery client — reconcile, no unclaimable probe: "+cause.Error())
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
	// Do NOT create an unclaimable probe (round 11): if the exchange lost its usable recovery path,
	// reconcile instead (deadReconcile derives the ACTUAL cycle from the order, lock HELD).
	if !e.usableRecoveryClientByCode(ctx, c.ExchangeCode, c.ExchangeID) {
		e.deadReconcile(ctx, c, "ambiguous cancel but no usable read-only recovery client — reconcile, no unclaimable probe: "+cause.Error())
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
func (e *Executor) handleAmbiguousPlaceProbe(ctx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, "place probe missing order/cycle context")
		return
	}
	info, ierr := e.orderRecoveryInfo(ctx, *c.OrderID)
	if ierr != nil {
		e.deadReconcile(ctx, c, "place probe: cannot read order: "+ierr.Error())
		return
	}
	if e.paceSend(ctx, c.ExchangeCode) != nil {
		return // cancelled while pacing: no read sent, request stays claimable
	}
	sendCtx, cancel := e.sendContext(ctx, c)
	defer cancel()
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
	e.noteRateLimit(c.ExchangeCode, err) // PR20 #4: park the exchange on a throttled probe
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
func (e *Executor) handleAmbiguousCancelProbe(ctx context.Context, c queue.Claimed, fp orders.FollowupPayload, client exchanges.PrivateClient) {
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

	if e.paceSend(ctx, c.ExchangeCode) != nil {
		return // cancelled while pacing: no read sent, request stays claimable
	}
	sendCtx, cancel := e.sendContext(ctx, c)
	defer cancel()
	st, err := client.GetOrder(sendCtx, fp.ExchangeOrderID)
	e.noteRateLimit(c.ExchangeCode, err) // PR20 #4: park the exchange on a throttled read
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
	if e.recoverySkip() {
		return // an `off` executor owns no cycle mode and must finalize nothing
	}
	// Discover stale IN_FLIGHT mutations by their AUTHORITATIVE order (round 9 #1/#2): join the
	// persisted order and its cycle, and scope by the ORDER's cycle mode — NEVER the (untrusted)
	// queue cycle_id/exchange_id. This finds rows the old per-exchange/claimed-cycle filter hid (a
	// NULL claimed cycle, an unwired or foreign claimed exchange, a cross-mode claimed cycle) and
	// can never mis-route them across the dry/live boundary. NULL-order rows have no order to key on
	// and are handled by sweepMalformedMutations instead.
	q := `
		SELECT er.id, er.request_type, er.cycle_id, er.order_id, er.exchange_id,
		       COALESCE(er.symbol,''), er.payload, er.inflight_at,
		       o.cycle_id, o.exchange_id, c.dry_run
		FROM exchange_requests er
		JOIN orders o ON o.id = er.order_id
		JOIN cycles c ON c.id = o.cycle_id
		WHERE er.status='IN_FLIGHT' AND er.request_type IN ('PLACE_ORDER','CANCEL_ORDER')
		  AND er.inflight_at IS NOT NULL
		  AND er.inflight_at < (NOW(6) - INTERVAL (er.timeout_ms/1000 + ?) SECOND)`
	args := []any{graceSeconds}
	if dry := e.claimDryRunFilter(); dry != nil {
		q += ` AND c.dry_run = ?`
		d := 0
		if *dry {
			d = 1
		}
		args = append(args, d)
	}
	q += ` ORDER BY er.id LIMIT 500`
	rows, err := e.store.DB().QueryContext(ctx, q, args...)
	if err != nil {
		if e.log != nil {
			e.log.Warn("recover stale mutating: query failed", "err", err)
		}
		return
	}
	var stale []staleMutation
	for rows.Next() {
		var s staleMutation
		var t string
		if err := rows.Scan(&s.id, &t, &s.cycleID, &s.orderID, &s.claimedExchangeID, &s.symbol, &s.payload, &s.inflightAt,
			&s.actualCycleID, &s.actualExchangeID, &s.actualDry); err != nil {
			rows.Close()
			return
		}
		s.typ = queue.RequestType(t)
		s.hasActual = true
		stale = append(stale, s)
	}
	rows.Close()
	for _, s := range stale {
		e.recoverOneStaleMutation(ctx, s.claimedExchangeID, s)
	}
}

type staleMutation struct {
	id                int64
	typ               queue.RequestType
	cycleID           sql.NullInt64
	orderID           sql.NullInt64
	claimedExchangeID int64 // er.exchange_id (the CLAIMED exchange — validated vs the order's, never trusted)
	symbol            string
	payload           []byte
	inflightAt        time.Time // first-seen (used to bound retryable recovery — PR20 correction #4)
	// AUTHORITATIVE ownership RETAINED from the discovery JOIN of the persisted order (round 10 #2):
	// if the order row later becomes unreadable (persistent orderRecoveryInfo failure), these — and
	// NEVER the queue row's claimed cycle_id — are what conservative finalization may touch.
	hasActual        bool
	actualCycleID    int64 // o.cycle_id at discovery
	actualExchangeID int64 // o.exchange_id at discovery
	actualDry        bool  // the order's cycle mode at discovery
}

// sweepMalformedMutations finalizes non-terminal MUTATING requests that Claim refuses or must not
// send: malformed (NULL order_id or cycle_id) OR inconsistent (a valid order whose claimed
// cycle/exchange does not match the order's). Ownership AND execution mode are derived from the
// AUTHORITATIVE order when present, else the claimed cycle — NEVER trusted from the queue row
// (round 9 #1/#3):
//   - valid order → DisposeDeniedMutation (derives the cycle from the order; a mismatch or a NULL
//     claimed cycle → conservative: request DEAD, the ACTUAL order+cycle NEEDS_RECONCILE, lock
//     HELD, any unrelated cycle/lock untouched), scoped to the ORDER's cycle mode;
//   - no order but a valid cycle → DisposeMalformedMutation (request DEAD, that cycle
//     NEEDS_RECONCILE, lock HELD), scoped to the CLAIMED cycle's mode;
//   - neither ownership relationship trustworthy → request DEAD only; NO cycle/lock is touched.
//
// One instance finalizes each row (FOR UPDATE SKIP LOCKED). An `off` executor finalizes nothing;
// a live/dry-run executor finalizes ONLY rows whose authoritative cycle matches its mode, so the
// two can never mutate each other's cycles or locks.
func (e *Executor) sweepMalformedMutations(ctx context.Context, limit int) {
	if e.recoverySkip() {
		return
	}
	dry := e.claimDryRunFilter() // nil (unset test mode) = unscoped
	// The AUTHORITATIVE execution-mode filter is applied INSIDE the SQL, BEFORE the LIMIT (round 10
	// #1): a Go-side skip after `LIMIT n` would let n rows of the OTHER mode fill the window on every
	// sweep and permanently starve this executor's own rows. Mode is derived from the order's cycle
	// when the order exists, else from the claimed cycle; a row with neither ownership is
	// mode-independent (only the request itself is finalized, so any executor may take it).
	// ORDER BY er.id keeps the scan deterministic so a bounded LIMIT always makes progress.
	q := `
		SELECT er.id, er.order_id, er.cycle_id, er.exchange_id, oc.dry_run, rc.dry_run
		FROM exchange_requests er
		LEFT JOIN orders o  ON o.id = er.order_id
		LEFT JOIN cycles oc ON oc.id = o.cycle_id
		LEFT JOIN cycles rc ON rc.id = er.cycle_id
		WHERE er.request_type IN ('PLACE_ORDER','CANCEL_ORDER')
		  AND er.status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT')
		  AND (er.order_id IS NULL OR er.cycle_id IS NULL
		       OR er.cycle_id <> o.cycle_id OR er.exchange_id <> o.exchange_id)`
	var args []any
	if dry != nil {
		d := 0
		if *dry {
			d = 1
		}
		q += `
		  AND ((o.id IS NOT NULL AND oc.dry_run = ?)
		       OR (o.id IS NULL AND rc.id IS NOT NULL AND rc.dry_run = ?)
		       OR (o.id IS NULL AND rc.id IS NULL))`
		args = append(args, d, d)
	}
	q += ` ORDER BY er.id LIMIT ?`
	args = append(args, limit)
	rows, err := e.store.DB().QueryContext(ctx, q, args...)
	if err != nil {
		if e.log != nil {
			e.log.Warn("sweep malformed mutations: query failed", "err", err)
		}
		return
	}
	type mal struct {
		id                         int64
		orderID, cycleID           sql.NullInt64
		exchangeID                 int64
		orderCycleDry, reqCycleDry sql.NullBool
	}
	var found []mal
	for rows.Next() {
		var m mal
		if err := rows.Scan(&m.id, &m.orderID, &m.cycleID, &m.exchangeID, &m.orderCycleDry, &m.reqCycleDry); err != nil {
			rows.Close()
			return
		}
		found = append(found, m)
	}
	rows.Close()

	modeMatches := func(cycleDry sql.NullBool) bool {
		if dry == nil {
			return true // unscoped (test harness)
		}
		return cycleDry.Valid && cycleDry.Bool == *dry
	}

	for _, m := range found {
		var run func(tx *sql.Tx) error
		var logMsg string
		switch {
		case m.orderID.Valid && m.orderCycleDry.Valid:
			if !modeMatches(m.orderCycleDry) {
				continue // another executor's mode owns this order's cycle
			}
			run = func(tx *sql.Tx) error {
				return orders.DisposeDeniedMutation(ctx, tx, e.q, orders.DenialParams{
					RequestID: m.id, OrderID: m.orderID.Int64,
					ClaimedCycleID: nullOr0(m.cycleID), ClaimedExchangeID: m.exchangeID,
					Kind: orders.KindUnknown, RequestDead: true, BroadTerminal: true,
					Cause: "malformed/inconsistent mutating request — cycle derived from order, cannot be sent",
				})
			}
			logMsg = "malformed/inconsistent mutating request finalized (cycle derived from order; DEAD + reconcile, lock held)"
		case m.cycleID.Valid && m.reqCycleDry.Valid:
			if !modeMatches(m.reqCycleDry) {
				continue
			}
			run = func(tx *sql.Tx) error {
				return orders.DisposeMalformedMutation(ctx, tx, e.q, m.id, &m.cycleID.Int64,
					"malformed mutating request (no order_id) — cannot be sent or recovered")
			}
			logMsg = "malformed mutating request finalized DEAD (no order_id; cycle needs reconcile, lock held)"
		default:
			// Neither the order nor the claimed cycle is trustworthy → resolve the request only.
			run = func(tx *sql.Tx) error {
				return e.q.MarkDeadMalformed(ctx, tx, m.id,
					"malformed mutating request (no trustworthy order/cycle) — DEAD, no cycle to reconcile")
			}
			logMsg = "malformed mutating request finalized DEAD (no trustworthy ownership; no cycle touched)"
		}

		txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
			var gotID int64
			if err := tx.QueryRowContext(ctx,
				"SELECT id FROM exchange_requests WHERE id=? AND status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT') FOR UPDATE SKIP LOCKED",
				m.id).Scan(&gotID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return nil // already handled by another instance
				}
				return err
			}
			return run(tx)
		})
		if e.log != nil {
			if txErr != nil {
				e.log.Warn("finalize malformed mutation failed", "id", m.id, "err", txErr)
			} else {
				e.log.Error(logMsg, "id", m.id)
			}
		}
	}
}

func (e *Executor) recoverOneStaleMutation(ctx context.Context, claimedExchangeID int64, s staleMutation) {
	// PR20 correction #4: a stale mutating IN_FLIGHT request must ALWAYS reach a terminal
	// decision — either a persisted read-only probe, or a conservative DEAD + NEEDS_RECONCILE
	// with the lock HELD. There is NO silent return that leaves it IN_FLIGHT forever.
	// With NO order id there is nothing to key on or derive from — finalize the queue row (and its
	// cycle, if the row names one) conservatively. This is the only genuinely un-derivable case.
	if !s.orderID.Valid {
		var cyc *int64
		if s.cycleID.Valid {
			cyc = &s.cycleID.Int64
		}
		e.finalizeStaleConservative(ctx, s.id, cyc,
			"stale malformed mutation (no order id) — DEAD + reconcile (lock held)")
		return
	}

	// Load the AUTHORITATIVE order: its OWN cycle_id, exchange_id, role and state (round 8 #4).
	// The cycle and exchange a probe may touch come from HERE, never from the queue-row metadata.
	info, err := e.orderRecoveryInfo(ctx, s.orderID.Int64)
	if e.cfg.hookOrderRecoveryInfoErr != nil {
		if herr := e.cfg.hookOrderRecoveryInfoErr(s.orderID.Int64); herr != nil {
			info, err = recoveryInfo{}, herr
		}
	}
	if err != nil {
		// The re-read failed. Conservative finalization must touch ONLY the AUTHORITATIVE ownership
		// RETAINED from the discovery JOIN (round 10 #2) — with a persisted order the queue row's
		// claimed cycle_id must NEVER be used to mutate cycle/order/lock state (it may point at an
		// unrelated cycle B while the order belongs to cycle A).
		if errors.Is(err, sql.ErrNoRows) {
			// The order row is genuinely gone (retry cannot help). No order state exists to change;
			// reconcile the RETAINED actual cycle (else, for a direct-call row with no retained
			// ownership, only the request).
			var cyc *int64
			if s.hasActual {
				cyc = &s.actualCycleID
			}
			e.finalizeStaleConservative(ctx, s.id, cyc,
				"stale mutation: order row missing — DEAD + reconcile of the ACTUAL cycle (lock held)")
			return
		}
		if e.staleRecoveryExpired(s.inflightAt) {
			// TEMPORARY failure that persisted past the recovery hard limit → finalize on the
			// RETAINED authoritative order + cycle: request DEAD, ACTUAL order + cycle
			// NEEDS_RECONCILE, lock HELD; the claimed cycle stays untouched.
			e.finalizeStaleAuthoritative(ctx, s,
				"stale mutation: order recovery info unavailable past the recovery limit ("+err.Error()+") — DEAD + ACTUAL order/cycle reconcile (lock held)")
			return
		}
		if e.log != nil {
			e.log.Warn("stale mutation: order recovery info temporarily unavailable — will retry next sweep",
				"id", s.id, "err", err)
		}
		return
	}

	// OWNERSHIP PROOF (round 8 #4): the queue row's claimed cycle (when present) and exchange MUST
	// equal the ORDER's own. A mismatch — or a malformed row with a NULL cycle_id — is resolved by
	// the AUTHORITATIVE disposition, which derives the real cycle from the ORDER, marks the request
	// DEAD, the ACTUAL order + cycle NEEDS_RECONCILE, holds the lock, and never touches any
	// unrelated cycle or lock. No probe is ever created from mixed ownership.
	cycleMismatch := s.cycleID.Valid && s.cycleID.Int64 != info.cycleID
	exchangeMismatch := claimedExchangeID != info.exchangeID
	if !s.cycleID.Valid || cycleMismatch || exchangeMismatch {
		claimedCycle := int64(0) // 0 → "no claim to verify"; DisposeDeniedMutation derives it from the order
		if s.cycleID.Valid {
			claimedCycle = s.cycleID.Int64
		}
		cause := "stale mutation ownership mismatch (request cycle/exchange != order's) — DEAD + reconcile (lock held)"
		if !s.cycleID.Valid {
			cause = "stale mutation with NULL cycle_id — cycle derived from order, DEAD + reconcile (lock held)"
		}
		txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
			// Single-finalizer guard: exactly one instance converts each stale row.
			var gotID int64
			if err := tx.QueryRowContext(ctx,
				"SELECT id FROM exchange_requests WHERE id=? AND status='IN_FLIGHT' FOR UPDATE SKIP LOCKED", s.id).Scan(&gotID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return nil // already handled by another instance
				}
				return err
			}
			// KindUnknown → always conservative (never releases the lock): a stale IN_FLIGHT mutation
			// may have reached the venue, so exposure can never be ruled out.
			return orders.DisposeDeniedMutation(ctx, tx, e.q, orders.DenialParams{
				RequestID: s.id, OrderID: s.orderID.Int64,
				ClaimedCycleID: claimedCycle, ClaimedExchangeID: claimedExchangeID,
				Kind: orders.KindUnknown, RequestDead: true, Cause: cause,
			})
		})
		if e.log != nil {
			if txErr != nil {
				e.log.Warn("recover stale mutating: ownership disposition failed", "id", s.id, "err", txErr)
			} else {
				e.log.Error("stale mutating request finalized on ownership mismatch (DEAD; ACTUAL order/cycle NEEDS_RECONCILE; lock held)",
					"id", s.id, "claimed_cycle", claimedCycle, "order_cycle", info.cycleID, "order_exchange", info.exchangeID)
			}
		}
		return
	}

	// A recovery probe must be CLAIMABLE (round 10 #3): the ORDER's exchange needs a wired client
	// in this executor AND `exchanges.enabled = 1` (Claim refuses disabled exchanges). Without both,
	// a GET_ORDER probe would sit QUEUED forever — order/cycle unresolved, lock held indefinitely.
	// In that case, finalize conservatively on the AUTHORITATIVE ownership instead: request DEAD,
	// ACTUAL order + cycle NEEDS_RECONCILE, lock HELD (manual reconciliation via the runbook).
	if !e.usableRecoveryClient(ctx, info.exchangeID) {
		e.finalizeStaleAuthoritative(ctx, s,
			"stale mutation: no usable read-only recovery client for the order's exchange — DEAD + reconcile (lock held), no unclaimable probe")
		return
	}

	// Ownership PROVEN consistent → schedule the read-only recovery probe, using the ORDER's
	// authoritative cycle and exchange (equal to the verified claimed ones, but sourced from the order).
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
			ExchangeID: info.exchangeID, Symbol: s.symbol, CycleID: info.cycleID, OrderID: s.orderID.Int64,
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
				// Cannot probe a cancel without an order id — mark DEAD + NEEDS_RECONCILE atomically,
				// on the ORDER's authoritative cycle.
				if err := orders.MarkNeedsReconcile(ctx, tx, s.orderID.Int64, &info.cycleID, "stale cancel with no exchange id — needs reconcile"); err != nil {
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
// to look the order up by (exchange id, the EXACT client id sent, the local id), the immutable
// fields to VERIFY a recovered order really is this order before attaching it, and — for stale
// recovery — the order's OWN authoritative cycle_id and exchange_id, which are the source of truth
// for which cycle/exchange a probe may touch (never the queue-row metadata; PR20 correction round
// 8 #4).
type recoveryInfo struct {
	role               string
	exchangeOrderID    string
	clientOrderIDSent  string
	localClientOrderID string
	side               string
	quantity           decimal.Decimal
	cycleID            int64  // AUTHORITATIVE — the order's own cycle
	exchangeID         int64  // AUTHORITATIVE — the order's own exchange
	orderState         string // the order's current state (diagnostic)
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

// finalizeStaleConservative marks a stale mutation DEAD and pushes its cycle to NEEDS_RECONCILE
// (lock HELD), atomically and with a single finalizer (FOR UPDATE SKIP LOCKED) so only one
// instance resolves each stale request (PR20 correction #4).
func (e *Executor) finalizeStaleConservative(ctx context.Context, requestID int64, cycleID *int64, cause string) {
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		var gotID int64
		if err := tx.QueryRowContext(ctx,
			"SELECT id FROM exchange_requests WHERE id=? AND status='IN_FLIGHT' FOR UPDATE SKIP LOCKED", requestID).Scan(&gotID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil // another instance already finalized it
			}
			return err
		}
		return orders.DisposeMalformedMutation(ctx, tx, e.q, requestID, cycleID, cause)
	})
	if e.log != nil {
		if txErr != nil {
			e.log.Warn("finalize stale mutation failed", "id", requestID, "err", txErr)
		} else {
			e.log.Error("stale mutating request finalized conservatively (DEAD + cycle NEEDS_RECONCILE, lock held)",
				"id", requestID, "cause", cause)
		}
	}
}

// finalizeStaleAuthoritative resolves a stale mutation whose order re-read kept failing past the
// recovery hard limit, using ONLY the authoritative ownership RETAINED from the discovery JOIN
// (round 10 #2): request → DEAD, the ACTUAL order + ACTUAL cycle → NEEDS_RECONCILE, lock HELD. The
// queue row's claimed cycle_id is never touched — with a persisted order it is untrusted metadata
// that may point at an unrelated cycle. Single finalizer (FOR UPDATE SKIP LOCKED); if this tx also
// fails (the DB is still unhealthy) the next sweep retries the same bounded finalization.
func (e *Executor) finalizeStaleAuthoritative(ctx context.Context, s staleMutation, cause string) {
	if !s.hasActual || !s.orderID.Valid {
		// No retained ownership (direct-call row) — fall back to request-only finalization.
		e.finalizeStaleConservative(ctx, s.id, nil, cause)
		return
	}
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		var gotID int64
		if err := tx.QueryRowContext(ctx,
			"SELECT id FROM exchange_requests WHERE id=? AND status='IN_FLIGHT' FOR UPDATE SKIP LOCKED", s.id).Scan(&gotID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil // another instance already finalized it
			}
			return err
		}
		if err := orders.MarkNeedsReconcile(ctx, tx, s.orderID.Int64, &s.actualCycleID, cause); err != nil {
			return err
		}
		return e.q.MarkDead(ctx, tx, s.id, cause)
	})
	if e.log != nil {
		if txErr != nil {
			e.log.Warn("finalize stale mutation (authoritative) failed — will retry next sweep", "id", s.id, "err", txErr)
		} else {
			e.log.Error("stale mutating request finalized on RETAINED authoritative ownership (DEAD; ACTUAL order/cycle NEEDS_RECONCILE; lock held)",
				"id", s.id, "order", s.orderID.Int64, "actual_cycle", s.actualCycleID)
		}
	}
}

// usableRecoveryClient reports whether the given exchange has a USABLE read-only recovery path in
// THIS executor: a wired client (present in the client map, so the claim loop iterates it) AND
// `exchanges.enabled = 1` (Claim's SQL refuses disabled exchanges). A GET_ORDER probe scheduled
// without both would sit QUEUED forever — unclaimable — leaving the order/cycle unresolved and the
// lock held indefinitely (round 10 #3). A DB error checking `enabled` fails CLOSED (not usable →
// the caller finalizes conservatively instead of queueing an unclaimable probe).
func (e *Executor) usableRecoveryClient(ctx context.Context, exchangeID int64) bool {
	wired := false
	for code, id := range e.exIDs {
		if id == exchangeID {
			_, wired = e.clients[code]
			break
		}
	}
	if !wired {
		return false
	}
	var enabled bool
	if err := e.store.DB().QueryRowContext(ctx,
		"SELECT enabled FROM exchanges WHERE id=?", exchangeID).Scan(&enabled); err != nil {
		return false
	}
	return enabled
}

// usableRecoveryClientByCode reports whether the exchange identified by `code` has a USABLE
// read-only recovery path: a wired client in this executor AND `exchanges.enabled = 1`. The
// ambiguous-recovery paths use this because they know the exact code they just sent through, so the
// check does not depend on the resolved e.exIDs map. Fails closed on a DB error.
func (e *Executor) usableRecoveryClientByCode(ctx context.Context, code string, exchangeID int64) bool {
	if _, ok := e.clients[code]; !ok {
		return false
	}
	var enabled bool
	if err := e.store.DB().QueryRowContext(ctx, "SELECT enabled FROM exchanges WHERE id=?", exchangeID).Scan(&enabled); err != nil {
		return false
	}
	return enabled
}

// usableRecoveryExchangeIDs is the set of exchange ids with a wired client in THIS executor AND
// `exchanges.enabled = 1` — i.e. exchanges whose read-only recovery probes CAN be claimed. It is
// the positive complement used by sweepUnclaimableRecoveryProbes to SQL-filter (before LIMIT) to
// only the probes that can NEVER be claimed. Exchanges whose `enabled` cannot be read are omitted
// (fail closed → their probes are treated as unclaimable).
func (e *Executor) usableRecoveryExchangeIDs(ctx context.Context) map[int64]bool {
	out := make(map[int64]bool, len(e.exIDs))
	for code, id := range e.exIDs {
		if _, ok := e.clients[code]; !ok {
			continue
		}
		var enabled bool
		if err := e.store.DB().QueryRowContext(ctx, "SELECT enabled FROM exchanges WHERE id=?", id).Scan(&enabled); err == nil && enabled {
			out[id] = true
		}
	}
	return out
}

// sweepUnclaimableRecoveryProbes finalizes non-terminal GET_ORDER recovery probes that can NEVER be
// claimed because the ORDER's exchange has no usable read-only recovery path in the executor that
// owns them — the exchange is disabled (`enabled=0`, which Claim's SQL refuses) or has no wired
// client (removed credential, or a restart that did not construct it). The creation-time check
// (usableRecoveryClient) cannot cover this: an exchange can become unusable AFTER a probe is already
// committed as QUEUED, and neither the claim loop (it never iterates a missing client) nor
// SweepStuck (it only handles stale CLAIMED/IN_FLIGHT) would ever resolve it, so the probe — and the
// order/cycle/lock behind it — would sit stuck forever (round 11).
//
// Ownership is derived from the PERSISTED order (o.cycle_id / o.exchange_id), never the queue row's
// claimed metadata, and the sweep is scoped by the AUTHORITATIVE execution mode of the order's cycle
// INSIDE the SQL, before LIMIT (so the other mode's probes cannot starve this one — round 10 #1).
// Each proven-unclaimable probe is finalized atomically: probe → DEAD, ACTUAL order + cycle →
// NEEDS_RECONCILE, lock HELD; unrelated cycles/locks are never touched. Idempotent and safe to run
// repeatedly (single finalizer via FOR UPDATE SKIP LOCKED; already-terminal rows are skipped). An
// `off` executor sweeps nothing.
func (e *Executor) sweepUnclaimableRecoveryProbes(ctx context.Context, limit int) {
	if e.recoverySkip() {
		return
	}
	usable := e.usableRecoveryExchangeIDs(ctx)
	var b strings.Builder
	b.WriteString(`
		SELECT er.id, er.order_id, o.cycle_id, o.exchange_id
		FROM exchange_requests er
		JOIN orders o ON o.id = er.order_id
		JOIN cycles c ON c.id = o.cycle_id
		WHERE er.request_type='GET_ORDER'
		  AND er.status IN ('QUEUED','RETRY_SCHEDULED')`)
	var args []any
	if dry := e.claimDryRunFilter(); dry != nil {
		b.WriteString(` AND c.dry_run = ?`)
		d := 0
		if *dry {
			d = 1
		}
		args = append(args, d)
	}
	// UNCLAIMABLE = the ORDER's exchange is not in the usable set (disabled or unwired). The empty
	// set means "nothing is usable" → every probe qualifies, so the NOT IN clause is simply omitted.
	if len(usable) > 0 {
		b.WriteString(` AND o.exchange_id NOT IN (`)
		first := true
		for id := range usable {
			if !first {
				b.WriteString(",")
			}
			b.WriteString("?")
			args = append(args, id)
			first = false
		}
		b.WriteString(`)`)
	}
	b.WriteString(` ORDER BY er.id LIMIT ?`)
	args = append(args, limit)

	rows, err := e.store.DB().QueryContext(ctx, b.String(), args...)
	if err != nil {
		if e.log != nil {
			e.log.Warn("sweep unclaimable recovery probes: query failed", "err", err)
		}
		return
	}
	type probe struct{ id, orderID, cycleID, exchangeID int64 }
	var found []probe
	for rows.Next() {
		var p probe
		if err := rows.Scan(&p.id, &p.orderID, &p.cycleID, &p.exchangeID); err != nil {
			rows.Close()
			return
		}
		found = append(found, p)
	}
	rows.Close()
	for _, p := range found {
		e.finalizeUnclaimableProbe(ctx, p.id, p.orderID, p.cycleID, p.exchangeID)
	}
}

// finalizeUnclaimableProbe resolves one proven-unclaimable GET_ORDER probe on the AUTHORITATIVE
// order + cycle: probe → DEAD, order + cycle → NEEDS_RECONCILE, lock HELD. It re-verifies the
// exchange is still unusable (a race could have re-enabled/wired it) and, if it became usable,
// leaves the probe QUEUED for the claim loop. Single finalizer (FOR UPDATE SKIP LOCKED).
func (e *Executor) finalizeUnclaimableProbe(ctx context.Context, probeID, orderID, actualCycleID, actualExchangeID int64) {
	if e.usableRecoveryClient(ctx, actualExchangeID) {
		return // became claimable between discovery and now — leave it for the claim loop
	}
	cause := "recovery probe unclaimable — order's exchange has no usable read-only recovery client (disabled/unwired) — DEAD + ACTUAL order/cycle reconcile (lock held)"
	txErr := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		var gotID int64
		if err := tx.QueryRowContext(ctx,
			"SELECT id FROM exchange_requests WHERE id=? AND status IN ('QUEUED','RETRY_SCHEDULED') FOR UPDATE SKIP LOCKED", probeID).Scan(&gotID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil // already handled / claimed by another instance
			}
			return err
		}
		if err := orders.MarkNeedsReconcile(ctx, tx, orderID, &actualCycleID, cause); err != nil {
			return err
		}
		return e.q.MarkDeadMalformed(ctx, tx, probeID, cause)
	})
	if e.log != nil {
		if txErr != nil {
			e.log.Warn("finalize unclaimable recovery probe failed — will retry next sweep", "id", probeID, "err", txErr)
		} else {
			e.log.Error("unclaimable GET_ORDER recovery probe finalized (DEAD; ACTUAL order/cycle NEEDS_RECONCILE; lock held)",
				"id", probeID, "order", orderID, "actual_cycle", actualCycleID, "actual_exchange", actualExchangeID)
		}
	}
}

// staleRecoveryExpired reports whether a request has been stuck IN_FLIGHT past the recovery hard
// limit, so a persistently-failing recovery is bounded and eventually finalized rather than
// retried forever (PR20 correction #4). The bound is the recovery TotalTimeout (min 5m).
func (e *Executor) staleRecoveryExpired(inflightAt time.Time) bool {
	limit := e.cfg.Recovery.TotalTimeout
	if limit < 5*time.Minute {
		limit = 5 * time.Minute
	}
	return !inflightAt.IsZero() && time.Since(inflightAt) > limit
}

func (e *Executor) orderRecoveryInfo(ctx context.Context, orderID int64) (recoveryInfo, error) {
	var (
		info  recoveryInfo
		exoid sql.NullString
		sent  sql.NullString
		qtyS  string
	)
	err := e.store.DB().QueryRowContext(ctx,
		"SELECT role, COALESCE(exchange_order_id,''), COALESCE(client_order_id_sent,''), local_client_order_id, side, quantity, cycle_id, exchange_id, state FROM orders WHERE id=?",
		orderID).Scan(&info.role, &exoid, &sent, &info.localClientOrderID, &info.side, &qtyS, &info.cycleID, &info.exchangeID, &info.orderState)
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

// denyPlace resolves a PLACE that the live gate refused, through the OFFICIAL state path
// (PR20 correction #1). Nothing was sent, but the disposition differs by side because the
// RISK differs:
//   - buy  → rejectBuyCleanly: no exposure exists, so request FAILED + order/cycle FAILED +
//     symbol lock RELEASED. Leaving the order QUEUED with no executable request would strand
//     the cycle and hold the lock forever.
//   - sell → rejectSellToReconcile: real inventory may already exist, so the lock is HELD and
//     order/cycle go to NEEDS_RECONCILE — the position stays visible and recoverable, never
//     abandoned.
//
// Without order/cycle context there is nothing to transition, so the request is simply failed.
func (e *Executor) denyPlace(ctx context.Context, c queue.Claimed, side, cause string) {
	if c.OrderID == nil || c.CycleID == nil {
		e.failTx(ctx, c.ID, cause)
		return
	}
	if side == "sell" {
		e.rejectSellToReconcile(ctx, c, cause)
		return
	}
	e.rejectBuyCleanly(ctx, c, cause)
}

// denyCancel resolves a CANCEL the live gate refused (PR20 correction #1). The order may be
// OPEN on the venue, so failing only the queue row would abandon it: the request is DEAD and
// the order + cycle go to NEEDS_RECONCILE with the symbol lock HELD (deadReconcile), keeping
// the potentially-open order under explicit management for the reconciler/operator.
func (e *Executor) denyCancel(ctx context.Context, c queue.Claimed, cause string) {
	if c.OrderID == nil {
		e.failTx(ctx, c.ID, cause)
		return
	}
	e.deadReconcile(ctx, c, cause)
}

// liveGatePlace is the FINAL live safety gate before a real PLACE send. In non-live
// modes (off/dry_run) no REAL exchange mutation is possible (only simulated/no clients are
// wired), so the gate is a no-op. In live mode it consults the Guard (caps/kill-switch/
// live-flags/credentials/dry-run/state); a denial fails the request (audited) and returns
// false so the send is skipped. PR20 correction: live mode with a NIL Guard FAILS CLOSED —
// a live executor that somehow lost its guard must deny every place, never silently allow.
// placePayload is the EXACT set of values the executor is about to send to the venue, taken
// from the queued mutation payload. The guard proves each against the persisted order before
// the send (PR20 correction #5) — the payload is never authoritative on its own.
type placePayload struct {
	Side               string
	Quantity           decimal.Decimal
	LimitPrice         decimal.Decimal
	OrderType          string
	TimeInForce        string
	LocalClientOrderID string // the intent's local id (matches DB local_client_order_id)
	ClientOrderIDSent  string // the EXACT normalized value to send (empty at the early guard)
}

// buildPlacePayload lifts the exact venue request into the values the guard proves against the
// persisted order. `localCOID` is the intent's local id; `sentCOID` is the adapter-normalized
// value that will actually be sent (empty for the early pre-pacing guard, set for the final
// guard once persisted). Deriving from execution.OrderRequest guarantees the guard checks
// precisely what PlaceOrder will send. PR20 correction #5/#6.
func buildPlacePayload(r execution.OrderRequest, localCOID, sentCOID string) placePayload {
	return placePayload{
		Side: r.Side, Quantity: r.Quantity, LimitPrice: r.LimitPrice, OrderType: r.OrderType,
		TimeInForce: r.TimeInForce, LocalClientOrderID: localCOID, ClientOrderIDSent: sentCOID,
	}
}

// recoveryCtx returns a context for the "definitely not sent" recovery DB work. If the parent
// is already cancelled (e.g. the send context expired during shutdown), it DETACHES to a fresh
// bounded context so the recovery still persists — otherwise the very recovery of a cancelled
// send would itself be cancelled and strand the request IN_FLIGHT (PR20 correction #3).
func (e *Executor) recoveryCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// notSentBeforePlace handles a definitely-not-sent PLACE (pre-network failure or an already-
// cancelled send context) — the mutation never reached the venue, so it is NEVER an ambiguous
// outcome and never a recovery probe (PR20 correction #3). A TEMPORARY failure (e.g. credential
// DB blip, cancelled context during shutdown) uses the ONLY sanctioned mutating retry — the
// atomic, status-guarded RequeueProvenUnexecuted (proof of non-execution) — for a bounded retry
// via the persisted queue rules. A PERMANENT failure (invalid symbol/request/client id) is a
// terminal local failure with the correct entry/exit disposition: a buy releases the lock only
// via the exposure proof (rejectBuyCleanly → OnBuyDenied); a sell holds the lock (inventory).
func (e *Executor) notSentBeforePlace(ctx context.Context, c queue.Claimed, isSell bool, cause error, claimed bool) {
	ctx, cancel := e.recoveryCtx(ctx)
	defer cancel()
	if execution.IsNotSentPermanent(cause) {
		if isSell {
			e.rejectSellToReconcile(ctx, c, "sell not sent (permanent pre-execution failure): "+cause.Error())
		} else {
			e.rejectBuyCleanly(ctx, c, "buy not sent (permanent pre-execution failure): "+cause.Error())
		}
		return
	}
	// Temporary: definitely-unsent → bounded safe retry (backoff/cooldown; never a blind
	// resend, never a probe). `claimed` selects the status guard (CLAIMED pre-MarkInFlight vs
	// IN_FLIGHT after). Kind selects the exhaustion disposition.
	kind := orders.KindEntryBuy
	if isSell {
		kind = orders.KindExitSell
	}
	e.requeueUnsent(ctx, c, kind, cause, claimed)
}

// notSentBeforeCancel handles a definitely-not-sent CANCEL. Temporary → the sanctioned
// proven-unexecuted requeue (bounded retry — we still want the remainder cancelled); permanent
// (e.g. an unusable exchange_order_id the adapter rejects locally) → NEEDS_RECONCILE with the
// lock HELD (deadReconcile), never an ambiguous probe (PR20 correction #3).
func (e *Executor) notSentBeforeCancel(ctx context.Context, c queue.Claimed, fp orders.FollowupPayload, cause error, claimed bool) {
	ctx, cancel := e.recoveryCtx(ctx)
	defer cancel()
	if execution.IsNotSentPermanent(cause) {
		e.deadReconcile(ctx, c, "cancel not sent (permanent pre-execution failure): "+cause.Error())
		return
	}
	e.requeueUnsent(ctx, c, orders.KindCancel, cause, claimed)
}

// recordFirstOrderChecklist records the canary session's first-order checklist AFTER the order
// was placed (PR20 correction #2 — it must never run between the final guard and MarkInFlight).
// Best-effort: any failure is swallowed by the guard's own logging.
func (e *Executor) recordFirstOrderChecklist(ctx context.Context, c queue.Claimed) {
	if e.cfg.Guard == nil || c.OrderID == nil || c.CycleID == nil {
		return
	}
	em, err := e.orderMarket(ctx, c.OrderID)
	if err != nil {
		return
	}
	e.cfg.Guard.RecordFirstOrderChecklist(ctx, live.PlaceCheck{
		ExchangeID: c.ExchangeID, ExchangeMarketID: em, ExchangeCode: c.ExchangeCode,
		Symbol: c.Symbol, RequestID: c.ID, OrderID: derefID(c.OrderID), CycleID: derefID(c.CycleID),
	})
}

// sendContext creates the exchange-call timeout context. It is created ONLY at the network
// boundary — after pacing, the final guard, and MarkInFlight (PR20 correction #3) — so the
// configured exchange timeout measures the real venue call, never the internal pacing wait.
func (e *Executor) sendContext(ctx context.Context, c queue.Claimed) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, time.Duration(c.Timeout())*time.Millisecond)
}

// prepareCtx returns a context carrying a pacing hook a two-stage adapter uses to reserve a
// per-exchange slot for ONLY the network calls it actually makes during preparation — e.g. a
// Bitpin auth/refresh call. An adapter that authenticates with an in-memory credential (Nobitex,
// Wallex) or reuses a still-fresh cached token makes no preparation network call and reserves no
// slot here, so pacing reservations equal actual HTTP calls (PR20 correction round 8 #3). The
// order/cancel send is paced separately by the caller immediately before MarkInFlight.
func (e *Executor) prepareCtx(ctx context.Context, code string) context.Context {
	return exchanges.WithNetworkPacer(ctx, func(c context.Context) error { return e.paceSend(c, code) })
}

// earlyGatePlace is the cheap pre-pacing pre-filter (no audit); finalGatePlace is the
// authoritative gate run immediately before MarkInFlight, writing the durable allow-audit that
// reflects the state at send time (PR20 correction #2).
func (e *Executor) earlyGatePlace(ctx context.Context, c queue.Claimed, pay placePayload) bool {
	return e.gatePlace(ctx, c, pay, false)
}
func (e *Executor) finalGatePlace(ctx context.Context, c queue.Claimed, pay placePayload) bool {
	return e.gatePlace(ctx, c, pay, true)
}

func (e *Executor) gatePlace(ctx context.Context, c queue.Claimed, pay placePayload, final bool) bool {
	if e.cfg.ExecutionMode != "live" {
		return true
	}
	// Every denial below routes through denyPlace: a refused request must never be left as
	// a QUEUED order with no executable request (PR20 correction #1).
	side := pay.Side
	if e.cfg.Guard == nil {
		e.denyPlace(ctx, c, side, "live gate: no Guard wired in live mode — fail closed, not sent")
		return false
	}
	// FAIL CLOSED on this exchange's cooldown durability (PR20 correction #4), for ENTRY BUYS
	// ONLY: if an active rate-limit park for THIS exchange cannot be persisted, a restart would
	// forget it and hammer a throttled venue — so we stop CREATING exposure there. Proven exit
	// sells and cancels are risk-reducing and stay available (an outage is never a reason to
	// strand inventory). Other exchanges are unaffected.
	if side == "buy" && !e.cooldownDurable(c.ExchangeCode) {
		e.denyPlace(ctx, c, side, "live gate: exchange cooldown durability lost — new entry buys disabled for this exchange, not sent")
		return false
	}
	// FAIL CLOSED: if the cycle's mode cannot be confirmed we must NOT send. (abortOnModeMismatch
	// already caught this at the top of the handler; this is the defensive belt-and-suspenders.)
	dry, derr := e.cycleDryRun(ctx, c.CycleID)
	if derr != nil {
		e.denyPlace(ctx, c, side, "live gate: cannot confirm cycle mode (not sent): "+derr.Error())
		return false
	}
	// FAIL CLOSED on market resolution (PR20 correction #2): market id 0 is not a safe
	// fallback — it would skip the symbol-level live-enabled check.
	em, merr := e.orderMarket(ctx, c.OrderID)
	if merr != nil {
		e.denyPlace(ctx, c, side, "live gate: cannot resolve the order's market (not sent): "+merr.Error())
		return false
	}
	pc := live.PlaceCheck{
		ExchangeID: c.ExchangeID, ExchangeMarketID: em, ExchangeCode: c.ExchangeCode,
		Symbol: c.Symbol, CycleID: derefID(c.CycleID), OrderID: derefID(c.OrderID), RequestID: c.ID,
		Side: pay.Side, Notional: pay.LimitPrice.Mul(pay.Quantity), BaseQty: pay.Quantity, DryRun: dry,
		LimitPrice: pay.LimitPrice, OrderType: pay.OrderType, TimeInForce: pay.TimeInForce,
		LocalClientOrderID: pay.LocalClientOrderID, ClientOrderIDSent: pay.ClientOrderIDSent,
	}
	var d live.Decision
	if final {
		d = e.cfg.Guard.CheckPlace(ctx, pc) // authoritative: writes the durable allow-audit
	} else {
		d = e.cfg.Guard.CheckPlaceEarly(ctx, pc) // early pre-filter: audits denials only
	}
	if !d.Allow {
		e.denyPlace(ctx, c, side, "live guard denied: "+d.Reason)
		return false
	}
	// PR20 correction #2: the first-order checklist is NO LONGER recorded here. It performs
	// synchronous DB work, and NOTHING synchronous may run between the final guard and
	// MarkInFlight (state could change during it, making the "final" allow stale). It is
	// recorded AFTER the exchange operation instead — see handlePlace.
	return true
}

// liveGateCancel gates a real CANCEL. exchangeOrderID is the venue id the payload targets;
// the guard proves it against the registered order (PR20 correction #3). Every denial routes
// through denyCancel so a possibly-open venue order is never abandoned (correction #1).
// earlyGateCancel is the pre-pacing pre-filter; finalGateCancel is the authoritative gate run
// immediately before MarkInFlight (PR20 correction #2). A cancel is deliberately NOT gated on
// cooldown durability: it reduces risk, and a durability outage is a reason to stop creating
// exposure, never a reason to strand an open order.
func (e *Executor) earlyGateCancel(ctx context.Context, c queue.Claimed, exchangeOrderID string) bool {
	return e.gateCancel(ctx, c, exchangeOrderID, false)
}
func (e *Executor) finalGateCancel(ctx context.Context, c queue.Claimed, exchangeOrderID string) bool {
	return e.gateCancel(ctx, c, exchangeOrderID, true)
}

func (e *Executor) gateCancel(ctx context.Context, c queue.Claimed, exchangeOrderID string, final bool) bool {
	if e.cfg.ExecutionMode != "live" {
		return true
	}
	if e.cfg.Guard == nil {
		e.denyCancel(ctx, c, "live gate: no Guard wired in live mode — fail closed, cancel not sent")
		return false
	}
	dry, derr := e.cycleDryRun(ctx, c.CycleID)
	if derr != nil {
		e.denyCancel(ctx, c, "live gate: cannot confirm cycle mode (cancel not sent): "+derr.Error())
		return false
	}
	pc := live.PlaceCheck{
		ExchangeID: c.ExchangeID, CycleID: derefID(c.CycleID), OrderID: derefID(c.OrderID), RequestID: c.ID,
		ExchangeOrderID: exchangeOrderID, DryRun: dry,
	}
	var d live.Decision
	if final {
		d = e.cfg.Guard.CheckCancel(ctx, pc) // authoritative: audits
	} else {
		d = e.cfg.Guard.CheckCancelEarly(ctx, pc) // early pre-filter: audits denials only
	}
	if !d.Allow {
		e.denyCancel(ctx, c, "live guard denied cancel: "+d.Reason)
		return false
	}
	return true
}

// orderMarket resolves the order's exchange_market_id and FAILS CLOSED (PR20 correction
// #2): a DB error, a missing order, or a NULL/zero market is an ERROR, never a silent 0.
// Market id 0 previously slipped past the symbol-level live-enabled check, so an
// unidentifiable market could reach a real send. The guard independently re-proves the
// market from the order (checkEntryBuyIdentity) — this value must agree with it.
func (e *Executor) orderMarket(ctx context.Context, orderID *int64) (int64, error) {
	if orderID == nil {
		return 0, errors.New("request carries no order context")
	}
	var em sql.NullInt64
	if err := e.store.DB().QueryRowContext(ctx,
		"SELECT exchange_market_id FROM orders WHERE id=?", *orderID).Scan(&em); err != nil {
		return 0, err
	}
	if !em.Valid || em.Int64 == 0 {
		return 0, errors.New("order has no exchange_market_id")
	}
	return em.Int64, nil
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

// recoverySkip reports whether this executor owns NO cycle mode and must therefore finalize
// nothing during malformed/stale recovery (round 9 #3). An `off` executor trades nothing and must
// never mutate a live or dry-run cycle. `live`/`dry_run` own their mode (scoped by
// claimDryRunFilter). An UNSET mode (test harness only — production always sets off/dry_run/live)
// is treated as unscoped: it owns everything, matching the pre-round-9 test behaviour.
func (e *Executor) recoverySkip() bool { return e.cfg.ExecutionMode == "off" }

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

// nullOr0 returns the int64 value of a nullable column, or 0 when NULL. Used by the malformed
// sweep to pass a claimed cycle to DisposeDeniedMutation, which treats 0 as "no cycle to verify —
// derive it from the order" (round 9 #1/#3).
func nullOr0(v sql.NullInt64) int64 {
	if v.Valid {
		return v.Int64
	}
	return 0
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
