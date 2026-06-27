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

	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
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
		var p struct {
			ExchangeOrderID string `json:"exchange_order_id"`
		}
		_ = json.Unmarshal(c.Payload, &p)
		st, err := client.GetOrder(sendCtx, p.ExchangeOrderID)
		e.handleReadOnly(ctx, c, mustJSON(st), err)
	case queue.TypeGetOpenOrders:
		var p struct {
			Symbol string `json:"symbol"`
		}
		_ = json.Unmarshal(c.Payload, &p)
		list, err := client.GetOpenOrders(sendCtx, p.Symbol)
		e.handleReadOnly(ctx, c, mustJSON(list), err)
	case queue.TypePlaceOrder:
		e.handlePlace(ctx, sendCtx, c, client)
	case queue.TypeCancelOrder:
		e.handleCancel(ctx, sendCtx, c, client)
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

// handlePlace processes a PLACE_ORDER. It marks IN_FLIGHT (committed) BEFORE
// sending so a crash is recoverable, then records the outcome conservatively.
func (e *Executor) handlePlace(ctx, sendCtx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	var req execution.OrderRequest
	if err := json.Unmarshal(c.Payload, &req); err != nil {
		e.failTx(ctx, c.ID, "bad PLACE_ORDER payload: "+err.Error())
		return
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		if e.log != nil {
			e.log.Warn("mark in-flight failed; not sending", "id", c.ID, "err", err)
		}
		return
	}

	ack, err := client.PlaceOrder(sendCtx, req)
	if err == nil {
		// Success: complete the request AND advance the order, atomically.
		e.completePlaceSuccess(ctx, c, ack)
		return
	}
	// Conservative outcome classification (rule #6): a definite rejection means
	// the order was NOT placed (safe FAILED); anything ambiguous (timeout, network,
	// 5xx, unknown) must NOT be re-sent — DEAD + order NEEDS_RECONCILE.
	if isDefiniteRejection(err) {
		e.failTx(ctx, c.ID, "place rejected (not placed): "+err.Error())
		return
	}
	e.deadReconcile(ctx, c, "place ambiguous outcome: "+err.Error())
}

// handleCancel processes a CANCEL_ORDER with the same conservative discipline.
// NOTE: the order-state resolution of a successful cancel (e.g. CANCEL_PENDING →
// CANCELLED) is deferred to PR10 order-status processing; PR7 only records the
// queue outcome on success.
func (e *Executor) handleCancel(ctx, sendCtx context.Context, c queue.Claimed, client exchanges.PrivateClient) {
	var p struct {
		ExchangeOrderID string `json:"exchange_order_id"`
	}
	if err := json.Unmarshal(c.Payload, &p); err != nil {
		e.failTx(ctx, c.ID, "bad CANCEL_ORDER payload: "+err.Error())
		return
	}
	if err := e.q.MarkInFlight(ctx, c.ID); err != nil {
		return
	}
	err := client.CancelOrder(sendCtx, p.ExchangeOrderID)
	if err == nil {
		_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
			return e.q.MarkSucceeded(ctx, tx, c.ID, mustJSON(map[string]string{"cancelled": p.ExchangeOrderID}))
		})
		return
	}
	if isDefiniteRejection(err) {
		e.failTx(ctx, c.ID, "cancel rejected: "+err.Error())
		return
	}
	e.deadReconcile(ctx, c, "cancel ambiguous outcome: "+err.Error())
}

// completePlaceSuccess records SUCCEEDED and advances the order QUEUED→SUBMITTED
// (stamping exchange_order_id) in ONE transaction (rule #9). Richer ack/fill
// mapping (ACKED/PARTIALLY_FILLED/FILLED) is PR10.
func (e *Executor) completePlaceSuccess(ctx context.Context, c queue.Claimed, ack execution.OrderAck) {
	err := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if err := e.q.MarkSucceeded(ctx, tx, c.ID, mustJSON(ack)); err != nil {
			return err
		}
		if c.OrderID == nil {
			return nil
		}
		var curState string
		var version int64
		if err := tx.QueryRowContext(ctx, "SELECT state, version FROM orders WHERE id=?", *c.OrderID).
			Scan(&curState, &version); err != nil {
			return err
		}
		if ack.ExchangeOrderID != "" {
			if _, err := tx.ExecContext(ctx, "UPDATE orders SET exchange_order_id=? WHERE id=?",
				ack.ExchangeOrderID, *c.OrderID); err != nil {
				return err
			}
		}
		_, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{
			OrderID: *c.OrderID, From: state.OrderState(curState), To: state.OrderSubmitted, Version: version,
			EventType: "place_ack", Reason: "order acknowledged by exchange",
		})
		return err
	})
	if err != nil && e.log != nil {
		// The whole tx rolled back; the request stays IN_FLIGHT and will be picked
		// up by the sweeper (conservatively) rather than wrongly marked succeeded.
		e.log.Warn("place completion tx failed (rolled back)", "id", c.ID, "err", err)
	}
}

// deadReconcile marks a mutating request DEAD and pushes its order to
// NEEDS_RECONCILE in one transaction. Used for ambiguous send outcomes.
func (e *Executor) deadReconcile(ctx context.Context, c queue.Claimed, cause string) {
	_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
		if c.OrderID != nil {
			var curState string
			var version int64
			if err := tx.QueryRowContext(ctx, "SELECT state, version FROM orders WHERE id=?", *c.OrderID).
				Scan(&curState, &version); err == nil {
				from := state.OrderState(curState)
				if state.ValidateOrderTransition(from, state.OrderNeedsReconcile) == nil {
					if _, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{
						OrderID: *c.OrderID, From: from, To: state.OrderNeedsReconcile, Version: version,
						EventType: "ambiguous_send", Reason: cause,
					}); err != nil {
						return err
					}
				}
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

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(fmt.Sprintf("{\"marshal_error\":%q}", err.Error()))
	}
	return b
}
