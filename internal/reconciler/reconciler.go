// Package reconciler makes the system safe after restart, timeout, partial fill,
// WebSocket disconnect, or DB/exchange divergence. It is READ-ONLY toward
// exchanges and NEVER auto-sends or auto-cancels (rule #1): the only writes it
// makes are local state transitions (through internal/state), lock releases (only
// when a cycle is positively safe), and decision logs.
//
// The reconciler holds a ReadOnlyClient — an interface that DELIBERATELY lacks
// PlaceOrder/CancelOrder — so re-sending a mutating request is impossible by
// construction. The safe default for anything ambiguous/missing/unverifiable is
// NEEDS_RECONCILE (rule #2): never guess.
package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/db"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/state"
	"v3TradeBot/internal/symbollock"
)

// ReadOnlyClient is the read-only subset of exchanges.PrivateClient the
// reconciler is allowed to use. Any PrivateClient satisfies it, but the
// reconciler can ONLY call these methods — it cannot place or cancel orders.
type ReadOnlyClient interface {
	Name() string
	Capabilities() exchanges.Capabilities
	GetOrder(ctx context.Context, exchangeOrderID string) (execution.OrderStatus, error)
	GetOpenOrders(ctx context.Context, symbol string) ([]execution.OrderStatus, error)
	GetBalances(ctx context.Context) ([]domain.Balance, error)
}

// Reconciler inspects DB vs exchange state and recovers safe cases.
type Reconciler struct {
	store   *db.Store
	clients map[string]ReadOnlyClient // exchange code -> read-only client
	log     *slog.Logger
}

// New builds a Reconciler.
func New(store *db.Store, clients map[string]ReadOnlyClient, log *slog.Logger) *Reconciler {
	if clients == nil {
		clients = map[string]ReadOnlyClient{}
	}
	return &Reconciler{store: store, clients: clients, log: log}
}

// Report summarises a reconciliation pass.
type Report struct {
	CyclesChecked  int
	SafeClosed     int
	NeedsReconcile int
	Continued      int
	Skipped        int // could not verify (no client for the exchange)
	StuckInFlight  int // IN_FLIGHT exchange_requests observed (reported, not resent)
	DeadRequests   int // DEAD exchange_requests observed
}

// ReconcileStartup runs the mandatory boot pass: load open cycles, reconcile each,
// and report stuck/dead requests. It is safe to run repeatedly (idempotent).
func (r *Reconciler) ReconcileStartup(ctx context.Context) (Report, error) {
	var rep Report

	cycles, err := r.loadOpenCycles(ctx)
	if err != nil {
		return rep, err
	}
	for _, c := range cycles {
		rep.CyclesChecked++
		r.reconcileCycle(ctx, c, &rep)
	}

	// Report (do NOT resend) stuck IN_FLIGHT / DEAD mutating requests. PR7's
	// SweepStuck already moves stuck mutating IN_FLIGHT to DEAD + order
	// NEEDS_RECONCILE; the reconciler observes and reports them.
	_ = r.store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_requests WHERE status='IN_FLIGHT'").Scan(&rep.StuckInFlight)
	_ = r.store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_requests WHERE status='DEAD'").Scan(&rep.DeadRequests)
	return rep, nil
}

// RunPeriodic runs ReconcileStartup on an interval until ctx is cancelled. It is
// idempotent: a NEEDS_RECONCILE cycle is left for the operator (no auto-exit), and
// the state machine's UNIQUE(version) prevents duplicate events, so repeated runs
// neither duplicate events nor oscillate state (rule #12).
func (r *Reconciler) RunPeriodic(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if rep, err := r.ReconcileStartup(ctx); err == nil && r.log != nil {
		r.log.Info("reconcile startup", "report", rep)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if _, err := r.ReconcileStartup(ctx); err != nil && r.log != nil {
				r.log.Warn("periodic reconcile failed", "err", err)
			}
		}
	}
}

// --- cycle reconciliation ---

type cycleRow struct {
	ID              int64
	State           state.CycleState
	Version         int64
	CanonicalSymbol string
	DryRun          bool // PR19: a simulated (dry-run) cycle, identified so it is never
	// confused with a real exchange order.
}

type orderRow struct {
	ID                 int64
	State              state.OrderState
	Version            int64
	ExchangeCode       string
	ExchangeOrderID    string
	LocalClientOrderID string
	Side               string
	FilledQty          decimal.Decimal
}

func (r *Reconciler) loadOpenCycles(ctx context.Context) ([]cycleRow, error) {
	rows, err := r.store.DB().QueryContext(ctx,
		"SELECT id, state, version, canonical_symbol, dry_run FROM cycles WHERE state NOT IN ('CLOSED','CANCELLED','FAILED')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cycleRow
	for rows.Next() {
		var c cycleRow
		var st string
		var dry int
		if err := rows.Scan(&c.ID, &st, &c.Version, &c.CanonicalSymbol, &dry); err != nil {
			return nil, err
		}
		c.State = state.CycleState(st)
		c.DryRun = dry != 0
		out = append(out, c)
	}
	return out, rows.Err()
}

func (r *Reconciler) loadOrders(ctx context.Context, cycleID int64) ([]orderRow, error) {
	rows, err := r.store.DB().QueryContext(ctx, `
		SELECT o.id, o.state, o.version, e.code, COALESCE(o.exchange_order_id,''),
		       o.local_client_order_id, o.side, o.filled_quantity
		FROM orders o JOIN exchanges e ON e.id = o.exchange_id
		WHERE o.cycle_id = ?`, cycleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []orderRow
	for rows.Next() {
		var o orderRow
		var st string
		if err := rows.Scan(&o.ID, &st, &o.Version, &o.ExchangeCode, &o.ExchangeOrderID,
			&o.LocalClientOrderID, &o.Side, &o.FilledQty); err != nil {
			return nil, err
		}
		o.State = state.OrderState(st)
		out = append(out, o)
	}
	return out, rows.Err()
}

// reconcileCycle reconciles one cycle's orders and then decides the cycle.
func (r *Reconciler) reconcileCycle(ctx context.Context, c cycleRow, rep *Report) {
	if c.State == state.CycleNeedsReconcile {
		// Operator-owned; leave it (idempotent no-op, no re-log).
		return
	}
	if c.DryRun {
		// Identify simulated (dry-run) cycles so they are never confused with real
		// exchange orders; they are still reconciled, but only ever against the
		// simulated client (or skipped when no client is wired).
		r.logDecision(ctx, "dry_run_cycle", "identified", c.ID, 0, "simulated dry-run cycle", nil)
	}
	orders, err := r.loadOrders(ctx, c.ID)
	if err != nil {
		r.logDecision(ctx, "reconcile_error", "no_action", c.ID, 0, err.Error(), nil)
		return
	}

	var anyNeedsReconcile, anyActive, anyFill, anySkipped bool
	for _, o := range orders {
		if state.IsTerminalOrder(o.State) {
			if o.FilledQty.IsPositive() {
				anyFill = true
			}
			// PR12 #2 (already-stored case): a terminal REJECTED or FAILED order is an
			// execution anomaly, NOT a clean zero-fill cancel — it must NEVER let the cycle
			// safe-close / release the lock (especially a sell, whose buy leg may hold
			// inventory). Force NEEDS_RECONCILE. Clean CANCELLED/EXPIRED zero-fill terminals
			// remain eligible for safe-close (gated by the active-request check in #3).
			if o.State == state.OrderRejected || o.State == state.OrderFailed {
				anyNeedsReconcile = true
			}
			continue
		}
		if o.State == state.OrderNeedsReconcile {
			anyNeedsReconcile = true
			continue
		}
		outcome := r.reconcileOrder(ctx, c, o)
		r.logDecision(ctx, "order_reconcile", string(outcome.Decision), c.ID, o.ID, outcome.Reason, nil)
		switch outcome.Decision {
		case AdvanceTerminal:
			diverted, err := r.applyOrderOutcome(ctx, o, outcome)
			if err != nil {
				// tx rolled back; treat as still-uncertain this pass.
				r.logDecision(ctx, "order_reconcile", "advance_failed", c.ID, o.ID, "advance apply failed: "+err.Error(), nil)
				anyNeedsReconcile = true
				continue
			}
			if diverted {
				// The intended terminal advance was illegal → order was diverted to
				// NEEDS_RECONCILE. Do NOT report it as a successful advance.
				anyNeedsReconcile = true
				continue
			}
			if outcome.TargetState == state.OrderFilled || o.FilledQty.IsPositive() {
				anyFill = true
			}
		case NeedsReconcile:
			if _, err := r.applyOrderOutcome(ctx, o, outcome); err != nil {
				r.logDecision(ctx, "order_reconcile", "reconcile_apply_failed", c.ID, o.ID, err.Error(), nil)
			}
			anyNeedsReconcile = true
		case Continue:
			anyActive = true
			// If we positively identified the order's exchange id (e.g. via its
			// client order id), record it even though the order stays active.
			if outcome.AttachExchangeOrderID != "" {
				_, _ = r.store.DB().ExecContext(ctx,
					"UPDATE orders SET exchange_order_id=? WHERE id=? AND (exchange_order_id IS NULL OR exchange_order_id='')",
					outcome.AttachExchangeOrderID, o.ID)
			}
		case NoAction:
			anySkipped = true // e.g. no client to verify
		}
	}

	switch {
	case anyNeedsReconcile:
		r.markCycleNeedsReconcile(ctx, c, "one or more orders need reconciliation")
		rep.NeedsReconcile++
	case anyActive:
		// Legitimately mid-flight; resume. Leave cycle + lock as-is.
		rep.Continued++
	case anySkipped:
		// Couldn't verify some orders (no client wired). Do NOT close and do NOT
		// flag — leave for the next pass / operator.
		rep.Skipped++
	case anyFill:
		// All orders terminal but inventory/fills exist → accounting is PR10;
		// the reconciler will not auto-close a cycle with exposure.
		r.markCycleNeedsReconcile(ctx, c, "all orders terminal but fills exist — accounting pending (PR10)")
		rep.NeedsReconcile++
	default:
		// Positive proof: every order terminal with ZERO fill → no exposure → safe
		// to close. A clean no-fill is CANCELLED (not FAILED); the lock is released
		// in the SAME transaction. If CANCELLED isn't legal from the current state,
		// flag NEEDS_RECONCILE — never force FAILED for a clean no-fill.
		if err := r.safeClose(ctx, c); err == nil {
			rep.SafeClosed++
		} else {
			r.markCycleNeedsReconcile(ctx, c, "zero-fill but not safe to close ("+err.Error()+"); operator review")
			rep.NeedsReconcile++
		}
	}
}

// reconcileOrder verifies one non-terminal order via read-only exchange calls and
// returns an outcome. It NEVER sends/cancels.
func (r *Reconciler) reconcileOrder(ctx context.Context, c cycleRow, o orderRow) OrderOutcome {
	client := r.clients[o.ExchangeCode]
	if client == nil {
		// No read-only client wired for this exchange: cannot verify. NoAction
		// (skip) rather than NEEDS_RECONCILE — the reconciler simply isn't
		// configured for this venue; flagging everything would be noise.
		return OrderOutcome{Decision: NoAction, Reason: "no read-only client for " + o.ExchangeCode + "; skipped"}
	}

	// Pre-send states with no exchange id: the order hasn't been sent yet (it is
	// pending in the queue) — legitimately active, not an error.
	if o.ExchangeOrderID == "" && (o.State == state.OrderNew || o.State == state.OrderRegistered || o.State == state.OrderQueued) {
		return OrderOutcome{Decision: Continue, Reason: "not yet sent (pending queue/executor)"}
	}

	caps := client.Capabilities()

	if o.ExchangeOrderID != "" {
		if !caps.FetchByOrderID {
			return OrderOutcome{Decision: NeedsReconcile, Reason: "cannot verify: GetOrder unsupported by " + o.ExchangeCode}
		}
		st, err := client.GetOrder(ctx, o.ExchangeOrderID)
		return decideKnownOrder(o.State, st, err)
	}

	// exchange_order_id unknown (only local_client_order_id). Do NOT resend.
	// Try to POSITIVELY identify the order via safe data.
	if caps.ClientOrderID && caps.FetchByOrderID && o.LocalClientOrderID != "" {
		// Client-order-id venues (e.g. Wallex) key GetOrder by the client id.
		st, err := client.GetOrder(ctx, o.LocalClientOrderID)
		if err == nil {
			out := decideKnownOrder(o.State, st, nil)
			if st.ExchangeOrderID != "" {
				out.AttachExchangeOrderID = st.ExchangeOrderID
			}
			return out
		}
		if errors.Is(err, execution.ErrOrderUnknown) {
			return OrderOutcome{Decision: NeedsReconcile, Reason: "client-order-id not found — NOT proof of no fill"}
		}
		return OrderOutcome{Decision: NeedsReconcile, Reason: "client-order-id lookup failed: " + err.Error()}
	}

	// No safe way to positively identify → conservative.
	return OrderOutcome{Decision: NeedsReconcile,
		Reason: "exchange_order_id unknown and cannot positively identify; not resending"}
}

// applyOrderOutcome applies an order transition (and optional exchange-order-id attach)
// atomically via the state machine. It NEVER silently skips an illegal transition (PR12 #4):
// if the intended target is not a legal transition from the order's current state, it DIVERTS
// the order to NEEDS_RECONCILE (returning diverted=true) rather than returning nil as if it
// succeeded — so the reconciliation report never claims an advance that did not happen. On any
// tx failure the whole tx rolls back and err is returned. `diverted` is also true when the
// intended target was already NEEDS_RECONCILE.
func (r *Reconciler) applyOrderOutcome(ctx context.Context, o orderRow, outcome OrderOutcome) (diverted bool, err error) {
	target := outcome.TargetState
	if outcome.Decision == NeedsReconcile {
		target = state.OrderNeedsReconcile
	}
	if o.State == target || state.IsTerminalOrder(o.State) {
		return target == state.OrderNeedsReconcile, nil // idempotent / nothing to do
	}
	et, reason := "reconcile", outcome.Reason
	if state.ValidateOrderTransition(o.State, target) != nil {
		// The intended transition is illegal from the current state. Do NOT silently skip —
		// divert the order to NEEDS_RECONCILE so it is never left in a stale state while the
		// report implies success. NEEDS_RECONCILE is reachable from any non-terminal state.
		target, et, reason, diverted = state.OrderNeedsReconcile, "reconcile_illegal",
			"intended "+string(outcome.TargetState)+" illegal from "+string(o.State)+" — diverted to needs_reconcile", true
	}
	txErr := r.store.WithTx(ctx, func(tx *sql.Tx) error {
		if outcome.AttachExchangeOrderID != "" {
			if _, err := tx.ExecContext(ctx, "UPDATE orders SET exchange_order_id=? WHERE id=?",
				outcome.AttachExchangeOrderID, o.ID); err != nil {
				return err
			}
		}
		_, e := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{
			OrderID: o.ID, From: o.State, To: target, Version: o.Version,
			EventType: et, Reason: reason,
		})
		return e
	})
	return diverted, txErr
}

// markCycleNeedsReconcile transitions the cycle to NEEDS_RECONCILE (if legal).
// The lock is intentionally NOT released — a cycle awaiting reconciliation must
// keep its symbol blocked.
func (r *Reconciler) markCycleNeedsReconcile(ctx context.Context, c cycleRow, reason string) {
	if c.State == state.CycleNeedsReconcile || state.IsTerminalCycle(c.State) {
		return
	}
	if state.ValidateCycleTransition(c.State, state.CycleNeedsReconcile) != nil {
		return
	}
	err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
		_, e := state.ApplyCycleTransition(ctx, tx, state.CycleTransition{
			CycleID: c.ID, From: c.State, To: state.CycleNeedsReconcile, Version: c.Version,
			EventType: "reconcile", Reason: reason,
		})
		return e
	})
	r.logDecision(ctx, "cycle_reconcile", "needs_reconcile", c.ID, 0, reason, errStr(err))
}

// errCannotSafeClose signals that CANCELLED is not a legal transition from the
// cycle's current state, so the cycle cannot be cleanly closed (the caller flags
// it NEEDS_RECONCILE instead — never FAILED for a clean no-fill).
var errCannotSafeClose = errors.New("reconciler: cannot safe-close (cancel not legal from current state)")

// errActiveRequests signals that the cycle still has an active exchange_request
// (QUEUED/CLAIMED/IN_FLIGHT/RETRY_SCHEDULED), so it is NOT safe to close / release the
// lock — the executor may still act on that request. The caller flags NEEDS_RECONCILE.
var errActiveRequests = errors.New("reconciler: cannot safe-close (active exchange_request(s) exist)")

// safeCloseReason documents the terminal-state decision for a clean no-fill
// recovery (rule from the owner): a zero-fill, zero-exposure attempt is NOT a
// failure — it is a clean abandon, so the cycle goes to CANCELLED, not FAILED.
const safeCloseReason = "SIMULATED_IOC_ZERO_FILL: all orders terminal with zero fill — no exposure (clean no-fill abandon)"

// safeClose closes a zero-exposure / zero-fill cycle to CANCELLED (a clean no-fill, NOT a
// failure) and releases its lock in ONE transaction. Returns nil on success; a non-nil error
// (the cycle then stays open and the caller flags NEEDS_RECONCILE, never FAILED) when:
//   - the cycle still has an ACTIVE exchange_request (errActiveRequests) — the executor may
//     still act on it, so closing + releasing the lock would race a live send;
//   - CANCELLED is illegal from the current state (errCannotSafeClose);
//   - any tx/DB error.
//
// Reads state/version + the active-request count fresh INSIDE the tx (atomic with the close),
// so a request enqueued concurrently cannot slip past the check.
func (r *Reconciler) safeClose(ctx context.Context, c cycleRow) error {
	err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
		var curState string
		var version int64
		if err := tx.QueryRowContext(ctx, "SELECT state, version FROM cycles WHERE id=?", c.ID).
			Scan(&curState, &version); err != nil {
			return err
		}
		from := state.CycleState(curState)
		if state.IsTerminalCycle(from) {
			return nil // already closed by someone else
		}
		// PR12 #3: never safe-close / release the lock while an exchange_request is still
		// active for this cycle (QUEUED/CLAIMED/IN_FLIGHT/RETRY_SCHEDULED) — the executor
		// could still send it. Leave the cycle for later reconciliation.
		var active int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM exchange_requests WHERE cycle_id=? AND status IN ('QUEUED','CLAIMED','IN_FLIGHT','RETRY_SCHEDULED')",
			c.ID).Scan(&active); err != nil {
			return err
		}
		if active > 0 {
			return errActiveRequests
		}
		if state.ValidateCycleTransition(from, state.CycleCancelled) != nil {
			return errCannotSafeClose
		}
		if _, err := state.ApplyCycleTransition(ctx, tx, state.CycleTransition{
			CycleID: c.ID, From: from, To: state.CycleCancelled, Version: version,
			EventType: "reconcile_no_fill", Reason: safeCloseReason,
		}); err != nil {
			return err
		}
		// Release the owning lock ONLY now that the cycle is positively terminal.
		if lock, ok, err := symbollock.ActiveByCycle(ctx, tx, c.ID); err != nil {
			return err
		} else if ok {
			if err := symbollock.Release(ctx, tx, lock.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		r.logDecision(ctx, "cycle_reconcile", "safe_close_no_fill", c.ID, 0, safeCloseReason, nil)
	} else {
		r.logDecision(ctx, "cycle_reconcile", "safe_close_refused", c.ID, 0, err.Error(), nil)
	}
	return err
}

// logDecision persists a reconciler decision to app_logs (no secrets). This is how
// "what was checked / result / decision / reason" is recorded (rule #11) without
// new schema; state changes are additionally recorded as state events.
func (r *Reconciler) logDecision(ctx context.Context, event, decision string, cycleID, orderID int64, reason string, applyErr *string) {
	fields := map[string]any{
		"event": event, "decision": decision, "cycle_id": cycleID, "reason": reason,
	}
	if orderID != 0 {
		fields["order_id"] = orderID
	}
	if applyErr != nil {
		fields["apply_error"] = *applyErr
	}
	b, _ := json.Marshal(fields)
	var cid, oid any
	if cycleID != 0 {
		cid = cycleID
	}
	if orderID != 0 {
		oid = orderID
	}
	_, _ = r.store.DB().ExecContext(ctx,
		"INSERT INTO app_logs (level, source_binary, message, fields, cycle_id, order_id) VALUES ('info','reconciler',?,?,?,?)",
		"reconcile: "+decision+" — "+reason, string(b), cid, oid)
}

func errStr(err error) *string {
	if err == nil {
		return nil
	}
	s := err.Error()
	return &s
}
