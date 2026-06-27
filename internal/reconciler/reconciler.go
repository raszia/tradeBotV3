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
		"SELECT id, state, version, canonical_symbol FROM cycles WHERE state NOT IN ('CLOSED','CANCELLED','FAILED')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cycleRow
	for rows.Next() {
		var c cycleRow
		var st string
		if err := rows.Scan(&c.ID, &st, &c.Version, &c.CanonicalSymbol); err != nil {
			return nil, err
		}
		c.State = state.CycleState(st)
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
			if err := r.applyOrderOutcome(ctx, o, outcome); err != nil {
				// tx rolled back; treat as still-uncertain this pass.
				anyNeedsReconcile = true
				continue
			}
			if outcome.TargetState == state.OrderFilled || o.FilledQty.IsPositive() {
				anyFill = true
			}
		case NeedsReconcile:
			_ = r.applyOrderOutcome(ctx, o, outcome)
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
		// to close. Closes to FAILED (the attempt produced no inventory) and
		// releases the symbol lock in the SAME transaction.
		if r.safeClose(ctx, c) {
			rep.SafeClosed++
		} else {
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

// applyOrderOutcome applies an order transition (and optional exchange-order-id
// attach) atomically via the state machine. On any failure the whole tx rolls
// back (rule #10/#13).
func (r *Reconciler) applyOrderOutcome(ctx context.Context, o orderRow, outcome OrderOutcome) error {
	target := outcome.TargetState
	if outcome.Decision == NeedsReconcile {
		target = state.OrderNeedsReconcile
	}
	if o.State == target {
		return nil // idempotent
	}
	if state.ValidateOrderTransition(o.State, target) != nil {
		return nil // can't legally transition (e.g. already terminal) — skip
	}
	return r.store.WithTx(ctx, func(tx *sql.Tx) error {
		if outcome.AttachExchangeOrderID != "" {
			if _, err := tx.ExecContext(ctx, "UPDATE orders SET exchange_order_id=? WHERE id=?",
				outcome.AttachExchangeOrderID, o.ID); err != nil {
				return err
			}
		}
		_, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{
			OrderID: o.ID, From: o.State, To: target, Version: o.Version,
			EventType: "reconcile", Reason: outcome.Reason,
		})
		return err
	})
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

// safeClose closes a zero-exposure cycle to FAILED and releases its lock in ONE
// transaction. Returns false if the close could not be applied (then the caller
// counts it as needs-reconcile). Reads the cycle state/version fresh inside the tx
// for a correct CAS.
func (r *Reconciler) safeClose(ctx context.Context, c cycleRow) bool {
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
		if err := state.ValidateCycleTransition(from, state.CycleFailed); err != nil {
			return err
		}
		if _, err := state.ApplyCycleTransition(ctx, tx, state.CycleTransition{
			CycleID: c.ID, From: from, To: state.CycleFailed, Version: version,
			EventType: "reconcile_safe_close", Reason: "all orders terminal with zero fill — no exposure",
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
	r.logDecision(ctx, "cycle_reconcile", "safe_close", c.ID, 0, "zero-exposure terminal cycle closed; lock released", errStr(err))
	return err == nil
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
