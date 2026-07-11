// Package simexec is the SIMULATED exchange client for dry-run trading mode (PR19). It
// implements exchanges.PrivateClient with NO network I/O whatsoever — it never reaches
// a real exchange, so a dry-run system exercises the full internal lifecycle (signal →
// cycle → lock → queue → executor → order processing → sell → close → reconcile) with
// deterministic, configurable simulated outcomes and zero real exposure.
//
// It is wired ONLY when the bootstrap execution mode is "dry_run"; the safe default
// ("off") wires no clients at all.
//
// Simulated order state is PERSISTED in `sim_exchange_orders` (migration 030), not in a
// process-local map. Every simulated PlaceOrder is recorded (keyed by the deterministic
// SIM-<client_order_id> AND by client_order_id) along with an explicit, mutable lifecycle
// (status + filled_quantity), so ANY executor instance — a different process, one started
// after a restart, or the reconciler — can look the order up (by exchange_order_id OR by
// client_order_id) and observe the SAME deterministic state.
//
// PR19 round 2 — ambiguous execution. Iranian venues frequently accept an order/cancel at
// the exchange while the HTTP response times out. The simulator models that faithfully:
//   - "accepted but PlaceOrder timed out" scenarios PERSIST the order first (with its real
//     state/fill) and THEN return ErrAckTimeout WITHOUT the exchange order id, so the
//     ambiguous-place recovery must find it by client_order_id;
//   - "cancel timed out" scenarios MUTATE the persisted state (canceled / partially-then-
//     canceled / filled-before-cancel / still-open) and THEN return ErrAckTimeout, so the
//     read-only recovery GetOrder returns the real post-cancel state.
//
// A re-placed identical client_order_id is IMMUTABLE + idempotent (the first accepted order
// is returned deterministically and never overwritten); a different payload on the same
// client_order_id is a hard conflict.
package simexec

import (
	"context"
	"database/sql"
	"errors"

	sqldriver "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
)

// Scenario selects the simulated outcome of an order's lifecycle.
type Scenario string

const (
	FullFill     Scenario = "full_fill"     // buy/sell fully fills
	PartialFill  Scenario = "partial_fill"  // half fills, remainder cancelled
	ZeroFill     Scenario = "zero_fill"     // nothing fills (clean no-fill)
	Ambiguous    Scenario = "ambiguous"     // GetOrder is unknown -> NEEDS_RECONCILE
	Rejected     Scenario = "rejected"      // PlaceOrder is definitively rejected
	PlaceTimeout Scenario = "place_timeout" // PlaceOrder ambiguous, order NOT accepted (alias of not-accepted)
	CancelRace   Scenario = "cancel_race"   // cancel raced a FULL fill (status=filled)

	// --- PR19 round 2: ambiguous PlaceOrder timeouts ---
	// The exchange either did or did not accept the order; the ack timed out. "not accepted"
	// persists nothing (a later lookup proves it was never placed); the "accepted" variants
	// persist the real state FIRST, then return ErrAckTimeout WITHOUT the exchange order id.
	PlaceTimeoutNotAccepted     Scenario = "place_timeout_not_accepted"
	PlaceTimeoutAcceptedOpen    Scenario = "place_timeout_accepted_open"
	PlaceTimeoutAcceptedPartial Scenario = "place_timeout_accepted_partial_fill"
	PlaceTimeoutAcceptedFull    Scenario = "place_timeout_accepted_full_fill"
	// PlaceTimeoutAcceptedFullDelayed models EVENTUAL CONSISTENCY: the order is accepted and
	// filled, but is INVISIBLE to GetOrder for the first `delayedVisibilityProbes` lookups
	// (returns ErrOrderUnknown) before it becomes queryable. Recovery must treat the early
	// "not found" as ambiguous and keep probing, not fail the cycle.
	PlaceTimeoutAcceptedFullDelayed Scenario = "place_timeout_accepted_full_delayed"

	// --- PR19 round 2: ambiguous CancelOrder timeouts ---
	// PlaceOrder succeeds normally (order rests OPEN); CancelOrder mutates the persisted state
	// then returns ErrAckTimeout, so the read-only recovery GetOrder returns the real outcome.
	CancelTimeoutButCanceled         Scenario = "cancel_timeout_but_canceled"
	CancelTimeoutStillOpen           Scenario = "cancel_timeout_still_open"
	CancelTimeoutPartialThenCanceled Scenario = "cancel_timeout_partial_then_canceled"
	CancelTimeoutFilledBeforeCancel  Scenario = "cancel_timeout_filled_before_cancel"
)

// simulated persisted lifecycle states.
const (
	statusOpen     = "OPEN"
	statusFilled   = "FILLED"
	statusCanceled = "CANCELED"
	statusUnknown  = "UNKNOWN" // Ambiguous scenario: GetOrder returns ErrOrderUnknown
)

// delayedVisibilityProbes is how many initial GetOrder lookups a delayed order stays invisible.
const delayedVisibilityProbes = 2

// Client is a simulated exchanges.PrivateClient backed by the sim_exchange_orders table, so
// its state survives restarts and is shared across executor/reconciler instances. Safe for
// concurrent use (all state is in the DB; mutations are single-statement).
type Client struct {
	db       *sql.DB
	code     string
	scenario Scenario // the scenario stamped onto orders THIS client places
}

// New builds a DB-backed simulated client for an exchange code with a default scenario.
func New(db *sql.DB, code string, scenario Scenario) *Client {
	if scenario == "" {
		scenario = FullFill
	}
	return &Client{db: db, code: code, scenario: scenario}
}

func (c *Client) Name() string { return c.code }

// Capabilities advertises client-order-id-on-place, fetch-by-order-id, AND
// lookup-by-client-order-id (GetOrder resolves either id) so the ambiguous-place recovery can
// find an order by client_order_id when the exchange id is unknown. ReliableNotFound is FALSE:
// the simulator models a venue WITH an eventual-consistency window (see hidden_probes), so a
// first "not found" is ambiguous, never proof of non-placement.
func (c *Client) Capabilities() exchanges.Capabilities {
	return exchanges.Capabilities{
		PlaceOrder: true, ClientOrderID: true, FetchByOrderID: true,
		LookupByClientOrderID: true, ReliableNotFound: false,
	}
}

// GetBalances returns a simulated, generous balance (read-only; never real).
func (c *Client) GetBalances(context.Context) ([]domain.Balance, error) {
	return []domain.Balance{{Asset: "USDT", Available: decimal.NewFromInt(1_000_000), Total: decimal.NewFromInt(1_000_000)}}, nil
}

// placePlan is the resolved effect of a PlaceOrder for a scenario.
type placePlan struct {
	status string          // persisted lifecycle status
	filled decimal.Decimal // persisted filled qty
	hidden int             // GetOrder lookups the order stays invisible (eventual consistency)
	ackErr error           // if non-nil, PlaceOrder returns this (ErrAckTimeout) instead of a normal ack
}

// planPlace resolves the persisted state + ack outcome for an ACCEPTED place. The two
// "not persisted" outcomes (Rejected, PlaceTimeout/NotAccepted) are handled before this.
func planPlace(scenario Scenario, qty decimal.Decimal) placePlan {
	half := qty.Div(decimal.NewFromInt(2))
	switch scenario {
	case PlaceTimeoutAcceptedOpen:
		return placePlan{status: statusOpen, filled: decimal.Zero, ackErr: execution.ErrAckTimeout}
	case PlaceTimeoutAcceptedPartial:
		return placePlan{status: statusOpen, filled: half, ackErr: execution.ErrAckTimeout}
	case PlaceTimeoutAcceptedFull:
		return placePlan{status: statusFilled, filled: qty, ackErr: execution.ErrAckTimeout}
	case PlaceTimeoutAcceptedFullDelayed:
		return placePlan{status: statusFilled, filled: qty, hidden: delayedVisibilityProbes, ackErr: execution.ErrAckTimeout}
	case FullFill, CancelRace:
		return placePlan{status: statusFilled, filled: qty}
	case PartialFill:
		return placePlan{status: statusCanceled, filled: half} // partial fill, remainder cancelled
	case ZeroFill:
		return placePlan{status: statusCanceled, filled: decimal.Zero}
	case Ambiguous:
		return placePlan{status: statusUnknown, filled: decimal.Zero}
	case CancelTimeoutButCanceled, CancelTimeoutStillOpen, CancelTimeoutPartialThenCanceled, CancelTimeoutFilledBeforeCancel:
		// Cancel-timeout scenarios place normally and rest OPEN; the cancel does the work.
		return placePlan{status: statusOpen, filled: decimal.Zero}
	default:
		return placePlan{status: statusFilled, filled: qty}
	}
}

// PlaceOrder simulates a submission. Rejected → a definite rejection (nothing persisted).
// PlaceTimeout/NotAccepted → an ack timeout with NOTHING persisted (a later lookup by
// client_order_id proves it was never placed). The "accepted-but-timed-out" scenarios
// persist the order FIRST (with its real state) and return ErrAckTimeout WITHOUT the
// exchange order id. All other scenarios persist and return a normal ack. Re-placing the
// same client_order_id with an identical payload is immutable + idempotent; a different
// payload is a conflict.
func (c *Client) PlaceOrder(ctx context.Context, req execution.OrderRequest) (execution.OrderAck, error) {
	clientOID := req.ClientOrderID
	if clientOID == "" {
		var err error
		if clientOID, err = c.syntheticClientID(ctx); err != nil {
			return execution.OrderAck{}, err
		}
	}

	// LOOK UP FIRST (PR19 round 4 #3): if an order already exists for this client id, the result
	// is derived ONLY from the STORED order and its STORED scenario — never this client instance's
	// configured scenario. A different-payload re-place is a conflict; an identical one replays
	// deterministically.
	if ex, err := c.loadByClientID(ctx, clientOID); err == nil {
		if !samePayload(ex, req) {
			return execution.OrderAck{}, c.conflictErr()
		}
		return c.replayFromStored(ex, req)
	} else if !errors.Is(err, execution.ErrOrderUnknown) {
		return execution.OrderAck{}, err
	}

	// No existing order → create with THIS client's scenario.
	switch c.scenario {
	case Rejected:
		return execution.OrderAck{}, &exchanges.NormalizedAPIError{Exchange: c.code, Op: "PlaceOrder", Category: exchanges.CatBadRequest, Message: "simulated rejection"}
	case PlaceTimeout, PlaceTimeoutNotAccepted:
		// Provably NOT accepted: persist nothing, so recovery's read-only lookup finds nothing.
		return execution.OrderAck{}, execution.ErrAckTimeout
	}
	exoid := "SIM-" + clientOID
	plan := planPlace(c.scenario, req.Quantity)
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO sim_exchange_orders
  (exchange_code, exchange_order_id, client_order_id, symbol, side, quantity, limit_price, order_type, time_in_force, scenario, status, filled_quantity, hidden_probes)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.code, exoid, clientOID, req.Symbol, req.Side, req.Quantity.String(), req.LimitPrice.String(),
		req.OrderType, req.TimeInForce, string(c.scenario), plan.status, plan.filled.String(), plan.hidden); err != nil {
		if isDuplicateKey(err) {
			// Concurrent insert of the same client id — fall back to the stored-order replay.
			if ex, lerr := c.loadByClientID(ctx, clientOID); lerr == nil {
				if !samePayload(ex, req) {
					return execution.OrderAck{}, c.conflictErr()
				}
				return c.replayFromStored(ex, req)
			}
		}
		return execution.OrderAck{}, err
	}
	if plan.ackErr != nil {
		// Accepted-but-timed-out: persisted, but no exchange id in the ack — recover by client id.
		return execution.OrderAck{}, plan.ackErr
	}
	return execution.OrderAck{
		ExchangeOrderID: exoid, ClientOrderID: clientOID, Symbol: req.Symbol, Side: req.Side,
		RequestedQty: req.Quantity, LimitPrice: req.LimitPrice, Status: execution.StateOpen,
	}, nil
}

// replayFromStored returns the deterministic PlaceOrder result for an EXISTING order, derived
// entirely from the order's STORED scenario (never the current client's) — so a restart / a second
// instance with a different default scenario cannot change an accepted order's behaviour.
func (c *Client) replayFromStored(ex simOrder, req execution.OrderRequest) (execution.OrderAck, error) {
	plan := planPlace(Scenario(ex.scenario), ex.qty)
	if plan.ackErr != nil {
		return execution.OrderAck{}, plan.ackErr // accepted-but-timed-out: same empty ack + timeout
	}
	return execution.OrderAck{
		ExchangeOrderID: ex.exoid, ClientOrderID: ex.clientOID, Symbol: ex.symbol, Side: ex.side,
		RequestedQty: ex.qty, LimitPrice: ex.price, Status: execution.StateOpen,
	}, nil
}

func (c *Client) conflictErr() error {
	return &exchanges.NormalizedAPIError{
		Exchange: c.code, Op: "PlaceOrder", Category: exchanges.CatBadRequest,
		Message: "duplicate client_order_id with a different payload (an accepted order is immutable)",
	}
}

// CancelOrder simulates a cancel. The behaviour is driven by the order's OWN PERSISTED
// scenario (loaded from the row), NOT the current client instance's configured scenario — so a
// different process / a restarted instance with a different default behaves deterministically
// per the order that was actually placed. The four cancel-timeout scenarios MUTATE the persisted
// state and return ErrAckTimeout (the real outcome is then observed via GetOrder). Every other
// scenario performs a normal successful cancel: an OPEN order becomes CANCELED (its partial fill
// preserved); a terminal order is a no-op.
func (c *Client) CancelOrder(ctx context.Context, exchangeOrderID string) error {
	ord, err := c.load(ctx, exchangeOrderID)
	if errors.Is(err, execution.ErrOrderUnknown) {
		// Nothing to cancel: an idempotent no-op (success) — the executor's final GET_ORDER is
		// authoritative for whether it filled.
		return nil
	}
	if err != nil {
		return err
	}
	persisted := Scenario(ord.scenario) // the order's OWN scenario, not c.scenario
	half := ord.qty.Div(decimal.NewFromInt(2))
	switch persisted {
	case CancelTimeoutButCanceled:
		if err := c.setStatus(ctx, ord.exoid, statusCanceled); err != nil {
			return err
		}
		return execution.ErrAckTimeout
	case CancelTimeoutStillOpen:
		// Cancel did NOT take: state unchanged, but the ack timed out.
		return execution.ErrAckTimeout
	case CancelTimeoutPartialThenCanceled:
		if err := c.setState(ctx, ord.exoid, statusCanceled, half); err != nil {
			return err
		}
		return execution.ErrAckTimeout
	case CancelTimeoutFilledBeforeCancel:
		if err := c.setState(ctx, ord.exoid, statusFilled, ord.qty); err != nil {
			return err
		}
		return execution.ErrAckTimeout
	default:
		// Normal cancel: cancel an OPEN order (preserve any partial fill); no-op if terminal.
		_, err := c.db.ExecContext(ctx,
			"UPDATE sim_exchange_orders SET status=? WHERE exchange_code=? AND exchange_order_id=? AND status=?",
			statusCanceled, c.code, ord.exoid, statusOpen)
		return err
	}
}

// GetOrder returns the simulated status. It looks the order up by exchange_order_id OR
// client_order_id (so the ambiguous-place recovery can find an order whose exchange id was
// never returned), then maps the persisted status + filled_quantity to a normalized status.
// An unknown/not-persisted id → ErrOrderUnknown; the Ambiguous scenario (status=UNKNOWN)
// deliberately returns ErrOrderUnknown even when the order IS persisted.
func (c *Client) GetOrder(ctx context.Context, orderID string) (execution.OrderStatus, error) {
	ord, err := c.load(ctx, orderID)
	if err != nil {
		return execution.OrderStatus{}, err
	}
	// Eventual consistency: while the order is within its hidden window it is NOT yet queryable
	// (the venue accepted it but it isn't visible). Decrement and report "unknown" — the recovery
	// must treat this early miss as ambiguous, not proof of non-placement.
	if ord.hiddenProbes > 0 {
		_, _ = c.db.ExecContext(ctx,
			"UPDATE sim_exchange_orders SET hidden_probes = hidden_probes - 1 WHERE exchange_code=? AND exchange_order_id=? AND hidden_probes > 0",
			c.code, ord.exoid)
		return execution.OrderStatus{}, execution.ErrOrderUnknown
	}
	if ord.status == statusUnknown {
		return execution.OrderStatus{}, execution.ErrOrderUnknown
	}
	st := execution.OrderStatus{
		ExchangeOrderID: ord.exoid, ClientOrderID: ord.clientOID, Symbol: ord.symbol, Side: ord.side,
		IntendedQty: ord.qty, AvgPrice: ord.price, Liquidity: "maker",
	}
	filled := ord.filled
	switch ord.status {
	case statusFilled:
		st.Status = execution.StateFilled
		st.FilledQty = ord.qty
		st.RemainingQty = decimal.Zero
		st.ExecutedQuote = ord.qty.Mul(ord.price)
	case statusCanceled:
		if filled.IsPositive() {
			st.Status = execution.StatePartiallyCanceled
			st.FilledQty = filled
			st.RemainingQty = ord.qty.Sub(filled)
			st.ExecutedQuote = filled.Mul(ord.price)
		} else {
			st.Status = execution.StateCanceled
			st.FilledQty = decimal.Zero
			st.RemainingQty = ord.qty
		}
	default: // statusOpen
		if filled.IsPositive() {
			st.Status = execution.StatePartiallyFilled
			st.FilledQty = filled
			st.RemainingQty = ord.qty.Sub(filled)
			st.ExecutedQuote = filled.Mul(ord.price)
		} else {
			st.Status = execution.StateOpen
			st.FilledQty = decimal.Zero
			st.RemainingQty = ord.qty
		}
	}
	return st, nil
}

// GetOrderByClientOrderID implements exchanges.ClientOrderLookup — the simulator resolves an order
// by its client order id (GetOrder already accepts either identifier).
func (c *Client) GetOrderByClientOrderID(ctx context.Context, clientOrderID string) (execution.OrderStatus, error) {
	return c.GetOrder(ctx, clientOrderID)
}

// GetOpenOrders returns nothing (simulation resolves via GetOrder).
func (c *Client) GetOpenOrders(context.Context, string) ([]execution.OrderStatus, error) {
	return nil, nil
}

// SubscribeOrderUpdates is unsupported (the dry-run flow is poll-based like the venues).
func (c *Client) SubscribeOrderUpdates(context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	return nil, exchanges.Unsupported(c.code, "SubscribeOrderUpdates")
}

// ---- persistence helpers ----

type simOrder struct {
	exoid        string
	clientOID    string
	symbol       string
	side         string
	qty          decimal.Decimal
	price        decimal.Decimal
	orderType    string
	tif          string
	scenario     string
	status       string
	filled       decimal.Decimal
	hiddenProbes int
}

// load reads a simulated order by exchange_order_id OR client_order_id.
func (c *Client) load(ctx context.Context, id string) (simOrder, error) {
	var (
		o                  simOrder
		qtyS, priceS, filS string
	)
	err := c.db.QueryRowContext(ctx, `
SELECT exchange_order_id, client_order_id, symbol, side, quantity, limit_price, order_type, time_in_force, scenario, status, filled_quantity, hidden_probes
FROM sim_exchange_orders
WHERE exchange_code=? AND (exchange_order_id=? OR client_order_id=?) LIMIT 1`,
		c.code, id, id).Scan(&o.exoid, &o.clientOID, &o.symbol, &o.side, &qtyS, &priceS, &o.orderType, &o.tif, &o.scenario, &o.status, &filS, &o.hiddenProbes)
	if errors.Is(err, sql.ErrNoRows) {
		return simOrder{}, execution.ErrOrderUnknown
	}
	if err != nil {
		return simOrder{}, err
	}
	o.qty = decimal.RequireFromString(qtyS)
	o.price = decimal.RequireFromString(priceS)
	o.filled = decimal.RequireFromString(filS)
	return o, nil
}

func (c *Client) loadByClientID(ctx context.Context, clientOID string) (simOrder, error) {
	return c.load(ctx, clientOID)
}

func (c *Client) setStatus(ctx context.Context, exoid, status string) error {
	_, err := c.db.ExecContext(ctx,
		"UPDATE sim_exchange_orders SET status=? WHERE exchange_code=? AND exchange_order_id=?",
		status, c.code, exoid)
	return err
}

func (c *Client) setState(ctx context.Context, exoid, status string, filled decimal.Decimal) error {
	_, err := c.db.ExecContext(ctx,
		"UPDATE sim_exchange_orders SET status=?, filled_quantity=? WHERE exchange_code=? AND exchange_order_id=?",
		status, filled.String(), c.code, exoid)
	return err
}

// syntheticClientID derives a per-exchange client id for the unit-test edge where the caller
// supplied none (the real flow always supplies the order's local_client_order_id).
func (c *Client) syntheticClientID(ctx context.Context) (string, error) {
	var n int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sim_exchange_orders WHERE exchange_code=?", c.code).Scan(&n); err != nil {
		return "", err
	}
	return "SIMC-" + c.code + "-" + decimal.NewFromInt(int64(n+1)).String(), nil
}

// samePayload compares ALL exchange-visible immutable fields of an existing order against a
// re-place request (symbol, side, quantity, price, order type, time-in-force) so an idempotent
// re-place is only accepted when the payload is truly identical; any difference is a conflict.
func samePayload(ex simOrder, req execution.OrderRequest) bool {
	return ex.symbol == req.Symbol && ex.side == req.Side &&
		ex.qty.Equal(req.Quantity) && ex.price.Equal(req.LimitPrice) &&
		ex.orderType == req.OrderType && ex.tif == req.TimeInForce
}

// isDuplicateKey reports whether err is a MariaDB duplicate-key (1062) violation.
func isDuplicateKey(err error) bool {
	var me *sqldriver.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// Compile-time assertion: a simulated client satisfies the full private interface.
var _ exchanges.PrivateClient = (*Client)(nil)
