// Package live is the LIMITED-LIVE safety layer (PR20). Before any real mutating order
// is sent, a Guard enforces hard caps + a global kill switch + per-exchange/per-symbol
// live flags + credential availability + valid state, and audits every decision. It is
// the FINAL gate inside the order-executor (not only the engine), so a real PlaceOrder/
// CancelOrder can never bypass the limits. Safe by default: an unconfigured guard, an
// engaged kill switch, a missing cap, a disabled exchange/symbol, or absent credentials
// all DENY.
package live

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/preflight"
	"v3TradeBot/internal/state"
)

// Controls is the global live-control row (caps + kill switch). A nil/zero cap means
// "not configured" — which makes Configured() false and blocks all live trading.
//
// OWNER DECISION (PR20 correction): there are NO daily trading limits. The historical
// max_daily_orders / max_daily_quote columns remain in the live_controls table (dropping
// them would be a needless destructive migration) but they are NOT read, NOT required by
// Configured(), and NEVER influence a trading decision.
type Controls struct {
	KillSwitch             bool
	MaxOpenCycles          int
	MaxOrderNotional       decimal.Decimal
	MaxBaseQty             decimal.Decimal
	MaxConsecutiveFailures int
	MaxUnresolvedReconcile int
	ConfigVersion          int64
	// PR23 canary acknowledgement gate.
	RequireCanaryAck   bool
	CanaryExchangeID   int64
	CanaryMarketID     int64
	CanaryAckMaxAgeMin int
	loaded             bool
}

// Configured reports whether every required cap is set (a missing cap blocks live).
// Daily order-count/quote caps are intentionally NOT part of this (owner decision).
func (c Controls) Configured() bool {
	return c.loaded &&
		c.MaxOpenCycles > 0 &&
		c.MaxOrderNotional.IsPositive() && c.MaxBaseQty.IsPositive() &&
		c.MaxConsecutiveFailures > 0 && c.MaxUnresolvedReconcile >= 0
}

// Decision is the guard verdict.
type Decision struct {
	Allow  bool
	Reason string
}

func deny(reason string) Decision  { return Decision{Allow: false, Reason: reason} }
func allow(reason string) Decision { return Decision{Allow: true, Reason: reason} }

// Guard enforces the live limits and audits decisions. It holds only a DB handle.
type Guard struct {
	db  *sql.DB
	clk clock.Clock
	log *slog.Logger
}

// NewGuard builds a Guard.
func NewGuard(db *sql.DB, clk clock.Clock, log *slog.Logger) *Guard {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Guard{db: db, clk: clk, log: log}
}

// LoadControls reads the singleton live_controls row. The historical max_daily_orders /
// max_daily_quote columns are deliberately NOT selected (owner decision: no daily limits).
func (g *Guard) LoadControls(ctx context.Context) (Controls, error) {
	var c Controls
	var (
		kill, reqAck                              int
		maxOpen, maxConsec, maxUnres              sql.NullInt64
		maxNotional, maxBase                      sql.NullString
		cfgVersion, canaryEx, canaryMk, ackMaxAge sql.NullInt64
	)
	err := g.db.QueryRowContext(ctx,
		"SELECT kill_switch, max_open_cycles, max_order_notional, max_base_qty, max_consecutive_failures, max_unresolved_reconcile, COALESCE(config_version,0), require_canary_ack, canary_exchange_id, canary_market_id, canary_ack_max_age_minutes FROM live_controls WHERE id=1").
		Scan(&kill, &maxOpen, &maxNotional, &maxBase, &maxConsec, &maxUnres, &cfgVersion, &reqAck, &canaryEx, &canaryMk, &ackMaxAge)
	if errors.Is(err, sql.ErrNoRows) {
		return Controls{KillSwitch: true}, nil // no row => safe: kill switch engaged, unconfigured
	}
	if err != nil {
		return Controls{}, err
	}
	c.loaded = true
	c.KillSwitch = kill != 0
	c.MaxOpenCycles = int(maxOpen.Int64)
	c.MaxConsecutiveFailures = int(maxConsec.Int64)
	c.MaxUnresolvedReconcile = int(maxUnres.Int64)
	c.MaxOrderNotional = decOrZero(maxNotional)
	c.MaxBaseQty = decOrZero(maxBase)
	c.ConfigVersion = cfgVersion.Int64
	c.RequireCanaryAck = reqAck != 0
	c.CanaryExchangeID = canaryEx.Int64
	c.CanaryMarketID = canaryMk.Int64
	c.CanaryAckMaxAgeMin = int(ackMaxAge.Int64)
	return c, nil
}

// PlaceCheck is the input to CheckPlace.
type PlaceCheck struct {
	ExchangeID       int64
	ExchangeMarketID int64
	ExchangeCode     string
	Symbol           string
	CycleID          int64
	OrderID          int64
	RequestID        int64
	Side             string // "buy" | "sell"
	Notional         decimal.Decimal
	BaseQty          decimal.Decimal
	DryRun           bool
	// ExchangeOrderID is the venue order id the CANCEL payload targets. The guard proves it
	// against the registered order before any real cancel (PR20 correction #3) — a payload
	// value is never trusted on its own.
	ExchangeOrderID string
	// The remaining fields are the EXACT values the executor is about to send to the venue,
	// taken from the queued mutation payload. The guard proves each against the persisted
	// order before the send (PR20 correction #5): the payload and the authoritative DB row
	// are two internal values that must agree exactly, or we do not know what we are placing.
	LimitPrice         decimal.Decimal
	OrderType          string
	TimeInForce        string
	LocalClientOrderID string
	// ClientOrderIDSent is the EXACT (adapter-normalized) client id the executor is about to
	// send. The FINAL guard proves it against the persisted `client_order_id_sent` column, so
	// the value sent to the venue equals the value proven and stored — no transformation
	// happens after the guard (PR20 correction #6). The early pre-pacing guard leaves it empty
	// (nothing is persisted yet); the final guard sets it.
	ClientOrderIDSent string
}

// CheckPlace is the final gate before a real PLACE_ORDER. It DENIES unless every
// condition holds. Entry (BUY) and exit (SELL) are separated (PR20 correction):
//
//   - A BUY increases exposure: it runs the full entry gauntlet — controls configured,
//     live flags, credentials, per-order caps, unresolved-reconcile / consecutive-failure
//     health, kill switch, open-cycle cap, canary acknowledgement.
//   - A SELL that provably EXITS already-acquired inventory (verified from DB state:
//     ownership, filled buy inventory, no oversell, no duplicate active sell) reduces
//     exposure and must stay possible even when the kill switch is engaged, entries are
//     disabled, or entry limits are reached. It still requires credentials, correct
//     dry-run/live routing, and the DB-proven risk-reduction checks.
//
// DURABLE AUDIT (PR20 correction): an ALLOW for a real place is committed to live_audit
// BEFORE the caller may send; if the audit cannot be persisted the decision flips to DENY
// (fail closed) — a real order is never sent without its durable allow record.
// CheckPlaceNoAudit runs the place decision WITHOUT writing any audit row (pure check).
func (g *Guard) CheckPlaceNoAudit(ctx context.Context, p PlaceCheck) Decision {
	return g.checkPlace(ctx, p)
}

// CheckPlaceEarly is the EARLY, pre-pacing pre-filter (PR20 correction #2): it rejects a
// locally-invalid request before it consumes a pacing slot. It audits ONLY denials — a denied
// live attempt is a real decision worth recording — while the authoritative ALLOW audit is
// written by CheckPlace at the FINAL guard, immediately before the send, so the allow reflects
// the state at send time (and is never written prematurely for a request that then waits in
// the pacer). Every terminal decision is thus audited exactly once.
func (g *Guard) CheckPlaceEarly(ctx context.Context, p PlaceCheck) Decision {
	d := g.checkPlace(ctx, p)
	if !d.Allow {
		if aerr := g.audit(ctx, auditRow{ExchangeID: p.ExchangeID, ExchangeMarketID: p.ExchangeMarketID, CycleID: p.CycleID,
			OrderID: p.OrderID, RequestID: p.RequestID, Action: "place_" + p.Side, Side: p.Side, Notional: p.Notional, Decision: d}); aerr != nil {
			g.log.Error("live: audit write failed for an early place denial", "reason", d.Reason, "err", aerr)
		}
	}
	return d
}

func (g *Guard) CheckPlace(ctx context.Context, p PlaceCheck) Decision {
	d := g.checkPlace(ctx, p)
	if aerr := g.audit(ctx, auditRow{ExchangeID: p.ExchangeID, ExchangeMarketID: p.ExchangeMarketID, CycleID: p.CycleID,
		OrderID: p.OrderID, RequestID: p.RequestID, Action: "place_" + p.Side, Side: p.Side, Notional: p.Notional, Decision: d}); aerr != nil {
		g.log.Error("live: audit write failed for a place decision", "decision", d.Allow, "reason", d.Reason, "err", aerr)
		if d.Allow {
			// FAIL CLOSED: without a durable allow-audit the real order must not be sent.
			d = deny("audit persistence failed — real order not sent")
			// Best-effort record of the flipped denial (the DB is already unhealthy; log regardless).
			_ = g.audit(ctx, auditRow{ExchangeID: p.ExchangeID, ExchangeMarketID: p.ExchangeMarketID, CycleID: p.CycleID,
				OrderID: p.OrderID, RequestID: p.RequestID, Action: "place_" + p.Side, Side: p.Side, Notional: p.Notional, Decision: d})
		}
	}
	return d
}

func (g *Guard) checkPlace(ctx context.Context, p PlaceCheck) Decision {
	if p.DryRun {
		return deny("dry-run request must never be sent live")
	}
	credOK, err := g.credentialsAvailable(ctx, p.ExchangeID)
	if err != nil {
		return deny("cannot verify credentials (db error) — fail closed")
	}
	if !credOK {
		return deny("no active credentials for exchange")
	}
	// EXIT SELL: exposure-reducing — proven from DB state, not from the payload's word.
	if p.Side == "sell" {
		return g.checkExitSell(ctx, p)
	}
	// ENTRY BUY: prove the order IS a real, live, correctly-owned entry buy before any
	// cap/flag reasoning (PR20 correction #2). A DB error or a missing/mismatched
	// relationship denies — an unidentifiable market is never a safe market.
	if d := g.checkEntryBuyIdentity(ctx, p); !d.Allow {
		return d
	}
	// Full gauntlet. Every safety query failure DENIES (fail closed).
	ctrl, err := g.LoadControls(ctx)
	if err != nil {
		return deny("failed to load live controls")
	}
	if !ctrl.Configured() {
		return deny("live not configured (a required cap is missing)")
	}
	liveOK, err := g.live(ctx, p.ExchangeID, p.ExchangeMarketID)
	if err != nil {
		return deny("cannot verify live flags (db error) — fail closed")
	}
	if !liveOK {
		return deny("exchange or symbol not live-enabled")
	}
	if p.Notional.GreaterThan(ctrl.MaxOrderNotional) {
		return deny("order notional exceeds max_order_notional")
	}
	if p.BaseQty.GreaterThan(ctrl.MaxBaseQty) {
		return deny("order base quantity exceeds max_base_qty")
	}
	unres, err := g.unresolvedReconcile(ctx)
	if err != nil {
		return deny("cannot count unresolved reconciles (db error) — fail closed")
	}
	if unres > ctrl.MaxUnresolvedReconcile {
		return deny("too many unresolved NEEDS_RECONCILE")
	}
	fails, err := g.consecutiveFailures(ctx, p.ExchangeID)
	if err != nil {
		return deny("cannot count consecutive failures (db error) — fail closed")
	}
	if fails >= ctrl.MaxConsecutiveFailures {
		return deny("too many consecutive failures")
	}
	if ctrl.KillSwitch {
		return deny("kill switch engaged: no new buy orders")
	}
	open, err := g.openCycles(ctx)
	if err != nil {
		return deny("cannot count open cycles (db error) — fail closed")
	}
	if open > ctrl.MaxOpenCycles {
		return deny("max open cycles reached")
	}
	if d := g.canaryAckOK(ctx, ctrl, p.ExchangeID, p.ExchangeMarketID); !d.Allow {
		return d
	}
	return allow("ok")
}

// cancellableOrderStates are the order states from which a real CANCEL may be sent: the
// order is (or may still be) live on the venue. Anything else — QUEUED (never sent, so there
// is nothing at the venue to cancel) or a terminal state — must not produce a cancel call.
var cancellableOrderStates = map[string]bool{
	string(state.OrderSubmitted):       true,
	string(state.OrderAcked):           true,
	string(state.OrderPartiallyFilled): true,
	string(state.OrderCancelPending):   true,
	string(state.OrderNeedsReconcile):  true,
}

// checkCancelTarget PROVES the cancel targets the exact registered order (PR20 correction
// #3). The queue payload supplies an exchange_order_id; on its own that is just an attacker-
// or bug-supplied string, so before any real CancelOrder we verify from the database that:
// the order belongs to this request's exchange AND cycle, the order's stored
// exchange_order_id is present and EQUAL to the payload's, and the order is in a state a
// cancel can legally act on. Any missing/unreadable/mismatched field denies — no cancel call.
func (g *Guard) checkCancelTarget(ctx context.Context, p PlaceCheck) Decision {
	if p.OrderID == 0 || p.CycleID == 0 {
		return deny("cancel check: request carries no order/cycle context")
	}
	var (
		oExchangeID, oCycleID int64
		ostate                string
		oExchangeOrderID      sql.NullString
	)
	err := g.db.QueryRowContext(ctx,
		"SELECT exchange_id, cycle_id, state, exchange_order_id FROM orders WHERE id=?", p.OrderID).
		Scan(&oExchangeID, &oCycleID, &ostate, &oExchangeOrderID)
	if err != nil {
		return deny("cancel check: cannot verify the order (db) — fail closed")
	}
	if oExchangeID != p.ExchangeID || oCycleID != p.CycleID {
		return deny("cancel check: order does not belong to this request's exchange/cycle")
	}
	if !oExchangeOrderID.Valid || oExchangeOrderID.String == "" {
		return deny("cancel check: order has no registered exchange_order_id")
	}
	if p.ExchangeOrderID == "" || p.ExchangeOrderID != oExchangeOrderID.String {
		return deny("cancel check: payload exchange_order_id does not match the registered order")
	}
	if !cancellableOrderStates[ostate] {
		return deny("cancel check: order state is not cancellable (" + ostate + ")")
	}
	return allow("cancel reduces risk (target proven)")
}

// checkEntryBuyIdentity is the AUTHORITATIVE pre-send proof for a live entry buy
// (PR20 correction #2). One query joins the order to its cycle, its exchange_market and that
// market's exchange, so a missing cycle/market/exchange relationship yields NO ROW and
// therefore a denial — never a silent zero. It proves, together and from the database (never
// from the payload): the order is an `entry_buy`, it is still `QUEUED` (a legal send state),
// its cycle is real (`dry_run=0`), the order belongs to the REQUEST's cycle and exchange, the
// market belongs to that SAME exchange, the resolved market matches the order's market, the
// request symbol matches the registered market's canonical symbol, and both the exchange and
// the market are live-enabled. ANY query error, missing row, NULL, or mismatch denies — so no
// PlaceOrder call happens.
func (g *Guard) checkEntryBuyIdentity(ctx context.Context, p PlaceCheck) Decision {
	o, err := g.loadSendOrder(ctx, p.OrderID)
	if err != nil {
		// Includes sql.ErrNoRows: an order/cycle/market/exchange relationship we cannot
		// resolve is NOT a safe order.
		return deny("entry check: cannot identify order/cycle/market (db) — fail closed")
	}
	if o.role != "entry_buy" {
		return deny("entry check: order is not an entry buy")
	}
	if o.state != string(state.OrderQueued) {
		return deny("entry check: order not in a sendable state (" + o.state + ")")
	}
	if o.dryRun != 0 {
		return deny("entry check: cycle is dry-run — must never be sent live")
	}
	if o.cycleID != p.CycleID || o.exchangeID != p.ExchangeID {
		return deny("entry check: order does not belong to this request's cycle/exchange")
	}
	if o.marketExchangeID != o.exchangeID {
		return deny("entry check: market belongs to a different exchange than the order")
	}
	if p.ExchangeMarketID != 0 && o.marketID != p.ExchangeMarketID {
		return deny("entry check: resolved market does not match the order's market")
	}
	if p.Symbol == "" || o.symbol != p.Symbol {
		return deny("entry check: request symbol does not match the registered market")
	}
	if o.exLive != 1 || o.marketLive != 1 {
		return deny("entry check: exchange or symbol not live-enabled")
	}
	if d := matchPayloadToOrder("entry", o, p); !d.Allow {
		return d
	}
	return allow("entry buy identity proven")
}

// sendOrder is the authoritative persisted view of an order about to be sent, joined to its
// cycle, its market and that market's exchange.
type sendOrder struct {
	role, state, symbol string
	exchangeID          int64
	cycleID             int64
	marketID            int64
	dryRun              int
	marketExchangeID    int64
	marketLive, exLive  int
	quantity            decimal.Decimal
	limitPrice          decimal.NullDecimal
	orderType           string
	timeInForce         sql.NullString
	localClientOrderID  string
	clientOrderIDSent   sql.NullString
}

// loadSendOrder runs the ONE authoritative query behind every pre-send proof. The JOINs mean
// a missing cycle/market/exchange relationship yields sql.ErrNoRows — a denial, never a
// silently-zero field.
func (g *Guard) loadSendOrder(ctx context.Context, orderID int64) (sendOrder, error) {
	var o sendOrder
	var qtyS string
	err := g.db.QueryRowContext(ctx, `
		SELECT o.role, o.state, o.exchange_id, o.cycle_id, o.exchange_market_id,
		       o.quantity, o.limit_price, o.order_type, o.time_in_force, o.local_client_order_id,
		       o.client_order_id_sent,
		       c.dry_run, em.exchange_id, em.canonical_symbol, em.live_enabled, ex.live_enabled
		FROM orders o
		JOIN cycles c            ON c.id  = o.cycle_id
		JOIN exchange_markets em ON em.id = o.exchange_market_id
		JOIN exchanges ex        ON ex.id = em.exchange_id
		WHERE o.id = ?`, orderID).
		Scan(&o.role, &o.state, &o.exchangeID, &o.cycleID, &o.marketID,
			&qtyS, &o.limitPrice, &o.orderType, &o.timeInForce, &o.localClientOrderID,
			&o.clientOrderIDSent,
			&o.dryRun, &o.marketExchangeID, &o.symbol, &o.marketLive, &o.exLive)
	if err != nil {
		return sendOrder{}, err
	}
	q, qerr := decimal.NewFromString(qtyS)
	if qerr != nil {
		return sendOrder{}, qerr
	}
	o.quantity = q
	return o, nil
}

// matchPayloadToOrder proves the mutation payload the executor is about to SEND equals the
// authoritative persisted order (PR20 correction #5). These are two INTERNAL values — the row
// we registered and committed before sending, and the queued payload being sent — so any
// disagreement means we do not know what we are actually placing (a stale, tampered, or
// mis-routed payload). It is not venue-response matching: no rounding/normalization tolerance
// applies, the values must be equal.
//
// decimal.Equal compares VALUES, so "0.50" == "0.5" (a DECIMAL(36,18) round-trip pads zeros);
// it is exact for the money quantities involved, never a float epsilon.
func matchPayloadToOrder(kind string, o sendOrder, p PlaceCheck) Decision {
	if !p.BaseQty.IsPositive() || !p.BaseQty.Equal(o.quantity) {
		return deny(kind + " check: payload quantity does not match the registered order")
	}
	if !o.limitPrice.Valid {
		return deny(kind + " check: registered order has no limit price")
	}
	if !p.LimitPrice.IsPositive() || !p.LimitPrice.Equal(o.limitPrice.Decimal) {
		return deny(kind + " check: payload price does not match the registered order")
	}
	if p.OrderType == "" || !strings.EqualFold(p.OrderType, o.orderType) {
		return deny(kind + " check: payload order type does not match the registered order")
	}
	// time_in_force NULL semantics are EXACT (PR20 correction #6): if the DB records no TIF
	// (NULL or empty — the strategy chose the venue default), the payload must ALSO carry no
	// TIF; a non-empty payload TIF (e.g. "FOK") is a mismatch. If the DB records one, the
	// payload must equal it exactly.
	dbTIF := ""
	if o.timeInForce.Valid {
		dbTIF = o.timeInForce.String
	}
	if dbTIF == "" {
		if p.TimeInForce != "" {
			return deny(kind + " check: registered order has no time-in-force but the payload sets one")
		}
	} else if !strings.EqualFold(p.TimeInForce, dbTIF) {
		return deny(kind + " check: payload time-in-force does not match the registered order")
	}
	if p.LocalClientOrderID == "" || p.LocalClientOrderID != o.localClientOrderID {
		return deny(kind + " check: payload client order id does not match the registered order")
	}
	// The EXACT value being sent (ClientOrderIDSent) is proven against the persisted
	// client_order_id_sent column (PR20 correction #6). The executor normalizes the local id,
	// verifies it non-empty, and persists it BEFORE this (final) guard, then sends exactly it.
	// When provided (the final guard), it must be non-empty and equal the persisted value; the
	// early pre-pacing guard leaves it empty and this check is deferred to the final one.
	if p.ClientOrderIDSent != "" {
		if !o.clientOrderIDSent.Valid || o.clientOrderIDSent.String == "" {
			return deny(kind + " check: no persisted sent client order id to prove against")
		}
		if o.clientOrderIDSent.String != p.ClientOrderIDSent {
			return deny(kind + " check: sent client order id does not match the persisted value")
		}
	}
	if p.Side == "" || !strings.EqualFold(p.Side, expectedSide(o.role)) {
		return deny(kind + " check: payload side does not match the registered order's role")
	}
	return allow("payload matches the registered order")
}

// expectedSide maps an order role to the only side it may ever be sent with.
func expectedSide(role string) string {
	if role == "exit_sell" {
		return "sell"
	}
	return "buy"
}

// exitMarketMatchesInventory proves the sell is routed to the exact market where this cycle's
// inventory was ACQUIRED (PR20 correction #5). Comparing to the cycle's entry buys (rather
// than only to the cycle row) ties the sell to the asset we actually hold.
func (g *Guard) exitMarketMatchesInventory(ctx context.Context, cycleID int64, o sendOrder) Decision {
	var buyMarketID, buyExchangeID int64
	var buySymbol string
	err := g.db.QueryRowContext(ctx, `
		SELECT o.exchange_market_id, o.exchange_id, em.canonical_symbol
		FROM orders o
		JOIN exchange_markets em ON em.id = o.exchange_market_id
		WHERE o.cycle_id = ? AND o.role = 'entry_buy' AND o.filled_quantity > 0
		ORDER BY o.id LIMIT 1`, cycleID).Scan(&buyMarketID, &buyExchangeID, &buySymbol)
	if err != nil {
		return deny("exit check: cannot identify the acquired market (db) — fail closed")
	}
	if buyMarketID != o.marketID || buyExchangeID != o.exchangeID || buySymbol != o.symbol {
		return deny("exit check: sell market/symbol is not where the inventory was acquired")
	}
	return allow("exit market matches the acquired inventory")
}

// checkExitSell PROVES from database state that a sell closes existing exposure before
// allowing it past the entry controls (PR20 correction: "do not blindly classify every
// sell as risk-reducing"). It verifies, failing CLOSED on every DB error:
//
//	ownership       — the order row is role='exit_sell' on exactly this exchange + cycle;
//	state legality  — the order is QUEUED (the only pre-send state a PLACE may act on);
//	quantity        — the payload quantity is positive and EQUALS the registered order's
//	                  quantity (the registered order already passed venue precision rules
//	                  in sellflow — a payload that disagrees with the DB is refused);
//	routing         — the owning cycle is a real (dry_run=0) cycle;
//	inventory       — the cycle's entry buys actually FILLED a positive quantity;
//	no oversell     — this sell plus every other non-terminal/completed exit sell of the
//	                  cycle stays within the filled inventory (this also blocks duplicate
//	                  active sells: a duplicate's committed quantity would overshoot).
//
// Entry-side controls (kill switch, open-cycle cap, canary ack, configured entry caps,
// live-enable flags, failure/reconcile counters) deliberately do NOT apply: blocking a
// proven exit strands real inventory and INCREASES risk.
func (g *Guard) checkExitSell(ctx context.Context, p PlaceCheck) Decision {
	o, err := g.loadSendOrder(ctx, p.OrderID)
	if err != nil {
		return deny("exit check: cannot identify order/cycle/market (db) — fail closed")
	}
	if o.role != "exit_sell" {
		return deny("exit check: order is not an exit sell")
	}
	if o.exchangeID != p.ExchangeID || o.cycleID != p.CycleID {
		return deny("exit check: order does not belong to this exchange/cycle")
	}
	if o.state != string(state.OrderQueued) {
		return deny("exit check: order not in a sendable state (" + o.state + ")")
	}
	if o.dryRun != 0 {
		return deny("exit check: cycle is dry-run — must never be sent live")
	}
	// The sell must be routed to the SAME exchange+market where the inventory was acquired
	// (PR20 correction #5). A sell in the wrong market or symbol is not a risk-reducing exit:
	// it opens a NEW short-ish exposure somewhere we hold nothing.
	if o.marketExchangeID != o.exchangeID {
		return deny("exit check: market belongs to a different exchange than the order")
	}
	if p.ExchangeMarketID != 0 && o.marketID != p.ExchangeMarketID {
		return deny("exit check: resolved market does not match the order's market")
	}
	// The exit-sell symbol is MANDATORY (PR20 correction #6): an empty symbol is not "no
	// opinion", it is an unproven route. It must be present AND equal the registered market.
	if p.Symbol == "" || o.symbol != p.Symbol {
		return deny("exit check: request symbol missing or does not match the registered market")
	}
	if d := g.exitMarketMatchesInventory(ctx, p.CycleID, o); !d.Allow {
		return d
	}
	// The payload must equal the registered sell exactly (quantity/price/type/TIF/client id).
	if d := matchPayloadToOrder("exit", o, p); !d.Allow {
		return d
	}
	var boughtS string
	if err := g.db.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(filled_quantity),0) FROM orders WHERE cycle_id=? AND role='entry_buy'", p.CycleID).Scan(&boughtS); err != nil {
		return deny("exit check: cannot read filled inventory (db) — fail closed")
	}
	bought, berr := decimal.NewFromString(boughtS)
	if berr != nil || !bought.IsPositive() {
		return deny("exit check: no filled inventory to exit")
	}
	// Oversell math (PR20 correction #4). A CANCELLED sell may have PARTIALLY FILLED before
	// it was cancelled, so its final state must NOT exclude it: those units are gone from
	// inventory forever. The safety condition is
	//
	//     already_sold + remaining_active_commitments + this_request <= acquired_inventory
	//
	// where `already_sold` sums filled_quantity across ALL other exit sells of this cycle
	// (any state, cancelled included), and `remaining_active_commitments` sums the UNFILLED
	// remainder (quantity - filled_quantity) of only those other sells that can still
	// execute. A terminal order's remainder can never execute, so it contributes 0 — but its
	// fills still count above. NEEDS_RECONCILE is deliberately treated as still-active: its
	// remainder may be resting on the venue.
	var soldS, remainingS string
	if err := g.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(filled_quantity),0),
		       COALESCE(SUM(CASE WHEN state IN ('CANCELLED','FILLED','REJECTED','EXPIRED','FAILED')
		                         THEN 0 ELSE GREATEST(quantity - filled_quantity, 0) END),0)
		FROM orders WHERE cycle_id=? AND role='exit_sell' AND id<>?`,
		p.CycleID, p.OrderID).Scan(&soldS, &remainingS); err != nil {
		return deny("exit check: cannot read sold/committed sell quantity (db) — fail closed")
	}
	sold, serr := decimal.NewFromString(soldS)
	remaining, rerr := decimal.NewFromString(remainingS)
	if serr != nil || rerr != nil {
		return deny("exit check: committed sell quantity invalid")
	}
	if sold.Add(remaining).Add(p.BaseQty).GreaterThan(bought) {
		return deny("exit check: sell would exceed acquired inventory (oversell/duplicate — " +
			"includes fills from cancelled sells)")
	}
	return allow("risk-reducing exit within acquired inventory")
}

// canaryAckOK enforces the PR23 canary acknowledgement gate for a live BUY: when
// require_canary_ack is on, the buy must be within the configured canary exchange/symbol
// scope AND covered by an ACTIVE acknowledgement whose preflight hash still matches the
// current config (so a config change since the operator acknowledged invalidates it). This
// is what prevents accidental live trading even when credentials/caps/live flags exist.
func (g *Guard) canaryAckOK(ctx context.Context, ctrl Controls, exchangeID, marketID int64) Decision {
	if !ctrl.RequireCanaryAck {
		return allow("ack not required")
	}
	if ctrl.CanaryExchangeID == 0 || ctrl.CanaryMarketID == 0 {
		return deny("canary acknowledgement required but canary scope is not configured")
	}
	if exchangeID != ctrl.CanaryExchangeID || marketID != ctrl.CanaryMarketID {
		return deny("outside canary scope: only the acknowledged exchange/symbol may trade live")
	}
	ack, ok := preflight.ActiveAck(ctx, g.db, exchangeID, marketID)
	if !ok {
		return deny("no live acknowledgement: run preflight and acknowledge before live trading")
	}
	curHash, err := preflight.ConfigHash(ctx, g.db, exchangeID, marketID, "live")
	if err != nil {
		return deny("failed to compute preflight hash")
	}
	if ack.Hash != curHash {
		return deny("live acknowledgement is stale: config changed since preflight — re-run preflight and re-acknowledge")
	}
	// Expiry: a config-only hash cannot catch conditions that rot with time, so the
	// acknowledgement itself ages out and must be renewed.
	maxAge := g.canaryAckMaxAge(ctrl)
	if g.clk.Now().UTC().Sub(ack.AcknowledgedAt) > maxAge {
		return deny("live acknowledgement expired: re-run preflight and re-acknowledge")
	}
	// Dynamic re-check of the time-sensitive safety conditions immediately before the buy
	// (credential/market/balance freshness, reconcile cap, stuck-IN_FLIGHT, dangerous queue).
	if dok, reason := preflight.DynamicRecheck(ctx, g.db, g.clk, exchangeID, marketID); !dok {
		return deny("live recheck failed: " + reason)
	}
	// PR24: a live buy requires an ACTIVE canary run session. Stopping the session blocks
	// new buys immediately (sells/cancels/status are unaffected — they never reach here).
	if _, ok := ActiveSession(ctx, g.db, exchangeID, marketID); !ok {
		return deny("no active canary session: start a live run session before buying")
	}
	return allow("acknowledged")
}

func (g *Guard) canaryAckMaxAge(ctrl Controls) time.Duration {
	m := ctrl.CanaryAckMaxAgeMin
	if m <= 0 {
		m = 30 // safe default
	}
	return time.Duration(m) * time.Minute
}

// CheckCancel gates a real CANCEL_ORDER. A cancel reduces risk, so it is permitted even
// with the kill switch engaged, but still requires credentials + not dry-run (credential
// verification fails CLOSED on a DB error). Audited — with a deliberately DIFFERENT audit
// policy from a place (PR20 correction): a cancel is exposure-REDUCING, so an audit-write
// outage must not create a dangerous inability to cancel. If the audit insert fails on an
// allowed cancel, the cancel still proceeds; the full decision is logged at Error level
// (safe fields only) so the missing row can be reconstructed/persisted later.
// checkCancel is the pure (no-audit) cancel decision.
func (g *Guard) checkCancel(ctx context.Context, p PlaceCheck) Decision {
	if p.DryRun {
		return deny("dry-run request must never be sent live")
	}
	credOK, err := g.credentialsAvailable(ctx, p.ExchangeID)
	if err != nil {
		return deny("cannot verify credentials (db error) — fail closed")
	}
	if !credOK {
		return deny("no active credentials for exchange")
	}
	// PR20 correction #3: a cancel is risk-reducing ONLY if it cancels the order we
	// think it does. The payload's exchange_order_id is never trusted on its own —
	// prove ownership + the exact venue id + a cancellable state from the DB.
	return g.checkCancelTarget(ctx, p)
}

// CheckCancelNoAudit is a pure cancel check (no audit).
func (g *Guard) CheckCancelNoAudit(ctx context.Context, p PlaceCheck) Decision {
	return g.checkCancel(ctx, p)
}

// CheckCancelEarly is the EARLY, pre-pacing cancel pre-filter: audits denials only; the
// authoritative record is written by CheckCancel at the final pre-send guard (PR20 #2).
func (g *Guard) CheckCancelEarly(ctx context.Context, p PlaceCheck) Decision {
	d := g.checkCancel(ctx, p)
	if !d.Allow {
		if aerr := g.audit(ctx, auditRow{ExchangeID: p.ExchangeID, CycleID: p.CycleID, OrderID: p.OrderID,
			RequestID: p.RequestID, Action: "cancel", Decision: d}); aerr != nil {
			g.log.Error("live: audit write failed for an early cancel denial", "reason", d.Reason, "err", aerr)
		}
	}
	return d
}

func (g *Guard) CheckCancel(ctx context.Context, p PlaceCheck) Decision {
	d := g.checkCancel(ctx, p)
	if aerr := g.audit(ctx, auditRow{ExchangeID: p.ExchangeID, CycleID: p.CycleID, OrderID: p.OrderID, RequestID: p.RequestID,
		Action: "cancel", Decision: d}); aerr != nil {
		// Do NOT flip an allowed cancel to deny: blocking a risk-reducing cancel because the
		// audit table is unavailable is more dangerous than a missing audit row. Reconstructible
		// record goes to the log (no secrets).
		g.log.Error("live: cancel audit write failed — cancel decision NOT blocked",
			"decision", d.Allow, "reason", d.Reason,
			"exchange_id", p.ExchangeID, "cycle_id", p.CycleID, "order_id", p.OrderID, "request_id", p.RequestID, "err", aerr)
	}
	return d
}

// AllowNewBuyCycle is checked by the engine BEFORE creating a live buy cycle: kill
// switch off, controls configured, exchange/symbol live-enabled, open-cycle cap not
// exceeded. (Belt: the executor re-checks before the actual send.) Every safety-query
// error DENIES (fail closed).
func (g *Guard) AllowNewBuyCycle(ctx context.Context, exchangeID, exchangeMarketID int64) Decision {
	ctrl, err := g.LoadControls(ctx)
	if err != nil {
		return deny("failed to load live controls")
	}
	if !ctrl.Configured() {
		return deny("live not configured")
	}
	if ctrl.KillSwitch {
		return deny("kill switch engaged")
	}
	liveOK, err := g.live(ctx, exchangeID, exchangeMarketID)
	if err != nil {
		return deny("cannot verify live flags (db error) — fail closed")
	}
	if !liveOK {
		return deny("exchange or symbol not live-enabled")
	}
	open, err := g.openCycles(ctx)
	if err != nil {
		return deny("cannot count open cycles (db error) — fail closed")
	}
	if open >= ctrl.MaxOpenCycles {
		return deny("max open cycles reached")
	}
	if d := g.canaryAckOK(ctx, ctrl, exchangeID, exchangeMarketID); !d.Allow {
		return d
	}
	return allow("ok")
}

// ---- queries ----
//
// PR20 correction: every safety query returns its error EXPLICITLY, and every caller
// treats a DB error as DENY (fail closed). A safety condition that cannot be evaluated
// must never read as "0, therefore fine". The daily order-count / daily quote queries
// were removed outright (owner decision: no daily limits).

func (g *Guard) live(ctx context.Context, exchangeID, exchangeMarketID int64) (bool, error) {
	var exLive, mkLive int
	if err := g.db.QueryRowContext(ctx, "SELECT live_enabled FROM exchanges WHERE id=?", exchangeID).Scan(&exLive); err != nil {
		return false, err
	}
	if exLive == 0 {
		return false, nil
	}
	if exchangeMarketID == 0 {
		return true, nil
	}
	if err := g.db.QueryRowContext(ctx, "SELECT live_enabled FROM exchange_markets WHERE id=?", exchangeMarketID).Scan(&mkLive); err != nil {
		return false, err
	}
	return mkLive != 0, nil
}

func (g *Guard) credentialsAvailable(ctx context.Context, exchangeID int64) (bool, error) {
	var n int
	if err := g.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active'", exchangeID).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

func (g *Guard) openCycles(ctx context.Context) (int, error) {
	var n int
	err := g.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state NOT IN ('CLOSED','CANCELLED','FAILED')").Scan(&n)
	return n, err
}

func (g *Guard) unresolvedReconcile(ctx context.Context) (int, error) {
	var n int
	err := g.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state='NEEDS_RECONCILE'").Scan(&n)
	return n, err
}

func (g *Guard) consecutiveFailures(ctx context.Context, exchangeID int64) (int, error) {
	// Same-day count of failed/dead PLACE requests for the exchange (a coarse health brake).
	var n int
	err := g.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_requests WHERE exchange_id=? AND request_type='PLACE_ORDER' AND status IN ('FAILED','DEAD') AND created_at >= CURDATE()", exchangeID).Scan(&n)
	return n, err
}

// ---- audit ----

type auditRow struct {
	ExchangeID       int64
	ExchangeMarketID int64
	CycleID          int64
	OrderID          int64
	RequestID        int64
	Action           string
	Side             string
	Notional         decimal.Decimal
	Decision         Decision
}

// audit durably records one guard decision and returns the insert error (PR20 correction:
// callers decide the policy — CheckPlace fails CLOSED on an allow it could not persist;
// CheckCancel proceeds but logs, so an audit outage cannot block risk reduction).
func (g *Guard) audit(ctx context.Context, a auditRow) error {
	decision := "deny"
	if a.Decision.Allow {
		decision = "allow"
	}
	// PR24 correlation: tag every live-guard decision with the run session, acknowledgement,
	// and config hash in force for the scope (best-effort; all nullable).
	sessionID, ackID, hash := g.correlation(ctx, a.ExchangeID, a.ExchangeMarketID)
	_, err := g.db.ExecContext(ctx, `
INSERT INTO live_audit (exchange_id, exchange_market_id, cycle_id, order_id, request_id, action, side, notional, decision, reason, execution_mode, live_session_id, acknowledgement_id, preflight_hash)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live', ?, ?, ?)`,
		nz(a.ExchangeID), nz(a.ExchangeMarketID), nz(a.CycleID), nz(a.OrderID), nz(a.RequestID),
		a.Action, nstr(a.Side), decOrNull(a.Notional), decision, a.Decision.Reason, sessionID, ackID, nstr(hash))
	return err
}

// correlation looks up the active session id + acknowledgement id + current config hash for
// the scope (best-effort; any may be nil/empty). marketID 0 (e.g. a cancel) skips
// session/ack lookup but still records the config hash by exchange where derivable.
func (g *Guard) correlation(ctx context.Context, exchangeID, marketID int64) (sessionID any, ackID any, hash string) {
	if exchangeID == 0 || marketID == 0 {
		return nil, nil, ""
	}
	if sess, ok := ActiveSession(ctx, g.db, exchangeID, marketID); ok {
		sessionID = sess.ID
	}
	if ack, ok := preflight.ActiveAck(ctx, g.db, exchangeID, marketID); ok {
		ackID = ack.ID
	}
	hash, _ = preflight.ConfigHash(ctx, g.db, exchangeID, marketID, "live")
	return sessionID, ackID, hash
}

func nz(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}
func nstr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func decOrNull(d decimal.Decimal) any {
	if d.IsZero() {
		return nil
	}
	return d.String()
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
