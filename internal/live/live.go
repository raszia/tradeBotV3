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
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/preflight"
)

// Controls is the global live-control row (caps + kill switch). A nil/zero cap means
// "not configured" — which makes Configured() false and blocks all live trading.
type Controls struct {
	KillSwitch             bool
	MaxOpenCycles          int
	MaxDailyOrders         int
	MaxDailyQuote          decimal.Decimal
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
func (c Controls) Configured() bool {
	return c.loaded &&
		c.MaxOpenCycles > 0 && c.MaxDailyOrders > 0 &&
		c.MaxOrderNotional.IsPositive() && c.MaxBaseQty.IsPositive() && c.MaxDailyQuote.IsPositive() &&
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

// LoadControls reads the singleton live_controls row.
func (g *Guard) LoadControls(ctx context.Context) (Controls, error) {
	var c Controls
	var (
		kill, reqAck                              int
		maxOpen, maxDaily, maxConsec, maxUnres    sql.NullInt64
		maxDailyQuote, maxNotional, maxBase       sql.NullString
		cfgVersion, canaryEx, canaryMk, ackMaxAge sql.NullInt64
	)
	err := g.db.QueryRowContext(ctx,
		"SELECT kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty, max_consecutive_failures, max_unresolved_reconcile, COALESCE(config_version,0), require_canary_ack, canary_exchange_id, canary_market_id, canary_ack_max_age_minutes FROM live_controls WHERE id=1").
		Scan(&kill, &maxOpen, &maxDaily, &maxDailyQuote, &maxNotional, &maxBase, &maxConsec, &maxUnres, &cfgVersion, &reqAck, &canaryEx, &canaryMk, &ackMaxAge)
	if errors.Is(err, sql.ErrNoRows) {
		return Controls{KillSwitch: true}, nil // no row => safe: kill switch engaged, unconfigured
	}
	if err != nil {
		return Controls{}, err
	}
	c.loaded = true
	c.KillSwitch = kill != 0
	c.MaxOpenCycles = int(maxOpen.Int64)
	c.MaxDailyOrders = int(maxDaily.Int64)
	c.MaxConsecutiveFailures = int(maxConsec.Int64)
	c.MaxUnresolvedReconcile = int(maxUnres.Int64)
	c.MaxDailyQuote = decOrZero(maxDailyQuote)
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
}

// CheckPlace is the final gate before a real PLACE_ORDER. It DENIES unless every
// condition holds. A BUY place is additionally blocked by the kill switch and the
// open-cycle / daily caps (new exposure); a SELL place (exiting existing inventory) is
// allowed under the kill switch and is not subject to the new-cycle cap, but still
// honours notional/qty/credential/live-flag checks. Decisions are audited.
func (g *Guard) CheckPlace(ctx context.Context, p PlaceCheck) Decision {
	d := g.checkPlace(ctx, p)
	g.audit(ctx, auditRow{ExchangeID: p.ExchangeID, ExchangeMarketID: p.ExchangeMarketID, CycleID: p.CycleID,
		OrderID: p.OrderID, RequestID: p.RequestID, Action: "place_" + p.Side, Side: p.Side, Notional: p.Notional, Decision: d})
	return d
}

func (g *Guard) checkPlace(ctx context.Context, p PlaceCheck) Decision {
	if p.DryRun {
		return deny("dry-run request must never be sent live")
	}
	ctrl, err := g.LoadControls(ctx)
	if err != nil {
		return deny("failed to load live controls")
	}
	if !ctrl.Configured() {
		return deny("live not configured (a required cap is missing)")
	}
	if !g.live(ctx, p.ExchangeID, p.ExchangeMarketID) {
		return deny("exchange or symbol not live-enabled")
	}
	if !g.credentialsAvailable(ctx, p.ExchangeID) {
		return deny("no active credentials for exchange")
	}
	if p.Notional.GreaterThan(ctrl.MaxOrderNotional) {
		return deny("order notional exceeds max_order_notional")
	}
	if p.BaseQty.GreaterThan(ctrl.MaxBaseQty) {
		return deny("order base quantity exceeds max_base_qty")
	}
	if n := g.unresolvedReconcile(ctx); n > ctrl.MaxUnresolvedReconcile {
		return deny("too many unresolved NEEDS_RECONCILE")
	}
	if n := g.consecutiveFailures(ctx, p.ExchangeID); n >= ctrl.MaxConsecutiveFailures {
		return deny("too many consecutive failures")
	}
	if g.dailyOrders(ctx) >= ctrl.MaxDailyOrders {
		return deny("max daily order count reached")
	}
	if g.dailyQuote(ctx).Add(p.Notional).GreaterThan(ctrl.MaxDailyQuote) {
		return deny("max daily quote exposure reached")
	}
	// BUY-only: kill switch + open-cycle cap (new exposure) + canary acknowledgement.
	if p.Side == "buy" {
		if ctrl.KillSwitch {
			return deny("kill switch engaged: no new buy orders")
		}
		if g.openCycles(ctx) > ctrl.MaxOpenCycles {
			return deny("max open cycles reached")
		}
		if d := g.canaryAckOK(ctx, ctrl, p.ExchangeID, p.ExchangeMarketID); !d.Allow {
			return d
		}
	}
	return allow("ok")
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
// with the kill switch engaged, but still requires live mode + credentials + not
// dry-run. Audited.
func (g *Guard) CheckCancel(ctx context.Context, p PlaceCheck) Decision {
	d := func() Decision {
		if p.DryRun {
			return deny("dry-run request must never be sent live")
		}
		if !g.credentialsAvailable(ctx, p.ExchangeID) {
			return deny("no active credentials for exchange")
		}
		return allow("cancel reduces risk")
	}()
	g.audit(ctx, auditRow{ExchangeID: p.ExchangeID, CycleID: p.CycleID, OrderID: p.OrderID, RequestID: p.RequestID,
		Action: "cancel", Decision: d})
	return d
}

// AllowNewBuyCycle is checked by the engine BEFORE creating a live buy cycle: kill
// switch off, controls configured, exchange/symbol live-enabled, open-cycle cap not
// exceeded. (Belt: the executor re-checks before the actual send.)
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
	if !g.live(ctx, exchangeID, exchangeMarketID) {
		return deny("exchange or symbol not live-enabled")
	}
	if g.openCycles(ctx) >= ctrl.MaxOpenCycles {
		return deny("max open cycles reached")
	}
	if d := g.canaryAckOK(ctx, ctrl, exchangeID, exchangeMarketID); !d.Allow {
		return d
	}
	return allow("ok")
}

// ---- queries ----

func (g *Guard) live(ctx context.Context, exchangeID, exchangeMarketID int64) bool {
	var exLive, mkLive int
	if err := g.db.QueryRowContext(ctx, "SELECT live_enabled FROM exchanges WHERE id=?", exchangeID).Scan(&exLive); err != nil || exLive == 0 {
		return false
	}
	if exchangeMarketID == 0 {
		return true
	}
	if err := g.db.QueryRowContext(ctx, "SELECT live_enabled FROM exchange_markets WHERE id=?", exchangeMarketID).Scan(&mkLive); err != nil {
		return false
	}
	return mkLive != 0
}

func (g *Guard) credentialsAvailable(ctx context.Context, exchangeID int64) bool {
	var n int
	g.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active'", exchangeID).Scan(&n)
	return n > 0
}

func (g *Guard) openCycles(ctx context.Context) int {
	var n int
	g.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state NOT IN ('CLOSED','CANCELLED','FAILED')").Scan(&n)
	return n
}

func (g *Guard) unresolvedReconcile(ctx context.Context) int {
	var n int
	g.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state='NEEDS_RECONCILE'").Scan(&n)
	return n
}

func (g *Guard) dailyOrders(ctx context.Context) int {
	var n int
	g.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM orders o JOIN cycles c ON c.id=o.cycle_id WHERE c.dry_run=0 AND o.created_at >= CURDATE()").Scan(&n)
	return n
}

func (g *Guard) dailyQuote(ctx context.Context) decimal.Decimal {
	var s sql.NullString
	g.db.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(o.quantity*o.limit_price),0) FROM orders o JOIN cycles c ON c.id=o.cycle_id WHERE c.dry_run=0 AND o.role='entry_buy' AND o.created_at >= CURDATE()").Scan(&s)
	return decOrZero(s)
}

func (g *Guard) consecutiveFailures(ctx context.Context, exchangeID int64) int {
	// Count consecutive failed/dead PLACE requests at the tail of recent history.
	var n int
	g.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_requests WHERE exchange_id=? AND request_type='PLACE_ORDER' AND status IN ('FAILED','DEAD') AND created_at >= CURDATE()", exchangeID).Scan(&n)
	return n
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

func (g *Guard) audit(ctx context.Context, a auditRow) {
	decision := "deny"
	if a.Decision.Allow {
		decision = "allow"
	}
	if _, err := g.db.ExecContext(ctx, `
INSERT INTO live_audit (exchange_id, exchange_market_id, cycle_id, order_id, request_id, action, side, notional, decision, reason, execution_mode)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live')`,
		nz(a.ExchangeID), nz(a.ExchangeMarketID), nz(a.CycleID), nz(a.OrderID), nz(a.RequestID),
		a.Action, nstr(a.Side), decOrNull(a.Notional), decision, a.Decision.Reason); err != nil {
		g.log.Warn("live: audit write failed", "err", err)
	}
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
