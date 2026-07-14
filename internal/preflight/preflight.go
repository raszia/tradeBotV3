// Package preflight is the LIVE READINESS checklist + canary acknowledgement (PR23). It
// is strictly READ-ONLY: it inspects DB state and reports whether the system is ready for
// real live execution — it never places/cancels orders, never mutates trading state, and
// holds no exchange client (DB handle only). A live BUY additionally requires an explicit
// operator acknowledgement bound to the preflight's config hash (enforced by the PR20
// guard), so live trading cannot start accidentally even when credentials/caps/live flags
// exist. Preflight does NOT replace the executor-side guard — both are mandatory.
package preflight

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"v3TradeBot/internal/clock"
)

// Status is a per-check verdict.
type Status string

const (
	StatusPass Status = "pass"
	StatusFail Status = "fail"
	StatusWarn Status = "warn" // advisory: recorded, does not block readiness
)

// Check is one readiness check result.
type Check struct {
	Name   string `json:"name"`
	Status Status `json:"status"`
	Detail string `json:"detail"`
}

// Report is the full preflight result. Ready is true only when NO check failed.
type Report struct {
	Ready        bool     `json:"ready"`
	ExchangeID   int64    `json:"exchange_id"`
	MarketID     int64    `json:"exchange_market_id"`
	ExchangeCode string   `json:"exchange_code"`
	Symbol       string   `json:"canonical_symbol"`
	Checks       []Check  `json:"checks"`
	Failures     []string `json:"failures,omitempty"`
	Warnings     []string `json:"warnings,omitempty"`
	// ConfigHash binds an acknowledgement to the config-relevant inputs (caps, live flags,
	// canary scope, credential identity, freshness windows, mode). It deliberately EXCLUDES
	// ephemeral freshness values + the kill switch, so a market tick does not invalidate an
	// ack but a config change does.
	ConfigHash string `json:"config_hash"`
}

// freshness-window defaults (used when the live_controls column is NULL).
const (
	defCredAgeMin   = 60
	defMarketAgeSec = 30
	defBalanceMin   = 10
	defDryRunMin    = 1440
	defCanaryAckMin = 30
)

// AckMissing/AckStale describe why a live ack is not usable (for the guard + dashboard).
var (
	ErrNotReady = errors.New("preflight: not ready (one or more checks failed)")
)

// Checker runs the readiness checks. It holds ONLY a DB handle (no exchange client), so it
// cannot place/cancel anything by construction.
type Checker struct {
	db   *sql.DB
	clk  clock.Clock
	log  *slog.Logger
	mode string // execution mode ("off"|"dry_run"|"live")
}

// New builds a Checker. mode is the system's configured execution mode.
func New(db *sql.DB, clk clock.Clock, log *slog.Logger, mode string) *Checker {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Checker{db: db, clk: clk, log: log, mode: mode}
}

// controls is the subset of live_controls preflight needs (raw read; no dependency on
// internal/live, to avoid an import cycle).
type controls struct {
	exists               bool
	killSwitch           bool
	maxOpenCycles        sql.NullInt64
	maxOrderNotional     sql.NullString
	maxBaseQty           sql.NullString
	maxConsecFailures    sql.NullInt64
	maxUnresolvedRecon   sql.NullInt64
	requireCanaryAck     bool
	canaryExchangeID     sql.NullInt64
	canaryMarketID       sql.NullInt64
	credValidationMaxMin sql.NullInt64
	marketDataMaxSec     sql.NullInt64
	balanceMaxMin        sql.NullInt64
	dryRunSuccessMaxMin  sql.NullInt64
	canaryAckMaxMin      sql.NullInt64
	healthRequired       bool
}

func loadControls(ctx context.Context, q querier) (controls, error) {
	var c controls
	var kill, reqAck, healthReq int
	err := q.QueryRowContext(ctx, `
SELECT kill_switch, max_open_cycles, max_order_notional, max_base_qty,
  max_consecutive_failures, max_unresolved_reconcile, require_canary_ack, canary_exchange_id, canary_market_id,
  credential_validation_max_age_minutes, market_data_max_age_seconds, balance_max_age_minutes,
  dry_run_success_max_age_minutes, canary_ack_max_age_minutes, health_required
FROM live_controls WHERE id=1`).Scan(
		&kill, &c.maxOpenCycles, &c.maxOrderNotional, &c.maxBaseQty,
		&c.maxConsecFailures, &c.maxUnresolvedRecon, &reqAck, &c.canaryExchangeID, &c.canaryMarketID,
		&c.credValidationMaxMin, &c.marketDataMaxSec, &c.balanceMaxMin, &c.dryRunSuccessMaxMin, &c.canaryAckMaxMin, &healthReq)
	if errors.Is(err, sql.ErrNoRows) {
		return controls{exists: false, killSwitch: true}, nil
	}
	if err != nil {
		return controls{}, err
	}
	c.exists = true
	c.killSwitch = kill != 0
	c.requireCanaryAck = reqAck != 0
	c.healthRequired = healthReq != 0
	return c, nil
}

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Run executes every readiness check for the target exchange/market. When exchangeID or
// marketID is 0 the canary scope from live_controls is used. It mutates nothing.
func (c *Checker) Run(ctx context.Context, exchangeID, marketID int64) (Report, error) {
	ctrl, err := loadControls(ctx, c.db)
	if err != nil {
		return Report{}, err
	}
	if exchangeID == 0 && ctrl.canaryExchangeID.Valid {
		exchangeID = ctrl.canaryExchangeID.Int64
	}
	if marketID == 0 && ctrl.canaryMarketID.Valid {
		marketID = ctrl.canaryMarketID.Int64
	}

	r := Report{ExchangeID: exchangeID, MarketID: marketID}
	add := func(name string, st Status, detail string) {
		r.Checks = append(r.Checks, Check{Name: name, Status: st, Detail: detail})
	}

	// Resolve exchange/symbol labels (also a sanity check on the canary scope).
	var code, symbol string
	if exchangeID == 0 || marketID == 0 {
		add("canary_scope_configured", StatusFail, "canary exchange/market not set (live_controls.canary_exchange_id / canary_market_id)")
	} else {
		_ = c.db.QueryRowContext(ctx, "SELECT code FROM exchanges WHERE id=?", exchangeID).Scan(&code)
		_ = c.db.QueryRowContext(ctx, "SELECT canonical_symbol FROM exchange_markets WHERE id=?", marketID).Scan(&symbol)
		r.ExchangeCode, r.Symbol = code, symbol
		if code == "" || symbol == "" {
			add("canary_scope_configured", StatusFail, "canary exchange/market id does not resolve")
		} else {
			add("canary_scope_configured", StatusPass, fmt.Sprintf("canary scope = %s %s", code, symbol))
		}
	}

	// 1. execution mode + AllowLiveExecution (the executor derives AllowLiveExecution from
	// mode=live; preflight checks the mode, the executor enforces the runtime flag).
	if c.mode == "live" {
		add("execution_mode_live", StatusPass, "execution mode is live (executor sets AllowLiveExecution)")
	} else {
		add("execution_mode_live", StatusFail, fmt.Sprintf("execution mode is %q, not live", c.mode))
	}

	// 2. kill switch.
	if !ctrl.exists {
		add("kill_switch_known", StatusFail, "no live_controls row (kill switch state unknown -> treated engaged)")
	} else {
		add("kill_switch_known", StatusPass, "live_controls present")
		if ctrl.killSwitch {
			add("kill_switch_disengaged", StatusFail, "kill switch is ENGAGED: new buys are blocked")
		} else {
			add("kill_switch_disengaged", StatusPass, "kill switch is disengaged")
		}
	}

	// 3. live flags.
	c.checkLive(ctx, exchangeID, marketID, add)

	// 4. caps configured + sane.
	c.checkCaps(ctrl, add)

	// 5. credentials (exists/enabled/active/validated/fresh).
	credID := c.checkCredential(ctx, exchangeID, ctrl, add)

	// 6. private health.
	c.checkHealth(ctx, exchangeID, ctrl, add)

	// 7. balance freshness.
	c.checkBalance(ctx, exchangeID, ctrl, add)

	// 8. market data freshness (DB proxy: a recent comparison_event implies fresh Binance +
	// Iranian books, since the engine writes one only when both are fresh).
	c.checkMarketData(ctx, symbol, ctrl, add)

	// 9. reconcile / queue / lock hygiene.
	c.checkReconcile(ctx, ctrl, add)
	c.checkStuckInflight(ctx, exchangeID, add)
	c.checkStaleLock(ctx, add)
	c.checkDangerousQueue(ctx, exchangeID, add)

	// 10. recent successful dry-run for the same exchange/symbol.
	c.checkDryRun(ctx, marketID, ctrl, add)

	// 11. operational: auth path + audit path.
	c.checkAuthPath(ctx, add)
	c.checkAuditPath(ctx, add)

	// Summarize.
	for _, ch := range r.Checks {
		switch ch.Status {
		case StatusFail:
			r.Failures = append(r.Failures, ch.Name)
		case StatusWarn:
			r.Warnings = append(r.Warnings, ch.Name)
		}
	}
	r.Ready = len(r.Failures) == 0
	r.ConfigHash, err = ConfigHash(ctx, c.db, exchangeID, marketID, c.mode)
	if err != nil {
		return Report{}, err
	}
	_ = credID
	return r, nil
}

func (c *Checker) checkLive(ctx context.Context, exchangeID, marketID int64, add func(string, Status, string)) {
	var exLive, mkLive int
	_ = c.db.QueryRowContext(ctx, "SELECT live_enabled FROM exchanges WHERE id=?", exchangeID).Scan(&exLive)
	if exLive == 1 {
		add("exchange_live_enabled", StatusPass, "exchange live_enabled")
	} else {
		add("exchange_live_enabled", StatusFail, "exchange not live-enabled")
	}
	_ = c.db.QueryRowContext(ctx, "SELECT live_enabled FROM exchange_markets WHERE id=?", marketID).Scan(&mkLive)
	if mkLive == 1 {
		add("symbol_live_enabled", StatusPass, "symbol live_enabled")
	} else {
		add("symbol_live_enabled", StatusFail, "symbol not live-enabled")
	}
}

func (c *Checker) checkCaps(ctrl controls, add func(string, Status, string)) {
	// Daily order-count / quote caps were removed from live decisions (owner decision) and
	// are no longer required for a configured live setup.
	configured := ctrl.exists &&
		ctrl.maxOpenCycles.Valid &&
		ctrl.maxOrderNotional.Valid && ctrl.maxBaseQty.Valid && ctrl.maxConsecFailures.Valid && ctrl.maxUnresolvedRecon.Valid
	if !configured {
		add("caps_configured", StatusFail, "one or more live caps are not set")
		return
	}
	add("caps_configured", StatusPass, "all caps set")
	sane := ctrl.maxOpenCycles.Int64 > 0 &&
		positive(ctrl.maxOrderNotional.String) && positive(ctrl.maxBaseQty.String) &&
		ctrl.maxConsecFailures.Int64 > 0 && ctrl.maxUnresolvedRecon.Int64 >= 0
	if !sane {
		add("caps_sane", StatusFail, "a cap is zero or negative")
	} else {
		add("caps_sane", StatusPass, "caps are positive")
	}
	// Canary expectation: a tiny first rollout uses a single open cycle.
	if ctrl.maxOpenCycles.Int64 == 1 {
		add("canary_single_cycle_cap", StatusPass, "max_open_cycles = 1 (canary)")
	} else {
		add("canary_single_cycle_cap", StatusWarn, fmt.Sprintf("max_open_cycles = %d (canary expects 1)", ctrl.maxOpenCycles.Int64))
	}
}

func (c *Checker) checkCredential(ctx context.Context, exchangeID int64, ctrl controls, add func(string, Status, string)) int64 {
	var id int64
	var enabled int
	var status string
	var lastChecked sql.NullTime
	err := c.db.QueryRowContext(ctx, `
SELECT id, enabled, status, last_checked_at FROM exchange_credentials
WHERE exchange_id=? AND enabled=1 AND status='active' ORDER BY key_version DESC, id DESC LIMIT 1`, exchangeID).
		Scan(&id, &enabled, &status, &lastChecked)
	if errors.Is(err, sql.ErrNoRows) {
		add("credential_active", StatusFail, "no enabled+active credential for the exchange")
		return 0
	}
	if err != nil {
		add("credential_active", StatusFail, "credential lookup error")
		return 0
	}
	add("credential_active", StatusPass, fmt.Sprintf("credential %d enabled+active", id))
	if !lastChecked.Valid {
		add("credential_validated", StatusFail, "credential never validated (last_checked_at is null)")
		return id
	}
	add("credential_validated", StatusPass, "credential has been validated")
	maxAge := nullIntOr(ctrl.credValidationMaxMin, defCredAgeMin)
	age := c.clk.Now().UTC().Sub(lastChecked.Time)
	if age.Minutes() > float64(maxAge) {
		add("credential_validation_fresh", StatusFail, fmt.Sprintf("validation is %.0fm old (max %dm)", age.Minutes(), maxAge))
	} else {
		add("credential_validation_fresh", StatusPass, fmt.Sprintf("validated %.0fm ago", age.Minutes()))
	}
	return id
}

func (c *Checker) checkHealth(ctx context.Context, exchangeID int64, ctrl controls, add func(string, Status, string)) {
	var apiKeyStatus string
	err := c.db.QueryRowContext(ctx, "SELECT api_key_status FROM exchange_health_current WHERE exchange_id=?", exchangeID).Scan(&apiKeyStatus)
	healthy := err == nil && apiKeyStatus == "ok"
	if healthy {
		add("private_health_ok", StatusPass, "private health api_key_status=ok")
		return
	}
	detail := fmt.Sprintf("private health not ok (api_key_status=%q)", apiKeyStatus)
	if ctrl.healthRequired {
		add("private_health_ok", StatusFail, detail+" and health_required=1")
	} else {
		add("private_health_ok", StatusWarn, detail+" (accepted: health_required=0)")
	}
}

func (c *Checker) checkBalance(ctx context.Context, exchangeID int64, ctrl controls, add func(string, Status, string)) {
	ok, detail := balanceFresh(ctx, c.db, c.clk, exchangeID, ctrl)
	add("balance_recent", passFail(ok), detail)
}

func (c *Checker) checkMarketData(ctx context.Context, symbol string, ctrl controls, add func(string, Status, string)) {
	ok, detail, hasBinance := marketFresh(ctx, c.db, c.clk, symbol, ctrl)
	add("market_data_fresh", passFail(ok), detail)
	if ok && hasBinance {
		add("binance_reference_fresh", StatusPass, "Binance reference present in recent comparison")
	} else {
		add("binance_reference_fresh", passFail(ok), "Binance reference "+detail)
	}
	add("iranian_market_fresh", passFail(ok), "Iranian book "+detail+" (implied by the comparison)")
}

func (c *Checker) checkReconcile(ctx context.Context, ctrl controls, add func(string, Status, string)) {
	ok, detail := reconcileWithinCap(ctx, c.db, ctrl)
	add("reconcile_within_cap", passFail(ok), detail)
}

func (c *Checker) checkStuckInflight(ctx context.Context, exchangeID int64, add func(string, Status, string)) {
	ok, detail := noStuckInflight(ctx, c.db, exchangeID)
	add("no_stuck_inflight", passFail(ok), detail)
}

func (c *Checker) checkStaleLock(ctx context.Context, add func(string, Status, string)) {
	var n int
	c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM symbol_locks WHERE state='ACTIVE' AND expires_at < NOW(6)").Scan(&n)
	if n > 0 {
		add("no_stale_lock", StatusFail, fmt.Sprintf("%d expired ACTIVE symbol lock(s)", n))
	} else {
		add("no_stale_lock", StatusPass, "no stale symbol lock")
	}
}

func (c *Checker) checkDangerousQueue(ctx context.Context, exchangeID int64, add func(string, Status, string)) {
	ok, detail := noDangerousQueue(ctx, c.db, exchangeID)
	add("no_dangerous_queue_state", passFail(ok), detail)
}

// ---- dynamic safety predicates (shared by Run + DynamicRecheck) ----
//
// These are the TIME-SENSITIVE conditions that can rot after an acknowledgement even when
// config does not change. The guard re-checks them immediately before every live buy, so a
// config-only hash is never the sole gate.

func credValidationFresh(ctx context.Context, q querier, clk clock.Clock, exchangeID int64, ctrl controls) (bool, string) {
	var status string
	var lastChecked sql.NullTime
	err := q.QueryRowContext(ctx,
		"SELECT status, last_checked_at FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active' ORDER BY key_version DESC, id DESC LIMIT 1", exchangeID).
		Scan(&status, &lastChecked)
	if err != nil {
		return false, "no active credential"
	}
	if !lastChecked.Valid {
		return false, "credential never validated"
	}
	maxAge := nullIntOr(ctrl.credValidationMaxMin, defCredAgeMin)
	age := clk.Now().UTC().Sub(lastChecked.Time)
	if age.Minutes() > float64(maxAge) {
		return false, fmt.Sprintf("credential validation %.0fm old (max %dm)", age.Minutes(), maxAge)
	}
	return true, fmt.Sprintf("validated %.0fm ago", age.Minutes())
}

func marketFresh(ctx context.Context, q querier, clk clock.Clock, symbol string, ctrl controls) (bool, string, bool) {
	maxAge := nullIntOr(ctrl.marketDataMaxSec, defMarketAgeSec)
	var newest sql.NullTime
	var hasBinance sql.NullBool
	q.QueryRowContext(ctx,
		"SELECT MAX(created_at), MAX(binance_price IS NOT NULL) FROM comparison_events WHERE canonical_symbol=?", symbol).
		Scan(&newest, &hasBinance)
	if !newest.Valid {
		return false, "no recent comparison_event for the symbol", false
	}
	age := clk.Now().UTC().Sub(newest.Time)
	if age.Seconds() > float64(maxAge) {
		return false, fmt.Sprintf("last comparison %.0fs ago (max %ds)", age.Seconds(), maxAge), hasBinance.Bool
	}
	return true, fmt.Sprintf("last comparison %.0fs ago (max %ds)", age.Seconds(), maxAge), hasBinance.Bool
}

func balanceFresh(ctx context.Context, q querier, clk clock.Clock, exchangeID int64, ctrl controls) (bool, string) {
	var newest sql.NullTime
	q.QueryRowContext(ctx, "SELECT MAX(COALESCE(last_seen_at, updated_at)) FROM wallet_balances_current WHERE exchange_id=?", exchangeID).Scan(&newest)
	if !newest.Valid {
		return false, "no balance snapshot (balance-sync has no data)"
	}
	maxAge := nullIntOr(ctrl.balanceMaxMin, defBalanceMin)
	age := clk.Now().UTC().Sub(newest.Time)
	if age.Minutes() > float64(maxAge) {
		return false, fmt.Sprintf("balance %.0fm old (max %dm)", age.Minutes(), maxAge)
	}
	return true, fmt.Sprintf("balance updated %.0fm ago", age.Minutes())
}

func reconcileWithinCap(ctx context.Context, q querier, ctrl controls) (bool, string) {
	var n int
	q.QueryRowContext(ctx, "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state='NEEDS_RECONCILE'").Scan(&n)
	cap := int(nullIntOr(ctrl.maxUnresolvedRecon, 0))
	if n > cap {
		return false, fmt.Sprintf("%d unresolved NEEDS_RECONCILE (cap %d)", n, cap)
	}
	return true, fmt.Sprintf("%d unresolved NEEDS_RECONCILE (cap %d)", n, cap)
}

func noStuckInflight(ctx context.Context, q querier, exchangeID int64) (bool, string) {
	var n int
	q.QueryRowContext(ctx, `
SELECT COUNT(*) FROM exchange_requests
WHERE exchange_id=? AND status='IN_FLIGHT' AND request_type IN ('PLACE_ORDER','CANCEL_ORDER')
  AND inflight_at IS NOT NULL AND inflight_at < (NOW(6) - INTERVAL (timeout_ms/1000) SECOND)`, exchangeID).Scan(&n)
	if n > 0 {
		return false, fmt.Sprintf("%d stuck IN_FLIGHT mutating request(s)", n)
	}
	return true, "no stuck IN_FLIGHT mutating request"
}

func noDangerousQueue(ctx context.Context, q querier, exchangeID int64) (bool, string) {
	var n int
	q.QueryRowContext(ctx, `
SELECT COUNT(*) FROM exchange_requests er JOIN cycles c ON c.id=er.cycle_id
WHERE er.exchange_id=? AND er.status='DEAD' AND er.request_type IN ('PLACE_ORDER','CANCEL_ORDER') AND c.dry_run=0`, exchangeID).Scan(&n)
	if n > 0 {
		return false, fmt.Sprintf("%d DEAD mutating request(s) on real cycles", n)
	}
	return true, "no DEAD mutating requests on real cycles"
}

func passFail(ok bool) Status {
	if ok {
		return StatusPass
	}
	return StatusFail
}

func (c *Checker) checkDryRun(ctx context.Context, marketID int64, ctrl controls, add func(string, Status, string)) {
	ok, detail := recentDryRun(ctx, c.db, c.clk, marketID, ctrl)
	add("recent_dry_run_success", passFail(ok), detail)
}

// recentDryRun reports whether a successful dry-run (CLOSED) for the exchange/symbol exists
// within the configured window. Shared by the preflight check + RecentDryRunOK.
func recentDryRun(ctx context.Context, q querier, clk clock.Clock, marketID int64, ctrl controls) (bool, string) {
	var newest sql.NullTime
	q.QueryRowContext(ctx,
		"SELECT MAX(closed_at) FROM cycles WHERE dry_run=1 AND state='CLOSED' AND exchange_market_id=?", marketID).Scan(&newest)
	if !newest.Valid {
		return false, "no successful dry-run (CLOSED) for this exchange/symbol"
	}
	maxAge := nullIntOr(ctrl.dryRunSuccessMaxMin, defDryRunMin)
	age := clk.Now().UTC().Sub(newest.Time)
	if age.Minutes() > float64(maxAge) {
		return false, fmt.Sprintf("last dry-run success %.0fm old (max %dm)", age.Minutes(), maxAge)
	}
	return true, fmt.Sprintf("dry-run succeeded %.0fm ago", age.Minutes())
}

// RecentDryRunOK reports whether a recent successful dry-run exists for the exchange/symbol
// (used by session start to enforce "a recent successful dry-run precedes the first live buy").
func RecentDryRunOK(ctx context.Context, db *sql.DB, clk clock.Clock, marketID int64) (bool, string) {
	if clk == nil {
		clk = clock.NewSystem()
	}
	ctrl, err := loadControls(ctx, db)
	if err != nil {
		return false, "failed to load controls"
	}
	return recentDryRun(ctx, db, clk, marketID, ctrl)
}

func (c *Checker) checkAuthPath(ctx context.Context, add func(string, Status, string)) {
	var n int
	c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dashboard_tokens WHERE enabled=1").Scan(&n)
	if n > 0 {
		add("auth_path_working", StatusPass, fmt.Sprintf("%d enabled dashboard token(s)", n))
	} else {
		add("auth_path_working", StatusFail, "no enabled dashboard tokens (operator auth unavailable)")
	}
}

func (c *Checker) checkAuditPath(ctx context.Context, add func(string, Status, string)) {
	// Confirm the live_audit table is present + readable (the audit path the guard writes to).
	var x int
	if err := c.db.QueryRowContext(ctx, "SELECT 1 FROM live_audit LIMIT 1").Scan(&x); err != nil && !errors.Is(err, sql.ErrNoRows) {
		add("audit_path_writable", StatusFail, "live_audit not available")
		return
	}
	add("audit_path_writable", StatusPass, "live_audit available")
}

// ---- config hash ----

// ConfigHash hashes the config-relevant inputs that an acknowledgement binds to. A change
// to any of them (caps, live flags, canary scope, credential identity, freshness windows,
// mode, ack requirement) changes the hash and invalidates a prior ack. Ephemeral values
// (market freshness, balances, the kill switch) are excluded.
func ConfigHash(ctx context.Context, db *sql.DB, exchangeID, marketID int64, mode string) (string, error) {
	ctrl, err := loadControls(ctx, db)
	if err != nil {
		return "", err
	}
	var code, symbol string
	var exLive, mkLive int
	_ = db.QueryRowContext(ctx, "SELECT code, live_enabled FROM exchanges WHERE id=?", exchangeID).Scan(&code, &exLive)
	_ = db.QueryRowContext(ctx, "SELECT canonical_symbol, live_enabled FROM exchange_markets WHERE id=?", marketID).Scan(&symbol, &mkLive)
	var credID, credKV int64
	var credStatus string
	_ = db.QueryRowContext(ctx,
		"SELECT id, key_version, status FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active' ORDER BY key_version DESC, id DESC LIMIT 1", exchangeID).
		Scan(&credID, &credKV, &credStatus)

	parts := []string{
		"v2", mode, code, symbol,
		fmt.Sprintf("exlive=%d", exLive), fmt.Sprintf("mklive=%d", mkLive),
		"caps=" + strings.Join([]string{
			nullIntStr(ctrl.maxOpenCycles),
			nullStr(ctrl.maxOrderNotional), nullStr(ctrl.maxBaseQty), nullIntStr(ctrl.maxConsecFailures), nullIntStr(ctrl.maxUnresolvedRecon),
		}, ","),
		fmt.Sprintf("ack=%t", ctrl.requireCanaryAck),
		fmt.Sprintf("canary=%s/%s", nullIntStr(ctrl.canaryExchangeID), nullIntStr(ctrl.canaryMarketID)),
		"fresh=" + strings.Join([]string{
			nullIntStr(ctrl.credValidationMaxMin), nullIntStr(ctrl.marketDataMaxSec), nullIntStr(ctrl.balanceMaxMin), nullIntStr(ctrl.dryRunSuccessMaxMin),
		}, ","),
		fmt.Sprintf("healthreq=%t", ctrl.healthRequired),
		fmt.Sprintf("cred=%d/%d/%s", credID, credKV, credStatus),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:]), nil
}

// ---- acknowledgement ----

// AckInput is an operator's explicit live acknowledgement.
type AckInput struct {
	ExchangeID int64
	MarketID   int64
	Operator   string
	Reason     string
}

// Acknowledge re-runs preflight and, only if it is READY, records an acknowledgement bound
// to the current ConfigHash (deactivating any prior ack for the same exchange/market). It
// returns the new ack id + the report. It never mutates trading state.
func (c *Checker) Acknowledge(ctx context.Context, in AckInput) (int64, Report, error) {
	if strings.TrimSpace(in.Operator) == "" {
		return 0, Report{}, fmt.Errorf("acknowledgement requires an operator")
	}
	if strings.TrimSpace(in.Reason) == "" {
		return 0, Report{}, fmt.Errorf("acknowledgement requires a reason")
	}
	report, err := c.Run(ctx, in.ExchangeID, in.MarketID)
	if err != nil {
		return 0, Report{}, err
	}
	if !report.Ready {
		return 0, report, ErrNotReady
	}
	var credID sql.NullInt64
	_ = c.db.QueryRowContext(ctx,
		"SELECT id FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active' ORDER BY key_version DESC, id DESC LIMIT 1", report.ExchangeID).
		Scan(&credID)
	capsJSON, _ := json.Marshal(map[string]any{"config_hash": report.ConfigHash})

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, report, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		"UPDATE live_acknowledgements SET active=0, revoked_at=NOW(6) WHERE exchange_id=? AND exchange_market_id=? AND active=1",
		report.ExchangeID, report.MarketID); err != nil {
		return 0, report, err
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO live_acknowledgements (operator, exchange_id, exchange_market_id, canonical_symbol, credential_id, preflight_hash, caps_json, reason)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		in.Operator, report.ExchangeID, report.MarketID, report.Symbol, nullableInt(credID), report.ConfigHash, capsJSON, in.Reason)
	if err != nil {
		return 0, report, err
	}
	id, _ := res.LastInsertId()
	if err := tx.Commit(); err != nil {
		return 0, report, err
	}
	return id, report, nil
}

// AckInfo is the active acknowledgement the guard checks (hash + age + id).
type AckInfo struct {
	ID             int64
	Hash           string
	AcknowledgedAt time.Time
}

// ActiveAck returns the active acknowledgement for the exchange/market (used by the guard
// to verify the ack still matches config AND has not expired).
func ActiveAck(ctx context.Context, q querier, exchangeID, marketID int64) (AckInfo, bool) {
	var a AckInfo
	err := q.QueryRowContext(ctx,
		"SELECT id, preflight_hash, acknowledged_at FROM live_acknowledgements WHERE exchange_id=? AND exchange_market_id=? AND active=1 ORDER BY id DESC LIMIT 1",
		exchangeID, marketID).Scan(&a.ID, &a.Hash, &a.AcknowledgedAt)
	if err != nil {
		return AckInfo{}, false
	}
	return a, true
}

// AckMaxAge returns the configured acknowledgement expiry window (default if unset). It is
// read from live_controls so the guard and dashboard agree on "expired".
func AckMaxAge(ctx context.Context, q querier) time.Duration {
	ctrl, err := loadControls(ctx, q)
	if err != nil {
		return defCanaryAckMin * time.Minute
	}
	return time.Duration(nullIntOr(ctrl.canaryAckMaxMin, defCanaryAckMin)) * time.Minute
}

// DynamicRecheck re-evaluates ONLY the time-sensitive safety conditions (the ones a
// config-only hash cannot catch) for a live buy: credential validation freshness, market
// data freshness, balance freshness, unresolved-reconcile cap, no stuck IN_FLIGHT mutating
// request, no dangerous (DEAD) queue state. It returns ok + the first failing reason. It is
// read-only and contacts no exchange.
func DynamicRecheck(ctx context.Context, db *sql.DB, clk clock.Clock, exchangeID, marketID int64) (bool, string) {
	if clk == nil {
		clk = clock.NewSystem()
	}
	ctrl, err := loadControls(ctx, db)
	if err != nil {
		return false, "failed to load live controls"
	}
	var symbol string
	_ = db.QueryRowContext(ctx, "SELECT canonical_symbol FROM exchange_markets WHERE id=?", marketID).Scan(&symbol)

	if ok, d := credValidationFresh(ctx, db, clk, exchangeID, ctrl); !ok {
		return false, "credential: " + d
	}
	if ok, d, _ := marketFresh(ctx, db, clk, symbol, ctrl); !ok {
		return false, "market data: " + d
	}
	if ok, d := balanceFresh(ctx, db, clk, exchangeID, ctrl); !ok {
		return false, "balance: " + d
	}
	if ok, d := reconcileWithinCap(ctx, db, ctrl); !ok {
		return false, "reconcile: " + d
	}
	if ok, d := noStuckInflight(ctx, db, exchangeID); !ok {
		return false, "queue: " + d
	}
	if ok, d := noDangerousQueue(ctx, db, exchangeID); !ok {
		return false, "queue: " + d
	}
	return true, "dynamic safety conditions ok"
}

// ---- small helpers ----

func positive(s string) bool {
	// crude positive-decimal check without importing decimal (raw string compare avoids 0/empty)
	s = strings.TrimSpace(s)
	return s != "" && s != "0" && !strings.HasPrefix(s, "-") && s != "0.0" && !isAllZero(s)
}

func isAllZero(s string) bool {
	for _, r := range s {
		if r != '0' && r != '.' {
			return false
		}
	}
	return true
}

func nullIntOr(v sql.NullInt64, def int64) int64 {
	if v.Valid {
		return v.Int64
	}
	return def
}

func nullIntStr(v sql.NullInt64) string {
	if v.Valid {
		return fmt.Sprintf("%d", v.Int64)
	}
	return "-"
}

func nullStr(v sql.NullString) string {
	if v.Valid {
		return v.String
	}
	return "-"
}

func nullableInt(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
}
