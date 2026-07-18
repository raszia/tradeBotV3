// Package dashboard is the operator dashboard. It runs as its own binary and holds only a
// *sql.DB + the versioned config store (no exchange client, no queue — it cannot trade by
// construction: it never places/cancels orders and never mutates cycles/orders/queue/locks).
//
//   - PR16 READ views stay strictly read-only: the GET endpoints and the WebSocket only
//     SELECT and display; any mutating method on a read route is 405.
//   - PR17 adds AUTHENTICATED, AUTHORIZED, VERSIONED, VALIDATED, AUDITED config-editing
//     routes (POST /api/config/*), plus login/session auth on every route. Config is the
//     ONLY thing these mutate, and only through the config-store versioning path.
//
// Access: only /healthz and POST /login are unauthenticated; everything else (UI, /api/*
// reads, config-editing mutations, /ws) requires a valid login session, and editing needs
// the config_operator/admin role.
//
// DB errors are never swallowed into a partial 200: a failed sub-query in cycle-detail or
// /api/config returns 500 (an operator must never misread a failed query as "no data").
//
// Secrets are never shown: it never reads the credentials table, and BOTH api-call-log and
// app-log views are masked (defence-in-depth on already-masked storage). The WebSocket is
// snapshot-only (same-origin) — it pushes periodic read-only snapshots and takes no commands
// from the socket (incoming messages are drained and ignored).
package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/credentials"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/opreconcile"
)

// Config tunes the dashboard.
type Config struct {
	DefaultLimit    int           // default row limit for list endpoints (default 50)
	MaxLimit        int           // hard cap on ?limit= (default 500)
	StaleBalanceAge time.Duration // a balance whose last_seen_at is older than this is flagged stale (default 5m)
	WSInterval      time.Duration // live-update push cadence (default 2s)
	SessionTTL      time.Duration // login session lifetime (default 12h)
	SecureCookies   bool          // set the Secure flag on the session cookie (enable behind HTTPS)
	// MasterKey is the bootstrap AES master key (PR22). When present + valid it enables credential
	// provisioning (create/validate/activate/disable). Empty/invalid → credential WRITE endpoints
	// are disabled (no plaintext fallback). Never logged.
	MasterKey string
}

func (c *Config) withDefaults() {
	if c.DefaultLimit <= 0 {
		c.DefaultLimit = 50
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = 500
	}
	if c.StaleBalanceAge <= 0 {
		c.StaleBalanceAge = 5 * time.Minute
	}
	if c.WSInterval <= 0 {
		c.WSInterval = 2 * time.Second
	}
	if c.SessionTTL <= 0 {
		c.SessionTTL = 12 * time.Hour
	}
}

// Server is the dashboard. It holds ONLY a database handle + the versioned config store
// (+ config/log): no exchange client and no queue, so it cannot trade by construction.
// PR16 read endpoints stay read-only; PR17 adds login/session auth + versioned+audited
// config editing (never any order/cycle/queue/lock mutation).
type Server struct {
	db       *sql.DB
	cfgStore *configstore.Store
	cfg      Config
	log      *slog.Logger
	// reconciler resolves NEEDS_RECONCILE cases (PR21). It holds only the DB handle, so like the
	// rest of the dashboard it cannot place/cancel orders — it only applies audited state changes.
	reconciler *opreconcile.Resolver
	previews   *previewStore // short-lived preview tokens binding apply to a prior preview
	// Credential provisioning (PR22). Non-nil only when a valid master key is configured; the
	// credBuilder builds a READ-ONLY client for a specific credential during validation. When nil,
	// credential WRITE endpoints are disabled (no plaintext fallback).
	provisioner *credentials.Provisioner
	credBuilder *credentials.Builder
}

// New builds a Server.
func New(db *sql.DB, log *slog.Logger, cfg Config) *Server {
	cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	s := &Server{db: db, cfg: cfg, log: log, previews: newPreviewStore()}
	if db != nil {
		s.cfgStore = configstore.New(db)
		s.reconciler = opreconcile.New(db, clock.NewSystem(), log)
		// Credential provisioning is enabled ONLY with a valid master key (PR22). Without it,
		// credential writes are disabled and secrets can never be decrypted — no plaintext fallback.
		if cfg.MasterKey != "" {
			if prov, err := credentials.NewProvisioner(db, cfg.MasterKey, clock.NewSystem(), log); err == nil {
				if provider, perr := credentials.NewProvider(db, cfg.MasterKey, clock.NewSystem(), log); perr == nil {
					iolog := exchanges.NewIOLogger(exchanges.IOLogConfig{Enabled: true, Source: "dashboard"}, db)
					s.provisioner = prov
					s.credBuilder = credentials.NewBuilder(db, provider, iolog)
				} else {
					log.Warn("dashboard: credential provisioning disabled (provider init failed)", "err", perr)
				}
			} else {
				log.Warn("dashboard: credential provisioning disabled (invalid master key)", "err", err)
			}
		} else {
			log.Info("dashboard: credential provisioning disabled (no master key configured)")
		}
	}
	return s
}

// Handler returns the route mux. Only /healthz and POST /login are unauthenticated;
// EVERY other route (the UI, all /api/* reads, the config-editing mutations, and /ws)
// requires a valid logged-in session (401 otherwise). Read routes are GET-only, so a
// mutating method on them is 405; mutation is confined to the explicit POST /api/config/*
// editing routes, which additionally require the config_operator or admin role (403
// otherwise). No route places/cancels orders or mutates cycles/orders/queue/locks.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Open (no session): liveness + login.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /login", s.login)
	mux.HandleFunc("POST /logout", s.requireSession(s.logout))

	// Authenticated read views (any logged-in role).
	mux.HandleFunc("GET /", s.requireSession(s.index))
	mux.HandleFunc("GET /api/cycles/open", s.requireSession(s.openCycles))
	mux.HandleFunc("GET /api/cycles/closed", s.requireSession(s.closedCycles))
	mux.HandleFunc("GET /api/cycles/{id}", s.requireSession(s.cycleDetail))
	mux.HandleFunc("GET /api/orders", s.requireSession(s.orders))
	mux.HandleFunc("GET /api/fills", s.requireSession(s.fills))
	mux.HandleFunc("GET /api/requests", s.requireSession(s.requests))
	mux.HandleFunc("GET /api/signals", s.requireSession(s.signals))
	mux.HandleFunc("GET /api/comparisons", s.requireSession(s.comparisons))
	mux.HandleFunc("GET /api/balances", s.requireSession(s.balances))
	mux.HandleFunc("GET /api/health", s.requireSession(s.health))
	mux.HandleFunc("GET /api/regime", s.requireSession(s.regime))
	mux.HandleFunc("GET /api/logs", s.requireSession(s.appLogs))
	mux.HandleFunc("GET /api/api-logs", s.requireSession(s.apiLogs))
	mux.HandleFunc("GET /api/config", s.requireSession(s.config))
	mux.HandleFunc("GET /api/audit", s.requireSession(s.audit))
	mux.HandleFunc("GET /api/me", s.requireSession(s.me))
	mux.HandleFunc("GET /ws", s.requireSession(s.ws))

	// Config EDITING (PR17): config_operator or admin. Versioned + audited + validated +
	// optimistic-concurrency (expected_config_version). No order/cycle/queue/lock mutation.
	mux.HandleFunc("POST /api/config/symbol/{id}", s.requireRole(RoleConfigOperator, s.editSymbolConfig))
	mux.HandleFunc("POST /api/config/market/{id}/flags", s.requireRole(RoleConfigOperator, s.editMarketFlags))
	mux.HandleFunc("POST /api/config/exchange/{id}", s.requireRole(RoleConfigOperator, s.editExchangeConfig))
	mux.HandleFunc("POST /api/config/fee", s.requireRole(RoleConfigOperator, s.editFee))

	// Operator RECONCILIATION (PR21): resolve NEEDS_RECONCILE cases. EXACT-set capability
	// (reconcile_operator or admin) on EVERY endpoint including read-only ones — details are never
	// returned to an unauthenticated (401) or unauthorized (403) user. apply requires a matching
	// prior preview (token). No exchange I/O; every change is audited.
	mux.HandleFunc("GET /api/reconcile", s.requireReconcileCapable(s.reconcileList))
	mux.HandleFunc("GET /api/reconcile/audit", s.requireReconcileCapable(s.reconcileAudit))
	mux.HandleFunc("GET /api/reconcile/{id}", s.requireReconcileCapable(s.reconcileDetail))
	mux.HandleFunc("POST /api/reconcile/{id}/preview", s.requireReconcileCapable(s.reconcilePreview))
	mux.HandleFunc("POST /api/reconcile/{id}/apply", s.requireReconcileCapable(s.reconcileApply))

	// Exchange CREDENTIAL provisioning (PR22): create → validate → activate/rotate → disable. Every
	// endpoint requires the exact-set credential capability (credential_operator or admin); metadata
	// is never served to an unauthorized user, and no endpoint returns plaintext or an encrypted
	// blob. Writes are disabled (503) without a valid master key.
	mux.HandleFunc("GET /api/credentials", s.requireCredentialCapable(s.credentialList))
	mux.HandleFunc("GET /api/credentials/audit", s.requireCredentialCapable(s.credentialAudit))
	mux.HandleFunc("POST /api/credentials", s.requireCredentialCapable(s.credentialCreate))
	mux.HandleFunc("POST /api/credentials/{id}/validate", s.requireCredentialCapable(s.credentialValidate))
	mux.HandleFunc("POST /api/credentials/{id}/activate-or-rotate", s.requireCredentialCapable(s.credentialActivateOrRotate))
	mux.HandleFunc("POST /api/credentials/{id}/disable", s.requireCredentialCapable(s.credentialDisable))
	return mux
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(`<!doctype html><meta charset=utf-8><title>v3TradeBot dashboard</title>
<h1>v3TradeBot — operator dashboard</h1>
<p>Read-only views under <code>/api/…</code> + live updates on <code>/ws</code>; authenticated, audited config editing under <code>/api/config/…</code> (PR17). Log in at <code>/login</code>.</p>
<ul>
<li><a href="/api/cycles/open">/api/cycles/open</a></li>
<li><a href="/api/balances">/api/balances</a></li>
<li><a href="/api/health">/api/health</a></li>
<li><a href="/api/regime">/api/regime</a></li>
</ul>`))
}

// ---- list endpoints (read-only SELECTs) ----

func (s *Server) openCycles(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT * FROM cycles WHERE state NOT IN ('CLOSED','CANCELLED','FAILED') ORDER BY id DESC LIMIT ?", s.limit(r))
}
func (s *Server) closedCycles(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT * FROM cycles WHERE state IN ('CLOSED','CANCELLED','FAILED') ORDER BY id DESC LIMIT ?", s.limit(r))
}
func (s *Server) orders(w http.ResponseWriter, r *http.Request) {
	if c := r.URL.Query().Get("cycle_id"); c != "" {
		s.list(w, r, "SELECT * FROM orders WHERE cycle_id=? ORDER BY id", c)
		return
	}
	s.list(w, r, "SELECT * FROM orders ORDER BY id DESC LIMIT ?", s.limit(r))
}
func (s *Server) fills(w http.ResponseWriter, r *http.Request) {
	if c := r.URL.Query().Get("cycle_id"); c != "" {
		s.list(w, r, "SELECT * FROM fills WHERE cycle_id=? ORDER BY id", c)
		return
	}
	s.list(w, r, "SELECT * FROM fills ORDER BY id DESC LIMIT ?", s.limit(r))
}
func (s *Server) signals(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT * FROM signals ORDER BY id DESC LIMIT ?", s.limit(r))
}
func (s *Server) comparisons(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT * FROM comparison_events ORDER BY id DESC LIMIT ?", s.limit(r))
}
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT h.*, e.code AS exchange_code FROM exchange_health_current h JOIN exchanges e ON e.id=h.exchange_id ORDER BY h.exchange_id")
}
func (s *Server) regime(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT c.*, b.name AS basket_name FROM market_regime_current c JOIN market_regime_baskets b ON b.id=c.basket_id ORDER BY c.basket_id")
}

// appLogs masks app-log rows before display (defence in depth): secrets should never be
// written to app_logs, but the dashboard must not rely on that assumption.
func (s *Server) appLogs(w http.ResponseWriter, r *http.Request) {
	data, err := s.rows(r.Context(), "SELECT * FROM app_logs ORDER BY id DESC LIMIT ?", s.limit(r))
	if err != nil {
		s.fail(w, err)
		return
	}
	maskAppLogs(data)
	writeJSON(w, http.StatusOK, data)
}

// maskAppLogs redacts secrets from app-log rows (the message + JSON fields column, and any
// other string value) so an api_key/secret/token/authorization/password/etc. can never
// reach the browser even if one was written to app_logs upstream. maskSecrets only rewrites
// key:value pairs for known-sensitive keys, so non-secret fields (level, source_binary…)
// pass through unchanged.
func maskAppLogs(rows []map[string]any) {
	for _, m := range rows {
		for k, v := range m {
			if str, ok := v.(string); ok {
				m[k] = maskSecrets(str)
			}
		}
	}
}

// requests adds a step_kind field disambiguating a scheduled next step from a real
// retry (RETRY_SCHEDULED with retry_count==0 is a planned simulated-IOC/reprice step;
// retry_count>0 is an actual retry).
func (s *Server) requests(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), "SELECT * FROM exchange_requests ORDER BY id DESC LIMIT ?", s.limit(r))
	if err != nil {
		s.fail(w, err)
		return
	}
	defer rows.Close()
	data, err := jsonRows(rows)
	if err != nil {
		s.fail(w, err)
		return
	}
	for _, m := range data {
		m["step_kind"] = stepKind(asString(m["status"]), asInt(m["retry_count"]))
	}
	writeJSON(w, http.StatusOK, data)
}

// balances flags a stale row (last_seen_at older than the configured age) without ever
// zeroing it — a missing asset keeps its last value and is simply marked stale.
func (s *Server) balances(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `
SELECT b.*, e.code AS exchange_code,
  (b.last_seen_at IS NOT NULL AND b.last_seen_at < NOW(6) - INTERVAL ? SECOND) AS stale
FROM wallet_balances_current b JOIN exchanges e ON e.id=b.exchange_id
ORDER BY b.exchange_id, b.asset`, int(s.cfg.StaleBalanceAge.Seconds()))
	if err != nil {
		s.fail(w, err)
		return
	}
	defer rows.Close()
	data, err := jsonRows(rows)
	if err != nil {
		s.fail(w, err)
		return
	}
	for _, m := range data {
		m["stale"] = truthy(m["stale"]) // expose a clean boolean, not the driver's "1"/"0"
	}
	writeJSON(w, http.StatusOK, data)
}

// apiLogs masks the (already-masked-at-storage) headers/bodies again as defence in
// depth, so no secret can ever reach the browser.
func (s *Server) apiLogs(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(),
		"SELECT id, exchange_id, cycle_id, order_id, request_id, method, url, request_headers, request_body, response_status, response_headers, response_body, latency_ms, error, timeout, created_at FROM api_call_logs ORDER BY id DESC LIMIT ?",
		s.limit(r))
	if err != nil {
		s.fail(w, err)
		return
	}
	defer rows.Close()
	data, err := jsonRows(rows)
	if err != nil {
		s.fail(w, err)
		return
	}
	for _, m := range data {
		for _, k := range []string{"request_headers", "request_body", "response_headers", "response_body", "url"} {
			if v, ok := m[k].(string); ok {
				m[k] = maskSecrets(v)
			}
		}
	}
	writeJSON(w, http.StatusOK, data)
}

// ---- cycle detail ----

func (s *Server) cycleDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cyc, err := s.oneRow(r.Context(), "SELECT * FROM cycles WHERE id=?", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if cyc == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "cycle not found"})
		return
	}
	// Every related query is error-checked: a failed sub-query must NOT be silently
	// swallowed into a 200 with partial data (an operator could misread "no orders" or
	// "no logs" when the query actually failed). Any error → 500, never partial 200.
	orders, err := s.rows(r.Context(), "SELECT * FROM orders WHERE cycle_id=? ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	fills, err := s.rows(r.Context(), "SELECT * FROM fills WHERE cycle_id=? ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	reqs, err := s.rows(r.Context(), "SELECT * FROM exchange_requests WHERE cycle_id=? ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	for _, m := range reqs {
		m["step_kind"] = stepKind(asString(m["status"]), asInt(m["retry_count"]))
	}
	cycEvents, err := s.rows(r.Context(), "SELECT * FROM cycle_state_events WHERE cycle_id=? ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	ordEvents, err := s.rows(r.Context(), "SELECT * FROM order_events WHERE order_id IN (SELECT id FROM orders WHERE cycle_id=?) ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	locks, err := s.rows(r.Context(), "SELECT * FROM symbol_locks WHERE cycle_id=? ORDER BY id", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	logs, err := s.rows(r.Context(), "SELECT * FROM app_logs WHERE cycle_id=? ORDER BY id DESC LIMIT 200", id)
	if err != nil {
		s.fail(w, err)
		return
	}
	maskAppLogs(logs) // defence in depth: never surface a secret from app_logs

	writeJSON(w, http.StatusOK, map[string]any{
		"cycle":        cyc,
		"orders":       orders,
		"fills":        fills,
		"requests":     reqs,
		"cycle_events": cycEvents,
		"order_events": ordEvents,
		"locks":        locks,
		"logs":         logs,
		// realized_quote nets only fees denominated in the quote currency; fees paid in
		// another asset are stored raw on the orders/cycle and are NOT folded in.
		"fee_note": "realized_quote includes quote-denominated fees only; non-quote fees are shown raw and not netted",
	})
}

// ---- config snapshot (read-only DISPLAY only; config EDITING is PR17 scope) ----

// config returns a read-only snapshot of the market/exchange/fee/regime config. Every
// query is error-checked: if ANY config table cannot be loaded the whole endpoint returns
// 500 — it never returns an incomplete config with 200 (which an operator could misread as
// "this config is empty/disabled"). It only SELECTs; it never edits config (that is PR17).
func (s *Server) config(w http.ResponseWriter, r *http.Request) {
	markets, err := s.rows(r.Context(), `
SELECT em.id AS exchange_market_id, e.code AS exchange_code, em.canonical_symbol,
  em.enabled_for_collection, em.enabled_for_signal, em.enabled_for_trading, em.enabled_for_sell_manage,
  sc.min_spread_bps, sc.buy_size, sc.buy_size_unit, sc.sell_offset_bps, sc.reprice_interval_seconds,
  sc.maker_first_enabled, sc.maker_attempts_before_taker, sc.taker_price_mode, sc.config_version
FROM exchange_markets em JOIN exchanges e ON e.id=em.exchange_id
LEFT JOIN symbol_configs sc ON sc.exchange_market_id=em.id ORDER BY em.id`)
	if err != nil {
		s.fail(w, err)
		return
	}
	exch, err := s.rows(r.Context(), "SELECT ec.*, e.code AS exchange_code FROM exchange_configs ec JOIN exchanges e ON e.id=ec.exchange_id ORDER BY ec.exchange_id")
	if err != nil {
		s.fail(w, err)
		return
	}
	fees, err := s.rows(r.Context(), "SELECT * FROM exchange_fees ORDER BY id")
	if err != nil {
		s.fail(w, err)
		return
	}
	regime, err := s.rows(r.Context(), "SELECT * FROM market_regime_baskets ORDER BY id")
	if err != nil {
		s.fail(w, err)
		return
	}
	version, err := s.oneRow(r.Context(), "SELECT id, status, created_by, note, created_at FROM config_versions WHERE status='active' ORDER BY id DESC LIMIT 1")
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active_version": version, "markets": markets, "exchanges": exch, "fees": fees, "regime_baskets": regime,
		"note": "read-only snapshot; edit via POST /api/config/* (authenticated config_operator/admin, versioned + audited)",
	})
}

// ---- helpers ----

func (s *Server) list(w http.ResponseWriter, r *http.Request, query string, args ...any) {
	rows, err := s.db.QueryContext(r.Context(), query, args...)
	if err != nil {
		s.fail(w, err)
		return
	}
	defer rows.Close()
	data, err := jsonRows(rows)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *Server) rows(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return jsonRows(rows)
}

func (s *Server) oneRow(ctx context.Context, query string, args ...any) (map[string]any, error) {
	data, err := s.rows(ctx, query, args...)
	if err != nil || len(data) == 0 {
		return nil, err
	}
	return data[0], nil
}

func (s *Server) limit(r *http.Request) int {
	n := s.cfg.DefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}
	if n > s.cfg.MaxLimit {
		n = s.cfg.MaxLimit
	}
	return n
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	s.log.Warn("dashboard query failed", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "query failed"})
}

// jsonRows turns a result set into []map[string]any (columns as JSON-safe values:
// []byte/text/decimal -> string, ints -> int64, NULL -> null). Read-only display only.
func jsonRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok {
				m[c] = string(b)
			} else {
				m[c] = vals[i]
			}
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// stepKind disambiguates RETRY_SCHEDULED: retry_count==0 is a planned next step,
// retry_count>0 is an actual retry (see PROJECT_ARCHITECTURE.md §8).
func stepKind(status string, retryCount int) string {
	if status != "RETRY_SCHEDULED" {
		return ""
	}
	if retryCount > 0 {
		return "retry"
	}
	return "scheduled_next_step"
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
func asInt(v any) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case int:
		return n
	}
	return 0
}

// truthy normalizes a SQL boolean expression result (driver may give int64 1/0 or a
// "1"/"0" byte string) to a Go bool.
func truthy(v any) bool {
	switch n := v.(type) {
	case bool:
		return n
	case int64:
		return n != 0
	case string:
		return n == "1" || n == "true"
	}
	return false
}
