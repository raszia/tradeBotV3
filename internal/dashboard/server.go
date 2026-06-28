// Package dashboard is the READ-ONLY operator dashboard (PR16): HTTP endpoints for
// the initial load plus a WebSocket for live updates. It is strictly read-only — it
// runs as its own binary, holds only a *sql.DB (no exchange client, no queue), and
// exposes ONLY GET routes (any mutating method is 405). It never places/cancels
// orders, creates cycles/orders, mutates the queue, or edits config (config editing
// is PR17). Secrets are never shown: it never reads the credentials table, and the
// api-call-log view is masked (defence-in-depth on already-masked storage).
package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/regime"
)

// Config tunes the dashboard.
type Config struct {
	DefaultLimit    int           // default row limit for list endpoints (default 50)
	MaxLimit        int           // hard cap on ?limit= (default 500)
	StaleBalanceAge time.Duration // a balance whose last_seen_at is older than this is flagged stale (default 5m)
	WSInterval      time.Duration // live-update push cadence (default 2s)
	// AllowedWSOrigins restricts the WebSocket Origin header. Empty = permissive (only
	// safe for local read-only use); set it to an allowlist for any non-local deploy.
	// The WS carries no commands either way, so it cannot affect trading.
	AllowedWSOrigins []string
	// ExecutionMode is the system's execution mode ("off"|"dry_run"|"live"), shown on
	// the /api/live status so operators can see LIVE/DRY_RUN clearly (read-only).
	ExecutionMode string
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
}

// Server is the dashboard. It holds a database handle + the versioned config stores
// (+ config/log): NO exchange client and NO queue, so it cannot trade by construction.
// Read views are open; config-editing routes are authenticated + authorized (PR17).
type Server struct {
	db       *sql.DB
	cfgStore *configstore.Store
	regStore *regime.Store
	cfg      Config
	log      *slog.Logger
}

// New builds a Server.
func New(db *sql.DB, log *slog.Logger, cfg Config) *Server {
	cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	s := &Server{db: db, cfg: cfg, log: log}
	if db != nil {
		s.cfgStore = configstore.New(db)
		s.regStore = regime.NewStore(db)
	}
	return s
}

// Handler returns the read-only route mux. Patterns are method-scoped to GET, so any
// POST/PUT/DELETE/PATCH (e.g. an attempt to mutate config) gets 405 Method Not Allowed
// — there is no mutating route on the dashboard at all.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /", s.index)

	mux.HandleFunc("GET /api/cycles/open", s.openCycles)
	mux.HandleFunc("GET /api/cycles/closed", s.closedCycles)
	mux.HandleFunc("GET /api/cycles/{id}", s.cycleDetail)
	mux.HandleFunc("GET /api/orders", s.orders)
	mux.HandleFunc("GET /api/fills", s.fills)
	mux.HandleFunc("GET /api/requests", s.requests)
	mux.HandleFunc("GET /api/signals", s.signals)
	mux.HandleFunc("GET /api/comparisons", s.comparisons)
	mux.HandleFunc("GET /api/balances", s.balances)
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/regime", s.regime)
	mux.HandleFunc("GET /api/logs", s.appLogs)
	mux.HandleFunc("GET /api/api-logs", s.apiLogs)
	mux.HandleFunc("GET /api/config", s.config)
	mux.HandleFunc("GET /api/audit", s.audit)
	mux.HandleFunc("GET /api/live", s.live)
	mux.HandleFunc("GET /ws", s.ws)

	// Config-editing routes (PR17): authenticated + authorized (config_operator/admin).
	// There is NO trading/cycle/order/queue/credential mutation route here.
	mux.HandleFunc("POST /api/config/symbol/{id}", s.requireConfigOperator(s.editSymbolConfig))
	mux.HandleFunc("POST /api/config/market/{id}/flags", s.requireConfigOperator(s.editMarketFlags))
	mux.HandleFunc("POST /api/config/exchange/{id}", s.requireConfigOperator(s.editExchangeConfig))
	mux.HandleFunc("POST /api/config/fee", s.requireConfigOperator(s.editFee))
	mux.HandleFunc("POST /api/config/regime/basket/{id}", s.requireConfigOperator(s.editRegimeBasket))
	mux.HandleFunc("POST /api/config/regime/basket/{id}/symbol", s.requireConfigOperator(s.editRegimeSymbol))
	mux.HandleFunc("POST /api/config/regime/basket/{id}/timeframe", s.requireConfigOperator(s.editRegimeTimeframe))
	return mux
}

func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT id, config_version, entity_type, entity_id, field, old_value, new_value, changed_by, reason, activated_at, created_at FROM config_change_audit ORDER BY id DESC LIMIT ?", s.limit(r))
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(`<!doctype html><meta charset=utf-8><title>v3TradeBot dashboard</title>
<h1>v3TradeBot — read-only dashboard</h1>
<p>Read-only JSON API. Endpoints under <code>/api/…</code>; live updates on <code>/ws</code>.</p>
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
		s.list(w, r, "SELECT o.*, c.dry_run FROM orders o LEFT JOIN cycles c ON c.id=o.cycle_id WHERE o.cycle_id=? ORDER BY o.id", c)
		return
	}
	s.list(w, r, "SELECT o.*, c.dry_run FROM orders o LEFT JOIN cycles c ON c.id=o.cycle_id ORDER BY o.id DESC LIMIT ?", s.limit(r))
}
func (s *Server) fills(w http.ResponseWriter, r *http.Request) {
	if c := r.URL.Query().Get("cycle_id"); c != "" {
		s.list(w, r, "SELECT f.*, c.dry_run FROM fills f LEFT JOIN cycles c ON c.id=f.cycle_id WHERE f.cycle_id=? ORDER BY f.id", c)
		return
	}
	s.list(w, r, "SELECT f.*, c.dry_run FROM fills f LEFT JOIN cycles c ON c.id=f.cycle_id ORDER BY f.id DESC LIMIT ?", s.limit(r))
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
func (s *Server) appLogs(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, "SELECT * FROM app_logs ORDER BY id DESC LIMIT ?", s.limit(r))
}

// requests adds a step_kind field disambiguating a scheduled next step from a real
// retry (RETRY_SCHEDULED with retry_count==0 is a planned simulated-IOC/reprice step;
// retry_count>0 is an actual retry).
func (s *Server) requests(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), "SELECT er.*, c.dry_run FROM exchange_requests er LEFT JOIN cycles c ON c.id=er.cycle_id ORDER BY er.id DESC LIMIT ?", s.limit(r))
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
	orders, _ := s.rows(r.Context(), "SELECT * FROM orders WHERE cycle_id=? ORDER BY id", id)
	fills, _ := s.rows(r.Context(), "SELECT * FROM fills WHERE cycle_id=? ORDER BY id", id)
	reqs, _ := s.rows(r.Context(), "SELECT * FROM exchange_requests WHERE cycle_id=? ORDER BY id", id)
	for _, m := range reqs {
		m["step_kind"] = stepKind(asString(m["status"]), asInt(m["retry_count"]))
	}
	cycEvents, _ := s.rows(r.Context(), "SELECT * FROM cycle_state_events WHERE cycle_id=? ORDER BY id", id)
	ordEvents, _ := s.rows(r.Context(), "SELECT * FROM order_events WHERE order_id IN (SELECT id FROM orders WHERE cycle_id=?) ORDER BY id", id)
	locks, _ := s.rows(r.Context(), "SELECT * FROM symbol_locks WHERE cycle_id=? ORDER BY id", id)
	logs, _ := s.rows(r.Context(), "SELECT * FROM app_logs WHERE cycle_id=? ORDER BY id DESC LIMIT 200", id)

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

// ---- config snapshot (read-only display; no editing — that is PR17) ----

func (s *Server) config(w http.ResponseWriter, r *http.Request) {
	markets, _ := s.rows(r.Context(), `
SELECT em.id AS exchange_market_id, e.code AS exchange_code, em.canonical_symbol,
  em.enabled_for_collection, em.enabled_for_signal, em.enabled_for_trading, em.enabled_for_sell_manage,
  sc.min_spread_bps, sc.buy_size, sc.buy_size_unit, sc.sell_offset_bps, sc.reprice_interval_seconds,
  sc.maker_first_enabled, sc.maker_attempts_before_taker, sc.taker_price_mode, sc.config_version
FROM exchange_markets em JOIN exchanges e ON e.id=em.exchange_id
LEFT JOIN symbol_configs sc ON sc.exchange_market_id=em.id ORDER BY em.id`)
	exch, _ := s.rows(r.Context(), "SELECT ec.*, e.code AS exchange_code FROM exchange_configs ec JOIN exchanges e ON e.id=ec.exchange_id ORDER BY ec.exchange_id")
	fees, _ := s.rows(r.Context(), "SELECT * FROM exchange_fees ORDER BY id")
	version, _ := s.oneRow(r.Context(), "SELECT id, status, created_by, note, created_at FROM config_versions WHERE status='active' ORDER BY id DESC LIMIT 1")
	writeJSON(w, http.StatusOK, map[string]any{
		"active_version": version, "markets": markets, "exchanges": exch, "fees": fees,
		"note": "read-only display; config editing is a later PR",
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
	case float64:
		return n != 0
	case string:
		return n == "1" || n == "true"
	}
	return false
}
