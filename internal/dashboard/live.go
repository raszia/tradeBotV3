package dashboard

import (
	"net/http"
)

// live serves the LIVE-execution status (read-only, PR20): the execution mode, kill
// switch + caps, live-enabled exchanges/symbols, remaining daily allowance, credential
// status, unresolved-reconcile count, and the last live order / last live error from
// the audit. No secrets (credential rows expose only status, never key material).
func (s *Server) live(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	controls, _ := s.oneRow(ctx, "SELECT * FROM live_controls WHERE id=1")
	liveExchanges, _ := s.rows(ctx, "SELECT code, name FROM exchanges WHERE live_enabled=1 ORDER BY code")
	liveSymbols, _ := s.rows(ctx, "SELECT em.canonical_symbol, e.code AS exchange_code FROM exchange_markets em JOIN exchanges e ON e.id=em.exchange_id WHERE em.live_enabled=1 ORDER BY em.id")
	// Credential status only — NEVER any key material.
	credentials, _ := s.rows(ctx, "SELECT e.code AS exchange_code, ec.label, ec.status, ec.enabled, ec.key_version, ec.last_checked_at FROM exchange_credentials ec JOIN exchanges e ON e.id=ec.exchange_id ORDER BY ec.exchange_id")
	lastOrder, _ := s.oneRow(ctx, "SELECT * FROM live_audit WHERE decision='allow' ORDER BY id DESC LIMIT 1")
	lastError, _ := s.oneRow(ctx, "SELECT * FROM live_audit WHERE decision='deny' ORDER BY id DESC LIMIT 1")

	var unresolved, openCycles, dailyOrders int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state='NEEDS_RECONCILE'").Scan(&unresolved)
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cycles WHERE dry_run=0 AND state NOT IN ('CLOSED','CANCELLED','FAILED')").Scan(&openCycles)
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders o JOIN cycles c ON c.id=o.cycle_id WHERE c.dry_run=0 AND o.created_at >= CURDATE()").Scan(&dailyOrders)

	killSwitch := true // default-safe if no controls row
	if controls != nil {
		killSwitch = truthy(controls["kill_switch"])
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"mode":                   s.cfg.ExecutionMode,
		"live":                   s.cfg.ExecutionMode == "live",
		"kill_switch":            killSwitch,
		"controls":               controls,
		"live_enabled_exchanges": liveExchanges,
		"live_enabled_symbols":   liveSymbols,
		"credentials":            credentials,
		"unresolved_reconcile":   unresolved,
		"open_cycles":            openCycles,
		"daily_orders":           dailyOrders,
		"last_live_order":        lastOrder,
		"last_live_error":        lastError,
	})
}
