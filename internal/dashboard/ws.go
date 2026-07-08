package dashboard

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// wsMaxClientFrameBytes caps the size of an inbound WebSocket frame. The dashboard socket
// is snapshot-only and accepts no commands, so no legitimate client frame is large; this
// bounds memory from a hostile/oversized frame.
const wsMaxClientFrameBytes = 4 << 10 // 4 KiB

var upgrader = websocket.Upgrader{
	// PR16 is read-only, but the live snapshot still exposes operational data (open
	// cycles, balances, health, regime), so the WebSocket must not be connectable from a
	// foreign website opened in the operator's browser. Enforce same-origin. A configurable
	// allowlist + full dashboard auth are PR17 scope.
	CheckOrigin: sameOriginOnly,
}

// sameOriginOnly permits a WebSocket upgrade ONLY from the same origin as the dashboard,
// so a cross-site page in the operator's browser cannot read the live snapshot.
//
//   - Origin present  → its host must equal the request Host (case-insensitive), else reject.
//   - Origin absent   → ALLOWED. Browsers always send Origin on a WebSocket handshake, so a
//     missing Origin means a non-browser client (curl, a health probe, native tooling),
//     which is not the cross-site threat this guards against. (A malformed Origin → reject.)
func sameOriginOnly(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // non-browser client (no Origin header) — not a cross-site risk
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false // malformed Origin
	}
	return strings.EqualFold(u.Host, r.Host)
}

// ws upgrades to a WebSocket and pushes a SAFE periodic snapshot (open cycles, health,
// regime, balances) for live updates. It is read-only: it only SELECTs and sends; it
// never receives commands that affect trading. The loop ends when the client disconnects
// or the request context is cancelled (server shutdown).
func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the error (e.g. 403 for a foreign origin)
	}
	defer conn.Close()

	// The socket accepts NO commands, so no client frame ever needs to be large. Cap the
	// inbound frame size so a hostile/huge frame can't force large allocations; exceeding
	// it makes ReadMessage error, which just closes this read-only connection.
	conn.SetReadLimit(wsMaxClientFrameBytes)

	ctx := r.Context()
	// Drain client messages so a close is detected promptly; ignore their content
	// (the dashboard takes no commands from the socket).
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// send builds a fresh snapshot and writes it. A DB error is NEVER turned into a
	// healthy-looking snapshot with empty sections — it becomes a generic snapshot_error
	// event (no raw DB details leak to the browser); the connection stays open so a
	// transient failure recovers on the next tick. The returned error is only the socket
	// WRITE error, which ends the loop.
	send := func() error {
		snap, serr := s.snapshot(ctx)
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if serr != nil {
			s.log.Warn("dashboard ws snapshot query failed", "err", serr)
			return conn.WriteJSON(map[string]any{"type": "snapshot_error", "error": "dashboard snapshot unavailable"})
		}
		return conn.WriteJSON(snap)
	}
	if err := send(); err != nil { // initial push
		return
	}
	t := time.NewTicker(s.cfg.WSInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := send(); err != nil {
				return
			}
		}
	}
}

// snapshot is the safe live-update payload. EVERY query is error-checked: if any fails,
// snapshot returns an error so the caller sends a snapshot_error event instead of a
// normal snapshot with silently-missing sections (an operator must never read an empty
// "open cycles" as fact when the query actually failed). `stale` is normalized to a
// bool, matching the HTTP /api/balances representation.
func (s *Server) snapshot(ctx context.Context) (map[string]any, error) {
	openCycles, err := s.rows(ctx, "SELECT * FROM cycles WHERE state NOT IN ('CLOSED','CANCELLED','FAILED') ORDER BY id DESC LIMIT 100")
	if err != nil {
		return nil, err
	}
	health, err := s.rows(ctx, "SELECT h.*, e.code AS exchange_code FROM exchange_health_current h JOIN exchanges e ON e.id=h.exchange_id ORDER BY h.exchange_id")
	if err != nil {
		return nil, err
	}
	regime, err := s.rows(ctx, "SELECT c.*, b.name AS basket_name FROM market_regime_current c JOIN market_regime_baskets b ON b.id=c.basket_id ORDER BY c.basket_id")
	if err != nil {
		return nil, err
	}
	balances, err := s.rows(ctx, `SELECT b.*, e.code AS exchange_code,
		(b.last_seen_at IS NOT NULL AND b.last_seen_at < NOW(6) - INTERVAL ? SECOND) AS stale
		FROM wallet_balances_current b JOIN exchanges e ON e.id=b.exchange_id ORDER BY b.exchange_id, b.asset`,
		int(s.cfg.StaleBalanceAge.Seconds()))
	if err != nil {
		return nil, err
	}
	for _, m := range balances {
		m["stale"] = truthy(m["stale"]) // clean boolean, consistent with HTTP /api/balances
	}
	return map[string]any{
		"type":        "snapshot",
		"open_cycles": openCycles,
		"health":      health,
		"regime":      regime,
		"balances":    balances,
	}, nil
}
