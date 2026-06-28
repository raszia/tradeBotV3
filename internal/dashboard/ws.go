package dashboard

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// originAllowed enforces the configured WebSocket origin allowlist. An empty allowlist
// is permissive (safe only for local read-only use); the WS carries no commands either
// way. A missing Origin header (non-browser client) is allowed.
func (s *Server) originAllowed(r *http.Request) bool {
	if len(s.cfg.AllowedWSOrigins) == 0 {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	for _, o := range s.cfg.AllowedWSOrigins {
		if o == origin {
			return true
		}
	}
	return false
}

// ws upgrades to a WebSocket and pushes a SAFE periodic snapshot (open cycles, health,
// regime, balances) for live updates. It is read-only: it only SELECTs and sends; it
// never receives commands that affect trading. The loop ends when the client
// disconnects or the request context is cancelled (server shutdown).
func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: s.originAllowed}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the error
	}
	defer conn.Close()

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

	send := func() error {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return conn.WriteJSON(s.snapshot(ctx))
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

// snapshot is the safe live-update payload.
func (s *Server) snapshot(ctx context.Context) map[string]any {
	openCycles, _ := s.rows(ctx, "SELECT * FROM cycles WHERE state NOT IN ('CLOSED','CANCELLED','FAILED') ORDER BY id DESC LIMIT 100")
	health, _ := s.rows(ctx, "SELECT h.*, e.code AS exchange_code FROM exchange_health_current h JOIN exchanges e ON e.id=h.exchange_id ORDER BY h.exchange_id")
	regime, _ := s.rows(ctx, "SELECT c.*, b.name AS basket_name FROM market_regime_current c JOIN market_regime_baskets b ON b.id=c.basket_id ORDER BY c.basket_id")
	balances, _ := s.rows(ctx, `SELECT b.*, e.code AS exchange_code,
		(b.last_seen_at IS NOT NULL AND b.last_seen_at < NOW(6) - INTERVAL ? SECOND) AS stale
		FROM wallet_balances_current b JOIN exchanges e ON e.id=b.exchange_id ORDER BY b.exchange_id, b.asset`,
		int(s.cfg.StaleBalanceAge.Seconds()))
	return map[string]any{
		"type":        "snapshot",
		"open_cycles": openCycles,
		"health":      health,
		"regime":      regime,
		"balances":    balances,
	}
}
