package dashboard

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"v3TradeBot/internal/preflight"
)

// Live preflight + canary acknowledgement handlers (PR23). Preflight is read-only and
// never places/cancels. Acknowledging activates canary live and requires admin; it records
// the operator + the preflight config hash so a later config change makes the ack stale.

// livePreflight runs the read-only readiness checklist for the target exchange/market
// (defaults to the configured canary scope) and returns the full report. Mutates nothing.
func (s *Server) livePreflight(w http.ResponseWriter, r *http.Request) {
	if s.preflight == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "preflight unavailable"})
		return
	}
	exID, _ := strconv.ParseInt(r.URL.Query().Get("exchange_id"), 10, 64)
	mkID, _ := strconv.ParseInt(r.URL.Query().Get("market_id"), 10, 64)
	report, err := s.preflight.Run(r.Context(), exID, mkID)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// liveAcknowledgements lists the recorded acknowledgements (read-only; no secrets).
func (s *Server) liveAcknowledgements(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, `
SELECT la.id, la.operator, e.code AS exchange_code, la.canonical_symbol, la.credential_id,
  la.preflight_hash, la.reason, la.active, la.acknowledged_at, la.revoked_at
FROM live_acknowledgements la JOIN exchanges e ON e.id=la.exchange_id
ORDER BY la.id DESC LIMIT ?`, s.limit(r))
}

// liveAcknowledge records an explicit canary-live acknowledgement (admin only). It re-runs
// preflight and refuses if not READY, binding the ack to the current config hash.
func (s *Server) liveAcknowledge(w http.ResponseWriter, r *http.Request, op operator) {
	if s.preflight == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "preflight unavailable"})
		return
	}
	var body struct {
		ExchangeID int64  `json:"exchange_id"`
		MarketID   int64  `json:"market_id"`
		Reason     string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	id, report, err := s.preflight.Acknowledge(r.Context(), preflight.AckInput{
		ExchangeID: body.ExchangeID, MarketID: body.MarketID, Operator: op.name, Reason: body.Reason,
	})
	if errors.Is(err, preflight.ErrNotReady) {
		// Preflight failed -> 409 with the failing checks so the operator can fix them.
		writeJSON(w, http.StatusConflict, map[string]any{"error": "preflight not ready", "report": report})
		return
	}
	if err != nil {
		if isAckValidation(err) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		s.log.Warn("live acknowledge failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "acknowledge failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "acknowledgement_id": id, "config_hash": report.ConfigHash, "report": report})
}

// isAckValidation flags the operator/reason input errors from Acknowledge (-> 400).
func isAckValidation(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "requires an operator") || strings.Contains(err.Error(), "requires a reason"))
}
