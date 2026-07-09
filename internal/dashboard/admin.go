package dashboard

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"v3TradeBot/internal/configstore"
)

// PR17 config-editing handlers. Every mutation is: authenticated (session) + authorized
// (role) + validated + VERSIONED + AUDITED (configstore) + optimistic-concurrency-guarded
// (expected_config_version). changed_by is ALWAYS the authenticated session user — never a
// client-supplied value. Nothing here places/cancels orders or mutates cycles/orders/queue/
// locks/credentials; edits go only through the official configstore versioning path.

// decodeStrictJSON decodes EXACTLY ONE JSON object into dst. It rejects unknown fields and
// any trailing content (a second JSON value or garbage) — a second decode must be io.EOF.
// Body size is bounded. Returns false (and writes 400) on any violation.
func decodeStrictJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body: " + err.Error()})
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "request body must contain exactly one JSON object"})
		return false
	}
	return true
}

// pathID parses the {id} path value as a positive int64 (400 otherwise).
func (s *Server) pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid id"})
		return 0, false
	}
	return id, true
}

// validReason enforces a non-empty, non-whitespace audit reason (400 otherwise).
func (s *Server) validReason(w http.ResponseWriter, reason string) bool {
	if strings.TrimSpace(reason) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "a non-empty reason is required for every config change"})
		return false
	}
	return true
}

// respondEdit maps a configstore edit result to an HTTP status.
func (s *Server) respondEdit(w http.ResponseWriter, version int64, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "config_version": version})
	case errors.Is(err, configstore.ErrStaleConfigVersion):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "stale expected_config_version — config changed since you loaded it; reload and retry"})
	case errors.Is(err, configstore.ErrSellManageExposed):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "sell management cannot be disabled while open exposure exists"})
	case errors.Is(err, configstore.ErrNoActiveVersion):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "no active config version"})
	case errors.Is(err, configstore.ErrMultipleActiveVersions):
		s.log.Warn("dashboard config edit refused: multiple active config versions", "err", err)
		writeJSON(w, http.StatusConflict, map[string]any{"error": "multiple active config versions — refusing to edit until resolved"})
	case errors.Is(err, configstore.ErrNoChanges):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "no fields to change"})
	case configstore.IsValidation(err):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
	default:
		s.log.Warn("dashboard config edit failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "config edit failed"})
	}
}

// editSymbolConfig edits a market's symbol config (config_operator+). The update struct is
// embedded so its JSON fields decode at the top level alongside expected_config_version +
// reason. changed_by is the authenticated user.
func (s *Server) editSymbolConfig(w http.ResponseWriter, r *http.Request) {
	sess := sessionFrom(r.Context())
	emID, ok := s.pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		configstore.SymbolConfigUpdate
		ExpectedConfigVersion int64  `json:"expected_config_version"`
		Reason                string `json:"reason"`
	}
	if !decodeStrictJSON(w, r, &body) || !s.validReason(w, body.Reason) {
		return
	}
	v, err := s.cfgStore.UpdateSymbolConfig(r.Context(), emID, body.SymbolConfigUpdate, sess.Username, body.Reason, body.ExpectedConfigVersion)
	s.respondEdit(w, v, err)
}

// editMarketFlags edits a market's enable flags. HIGH-RISK transitions — enabling trading,
// or disabling sell management — require the admin role; disabling sell management is also
// blocked (409) by configstore while open exposure exists.
func (s *Server) editMarketFlags(w http.ResponseWriter, r *http.Request) {
	sess := sessionFrom(r.Context())
	emID, ok := s.pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		configstore.MarketFlags
		ExpectedConfigVersion int64  `json:"expected_config_version"`
		Reason                string `json:"reason"`
	}
	if !decodeStrictJSON(w, r, &body) || !s.validReason(w, body.Reason) {
		return
	}
	highRisk := (body.Trading != nil && *body.Trading) || (body.SellManage != nil && !*body.SellManage)
	if highRisk && sess.Role != RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "enabling trading or disabling sell management requires the admin role"})
		return
	}
	v, err := s.cfgStore.UpdateMarketFlags(r.Context(), emID, body.MarketFlags, sess.Username, body.Reason, body.ExpectedConfigVersion)
	s.respondEdit(w, v, err)
}

// editExchangeConfig edits a per-exchange operational config (config_operator+).
func (s *Server) editExchangeConfig(w http.ResponseWriter, r *http.Request) {
	sess := sessionFrom(r.Context())
	exID, ok := s.pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		configstore.ExchangeConfigUpdate
		ExpectedConfigVersion int64  `json:"expected_config_version"`
		Reason                string `json:"reason"`
	}
	if !decodeStrictJSON(w, r, &body) || !s.validReason(w, body.Reason) {
		return
	}
	v, err := s.cfgStore.UpdateExchangeConfig(r.Context(), exID, body.ExchangeConfigUpdate, sess.Username, body.Reason, body.ExpectedConfigVersion)
	s.respondEdit(w, v, err)
}

// editFee upserts a maker/taker fee (config_operator+). exchange_market_id 0/absent = the
// exchange-wide default.
func (s *Server) editFee(w http.ResponseWriter, r *http.Request) {
	sess := sessionFrom(r.Context())
	var body struct {
		ExchangeID            int64  `json:"exchange_id"`
		ExchangeMarketID      int64  `json:"exchange_market_id"`
		MakerFee              string `json:"maker_fee"`
		TakerFee              string `json:"taker_fee"`
		ExpectedConfigVersion int64  `json:"expected_config_version"`
		Reason                string `json:"reason"`
	}
	if !decodeStrictJSON(w, r, &body) || !s.validReason(w, body.Reason) {
		return
	}
	if body.ExchangeID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "exchange_id is required"})
		return
	}
	v, err := s.cfgStore.UpsertFee(r.Context(), body.ExchangeID, body.ExchangeMarketID, body.MakerFee, body.TakerFee, sess.Username, body.Reason, body.ExpectedConfigVersion)
	s.respondEdit(w, v, err)
}

// audit exposes the config-change audit history (read-only; any logged-in role).
func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, `
SELECT id, config_version, entity_type, entity_id, field, old_value, new_value, changed_by, reason, activated_at, created_at
FROM config_change_audit ORDER BY id DESC LIMIT ?`, s.limit(r))
}
