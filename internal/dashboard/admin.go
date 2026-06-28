package dashboard

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/regime"
)

// Config-editing handlers (PR17). Each is wrapped by requireConfigOperator (auth +
// role). They decode a JSON body, call the versioned+audited store method, and map
// validation errors to 400 / no-change to 400 / others to 500. The audit changed_by
// is the AUTHENTICATED operator (never client-supplied). None of these trade or touch
// cycles/orders/queue/credentials.

func (s *Server) editSymbolConfig(w http.ResponseWriter, r *http.Request, op operator) {
	emID, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		configstore.SymbolConfigUpdate
		Reason string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	v, err := s.cfgStore.UpdateSymbolConfig(r.Context(), emID, body.SymbolConfigUpdate, op.name, body.Reason)
	s.respondEdit(w, v, err)
}

func (s *Server) editMarketFlags(w http.ResponseWriter, r *http.Request, op operator) {
	emID, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		configstore.MarketFlags
		Reason string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	v, err := s.cfgStore.UpdateMarketFlags(r.Context(), emID, body.MarketFlags, op.name, body.Reason)
	s.respondEdit(w, v, err)
}

func (s *Server) editExchangeConfig(w http.ResponseWriter, r *http.Request, op operator) {
	exID, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		configstore.ExchangeConfigUpdate
		Reason string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	v, err := s.cfgStore.UpdateExchangeConfig(r.Context(), exID, body.ExchangeConfigUpdate, op.name, body.Reason)
	s.respondEdit(w, v, err)
}

func (s *Server) editFee(w http.ResponseWriter, r *http.Request, op operator) {
	var body struct {
		ExchangeID       int64  `json:"exchange_id"`
		ExchangeMarketID int64  `json:"exchange_market_id"` // 0 = exchange default
		MakerFee         string `json:"maker_fee"`
		TakerFee         string `json:"taker_fee"`
		Reason           string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if body.ExchangeID == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "exchange_id is required"})
		return
	}
	v, err := s.cfgStore.UpsertFee(r.Context(), body.ExchangeID, body.ExchangeMarketID, body.MakerFee, body.TakerFee, op.name, body.Reason)
	s.respondEdit(w, v, err)
}

func (s *Server) editRegimeBasket(w http.ResponseWriter, r *http.Request, op operator) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		Name                  *string `json:"name"`
		Enabled               *bool   `json:"enabled"`
		UpdateIntervalSeconds *int    `json:"update_interval_seconds"`
		NeutralBandBps        *int    `json:"neutral_band_bps"`
		ModerateThresholdBps  *int    `json:"moderate_threshold_bps"`
		StrongThresholdBps    *int    `json:"strong_threshold_bps"`
		Reason                string  `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	v, err := s.regStore.UpdateBasket(r.Context(), id, regime.BasketUpdate{
		Name: body.Name, Enabled: body.Enabled, UpdateIntervalSeconds: body.UpdateIntervalSeconds,
		NeutralBandBps: body.NeutralBandBps, ModerateThresholdBps: body.ModerateThresholdBps, StrongThresholdBps: body.StrongThresholdBps,
	}, op.name, body.Reason)
	s.respondEdit(w, v, err)
}

func (s *Server) editRegimeSymbol(w http.ResponseWriter, r *http.Request, op operator) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		Symbol  string `json:"symbol"`
		Weight  string `json:"weight"`
		Enabled bool   `json:"enabled"`
		Reason  string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	v, err := s.regStore.UpsertBasketSymbol(r.Context(), id, body.Symbol, body.Weight, body.Enabled, op.name, body.Reason)
	s.respondEdit(w, v, err)
}

func (s *Server) editRegimeTimeframe(w http.ResponseWriter, r *http.Request, op operator) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		Label   string `json:"label"`
		Seconds int    `json:"seconds"`
		Weight  string `json:"weight"`
		Reason  string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	v, err := s.regStore.UpsertTimeframe(r.Context(), id, body.Label, body.Seconds, body.Weight, op.name, body.Reason)
	s.respondEdit(w, v, err)
}

// ---- helpers ----

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid id"})
		return 0, false
	}
	return id, true
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid request body: " + err.Error()})
		return false
	}
	return true
}

// respondEdit maps a store result to HTTP: validation/no-change → 400, other → 500,
// success → 200 with the new config version.
func (s *Server) respondEdit(w http.ResponseWriter, version int64, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "config_version": version})
	case configstore.IsValidation(err):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
	case errors.Is(err, configstore.ErrNoChanges):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "no fields to change"})
	default:
		s.log.Warn("config edit failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "config edit failed"})
	}
}
