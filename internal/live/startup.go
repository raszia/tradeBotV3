package live

import (
	"context"
	"database/sql"

	"v3TradeBot/internal/clock"
)

// SafetySummary is the startup live-safety snapshot a live-capable binary logs so an
// operator can see, at a glance, exactly how dangerous the process is. It contains NO
// secrets — only the credential STATUS, exchange/symbol codes, and booleans.
type SafetySummary struct {
	ExecutionMode      string `json:"execution_mode"`
	LiveEnabled        bool   `json:"live_enabled"`
	KillSwitch         bool   `json:"kill_switch"`
	CanaryExchange     string `json:"canary_exchange"`
	CanarySymbol       string `json:"canary_symbol"`
	CapsConfigured     bool   `json:"caps_configured"`
	CredentialStatus   string `json:"credential_status"`
	ActiveSession      bool   `json:"active_session"`
	NewLiveBuysAllowed bool   `json:"new_live_buys_allowed"`
}

// BuildSafetySummary computes the startup safety snapshot for the configured canary scope.
// It is read-only and writes nothing (it does NOT audit). new_live_buys_allowed reflects
// the full guard verdict (mode + caps + kill switch + scope + ack + dynamic re-check +
// active session) for the canary scope.
func BuildSafetySummary(ctx context.Context, db *sql.DB, clk clock.Clock, mode string) SafetySummary {
	if clk == nil {
		clk = clock.NewSystem()
	}
	g := NewGuard(db, clk, nil)
	ctrl, _ := g.LoadControls(ctx)

	s := SafetySummary{
		ExecutionMode:  mode,
		LiveEnabled:    mode == "live",
		KillSwitch:     ctrl.KillSwitch,
		CapsConfigured: ctrl.Configured(),
	}
	if ctrl.CanaryExchangeID != 0 {
		_ = db.QueryRowContext(ctx, "SELECT code FROM exchanges WHERE id=?", ctrl.CanaryExchangeID).Scan(&s.CanaryExchange)
	}
	if ctrl.CanaryMarketID != 0 {
		_ = db.QueryRowContext(ctx, "SELECT canonical_symbol FROM exchange_markets WHERE id=?", ctrl.CanaryMarketID).Scan(&s.CanarySymbol)
	}
	s.CredentialStatus = "none"
	if ctrl.CanaryExchangeID != 0 {
		var st sql.NullString
		_ = db.QueryRowContext(ctx,
			"SELECT status FROM exchange_credentials WHERE exchange_id=? AND enabled=1 ORDER BY key_version DESC, id DESC LIMIT 1", ctrl.CanaryExchangeID).Scan(&st)
		if st.Valid {
			s.CredentialStatus = st.String
		}
	}
	if ctrl.CanaryExchangeID != 0 && ctrl.CanaryMarketID != 0 {
		_, s.ActiveSession = ActiveSession(ctx, db, ctrl.CanaryExchangeID, ctrl.CanaryMarketID)
		if s.LiveEnabled {
			s.NewLiveBuysAllowed = g.AllowNewBuyCycle(ctx, ctrl.CanaryExchangeID, ctrl.CanaryMarketID).Allow
		}
	}
	return s
}

// LogArgs returns the summary as slog key/value pairs (no secrets) for one structured line.
func (s SafetySummary) LogArgs() []any {
	return []any{
		"execution_mode", s.ExecutionMode,
		"live_enabled", s.LiveEnabled,
		"kill_switch", s.KillSwitch,
		"canary_exchange", s.CanaryExchange,
		"canary_symbol", s.CanarySymbol,
		"caps_configured", s.CapsConfigured,
		"credential_status", s.CredentialStatus,
		"active_session", s.ActiveSession,
		"new_live_buys_allowed", s.NewLiveBuysAllowed,
	}
}
