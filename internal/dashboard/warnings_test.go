package dashboard

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func warningCodes(obj map[string]any) map[string]bool {
	set := map[string]bool{}
	arr, _ := obj["warnings"].([]any)
	for _, wRaw := range arr {
		if m, ok := wRaw.(map[string]any); ok {
			set[asString(m["code"])] = true
		}
	}
	return set
}

func TestLiveWarningsDangerStates(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary() // live mode, kill switch off, canary scope
	admin := f.token(RoleAdmin)
	body := fmt.Sprintf(`{"exchange_id":%d,"market_id":%d,"reason":"go"}`, exID, mkID)
	f.post("/api/live/acknowledge", admin, body)
	f.post("/api/live/session/start", admin, body)

	_, obj := f.getObj("/api/live/warnings")
	codes := warningCodes(obj)
	for _, want := range []string{"live_mode_enabled", "kill_switch_disengaged", "session_active", "first_order_pending"} {
		if !codes[want] {
			t.Errorf("expected warning %q; got %v", want, codes)
		}
	}

	// Make balance + market stale -> staleness warnings appear.
	f.exec("UPDATE wallet_balances_current SET last_seen_at = NOW(6) - INTERVAL 1 DAY WHERE exchange_id=?", exID)
	f.exec("UPDATE comparison_events SET created_at = NOW(6) - INTERVAL 1 DAY WHERE exchange_id=?", exID)
	_, obj2 := f.getObj("/api/live/warnings")
	codes2 := warningCodes(obj2)
	if !codes2["balance_stale"] || !codes2["market_data_stale"] {
		t.Errorf("expected balance_stale + market_data_stale; got %v", codes2)
	}
}

func TestLiveAuditExportIncludesBundleNoSecrets(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary()
	admin := f.token(RoleAdmin)
	body := fmt.Sprintf(`{"exchange_id":%d,"market_id":%d,"reason":"go"}`, exID, mkID)
	f.post("/api/live/acknowledge", admin, body)
	_, startObj := f.post("/api/live/session/start", admin, body)
	sid := int64(startObj["session_id"].(float64))

	// Seed a correlated live_audit decision + a first-order checklist (no secrets).
	f.exec("INSERT INTO live_audit (exchange_id, action, side, decision, reason, execution_mode, live_session_id) VALUES (?, 'place_buy', 'buy', 'allow', 'ok', 'live', ?)", exID, sid)
	f.exec(`UPDATE live_run_sessions SET first_order_checklist_json = '{"mode":"live","session_id":1,"caps_remaining":{}}' WHERE id=?`, sid)

	code, exp := f.getObj("/api/live/session/export")
	if code != http.StatusOK {
		t.Fatalf("export = %d", code)
	}
	for _, key := range []string{"session", "preflight_hash", "acknowledgement", "caps", "first_order_checklist", "requests", "orders", "decisions", "denials"} {
		if _, ok := exp[key]; !ok {
			t.Errorf("export missing %q", key)
		}
	}
	if decisions, _ := exp["decisions"].([]any); len(decisions) == 0 {
		t.Error("export should include the correlated allow decision")
	}
	// No secret material anywhere in the export.
	full := fmt.Sprintf("%v", exp)
	for _, leak := range []string{"api_key", "api_secret", "encrypted", "passphrase", "master_key"} {
		if strings.Contains(full, leak) {
			t.Errorf("export leaked %q", leak)
		}
	}
	_ = mkID
}
