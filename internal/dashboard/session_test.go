package dashboard

import (
	"fmt"
	"net/http"
	"testing"
)

func TestSessionStartRequiresAdmin(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary()
	body := fmt.Sprintf(`{"exchange_id":%d,"market_id":%d,"reason":"go"}`, exID, mkID)
	if c, _ := f.post("/api/live/session/start", "", body); c != http.StatusUnauthorized {
		t.Errorf("no token = %d, want 401", c)
	}
	for _, role := range []string{RoleViewer, RoleConfigOperator, RoleCredentialOperator, RoleReconcileOperator} {
		if c, _ := f.post("/api/live/session/start", f.token(role), body); c != http.StatusForbidden {
			t.Errorf("%s = %d, want 403 (admin required)", role, c)
		}
	}
}

func TestSessionStartRequiresAck(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary() // ready, but no acknowledgement yet
	body := fmt.Sprintf(`{"exchange_id":%d,"market_id":%d,"reason":"go"}`, exID, mkID)
	c, obj := f.post("/api/live/session/start", f.token(RoleAdmin), body)
	if c != http.StatusBadRequest {
		t.Fatalf("start without ack = %d (%v), want 400", c, obj)
	}
}

func TestSessionStartFailedPreflightReturns409(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary()
	f.exec("UPDATE live_controls SET kill_switch=1 WHERE id=1") // preflight not ready
	body := fmt.Sprintf(`{"exchange_id":%d,"market_id":%d,"reason":"go"}`, exID, mkID)
	c, _ := f.post("/api/live/session/start", f.token(RoleAdmin), body)
	if c != http.StatusConflict {
		t.Errorf("start with failed preflight = %d, want 409", c)
	}
}

func TestSessionStartStopAndView(t *testing.T) {
	f := setupDLive(t)
	exID, mkID := f.seedReadyCanary()
	admin := f.token(RoleAdmin)
	body := fmt.Sprintf(`{"exchange_id":%d,"market_id":%d,"reason":"first canary"}`, exID, mkID)

	// Acknowledge (admin), then start the session.
	if c, o := f.post("/api/live/acknowledge", admin, body); c != http.StatusOK {
		t.Fatalf("acknowledge = %d (%v)", c, o)
	}
	c, obj := f.post("/api/live/session/start", admin, body)
	if c != http.StatusOK {
		t.Fatalf("start = %d (%v)", c, obj)
	}
	if _, ok := obj["session_id"]; !ok {
		t.Error("start should return session_id")
	}

	// The session view shows the ACTIVE session + visibility fields.
	code, view := f.getObj("/api/live/session")
	if code != http.StatusOK {
		t.Fatalf("session view = %d", code)
	}
	sess, _ := view["session"].(map[string]any)
	if sess == nil || sess["status"] != "ACTIVE" {
		t.Errorf("expected an ACTIVE session in the view, got %v", view["session"])
	}
	for _, k := range []string{"order_count", "open_cycles", "kill_switch", "acknowledgement"} {
		if _, ok := view[k]; !ok {
			t.Errorf("session view missing %q", k)
		}
	}

	// Stop the session.
	if c, o := f.post("/api/live/session/stop", admin, body); c != http.StatusOK {
		t.Fatalf("stop = %d (%v)", c, o)
	}
	_, view2 := f.getObj("/api/live/session")
	if view2["session"] != nil {
		t.Error("after stop there should be no active session")
	}
	// Stopping again -> 409 (no active session).
	if c, _ := f.post("/api/live/session/stop", admin, body); c != http.StatusConflict {
		t.Errorf("second stop = %d, want 409", c)
	}
}
