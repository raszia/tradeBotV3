package live

import (
	"database/sql"
	"strings"
	"testing"

	"v3TradeBot/internal/clock"
)

func (f *lfix) activeSessionID() int64 {
	var id int64
	f.db.QueryRow("SELECT id FROM live_run_sessions WHERE exchange_id=? AND exchange_market_id=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1", f.ex, f.em).Scan(&id)
	return id
}

func TestStartSessionRequiresValidAck(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady()
	// No acknowledgement -> cannot start.
	if _, err := StartSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "op", "go"); err != ErrNoValidAck {
		t.Errorf("start without ack err = %v, want ErrNoValidAck", err)
	}
}

func TestStartSessionOutOfScopeRejected(t *testing.T) {
	f := setupL(t)
	f.canaryControls() // canary scope = f.ex/f.em
	f.seedDynamicReady()
	f.insertAck(f.hash())
	otherEx, otherEm := f.seedMarket(1, 1)
	if _, err := StartSession(f.ctx, f.db, clock.NewSystem(), otherEx, otherEm, "op", "go"); err != ErrOutOfCanaryScope {
		t.Errorf("out-of-scope start err = %v, want ErrOutOfCanaryScope", err)
	}
}

func TestStartSessionFailedReadinessRejected(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady()
	f.insertAck(f.hash())
	// Break a dynamic condition AFTER the ack -> start must refuse (not ready).
	f.exec("UPDATE exchange_credentials SET last_checked_at = NOW(6) - INTERVAL 200 MINUTE WHERE exchange_id=?", f.ex)
	if _, err := StartSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "op", "go"); err != ErrNotReady {
		t.Errorf("not-ready start err = %v, want ErrNotReady", err)
	}
}

func TestStartSessionSucceedsRecordedAndSingle(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady()
	f.insertAck(f.hash())
	id, err := StartSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "alice", "first canary run")
	if err != nil {
		t.Fatal(err)
	}
	var op, status, reason string
	f.db.QueryRow("SELECT operator, status, start_reason FROM live_run_sessions WHERE id=?", id).Scan(&op, &status, &reason)
	if op != "alice" || status != "ACTIVE" || reason == "" {
		t.Errorf("session = op:%s status:%s reason:%q", op, status, reason)
	}
	// A second concurrent session is rejected.
	if _, err := StartSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "bob", "again"); err != ErrSessionAlreadyActive {
		t.Errorf("second start err = %v, want ErrSessionAlreadyActive", err)
	}
}

func TestStopSessionBlocksBuysAllowsSellCancel(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady()
	f.insertAck(f.hash())
	if _, err := StartSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "alice", "run"); err != nil {
		t.Fatal(err)
	}
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		t.Fatalf("buy should be allowed with an active session: %s", d.Reason)
	}
	// Stop -> new buys blocked immediately.
	if err := StopSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "alice", "stopping the canary"); err != nil {
		t.Fatal(err)
	}
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("after stop, new buys must be blocked")
	}
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Error("after stop, a live buy place must be blocked")
	}
	// Risk-reducing paths remain available.
	if d := f.g.CheckPlace(f.ctx, f.sell()); !d.Allow {
		t.Errorf("sell must remain allowed after stop: %s", d.Reason)
	}
	if d := f.g.CheckCancel(f.ctx, PlaceCheck{ExchangeID: f.ex}); !d.Allow {
		t.Errorf("cancel must remain allowed after stop: %s", d.Reason)
	}
	// The stop is audited on the session row.
	var stoppedReason string
	var stoppedAt []byte
	f.db.QueryRow("SELECT stop_reason, stopped_at FROM live_run_sessions WHERE exchange_id=? AND status='STOPPED' ORDER BY id DESC LIMIT 1", f.ex).Scan(&stoppedReason, &stoppedAt)
	if stoppedReason == "" || stoppedAt == nil {
		t.Error("stop must record stop_reason + stopped_at")
	}
}

func TestStopSessionWithoutActiveSession(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	if err := StopSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "op", "x"); err != ErrNoActiveSession {
		t.Errorf("stop with no active session err = %v, want ErrNoActiveSession", err)
	}
}

func TestLiveAuditIncludesSessionAndAck(t *testing.T) {
	f := setupL(t)
	f.readyAck() // ack + active session
	sessID := f.activeSessionID()
	// A buy decision is correlated with the session + ack + hash.
	f.g.CheckPlace(f.ctx, f.buy("50", "0.5"))
	var auditSession, auditAck sql.NullInt64
	var hash sql.NullString
	f.db.QueryRow("SELECT live_session_id, acknowledgement_id, preflight_hash FROM live_audit WHERE exchange_id=? AND action='place_buy' ORDER BY id DESC LIMIT 1", f.ex).
		Scan(&auditSession, &auditAck, &hash)
	if !auditSession.Valid || auditSession.Int64 != sessID {
		t.Errorf("live_audit live_session_id = %v, want %d", auditSession, sessID)
	}
	if !auditAck.Valid || !hash.Valid || hash.String == "" {
		t.Error("live_audit should carry acknowledgement_id + preflight_hash")
	}
}

func TestFirstOrderChecklistWritten(t *testing.T) {
	f := setupL(t)
	f.readyAck()
	sessID := f.activeSessionID()
	pc := f.buy("50", "0.5")
	pc.ExchangeCode, pc.Symbol = "lx", "BAS/IRT"
	pc.RequestID, pc.OrderID, pc.CycleID = 11, 22, 33
	f.g.RecordFirstOrderChecklist(f.ctx, pc)

	var checklist sql.NullString
	f.db.QueryRow("SELECT first_order_checklist_json FROM live_run_sessions WHERE id=?", sessID).Scan(&checklist)
	if !checklist.Valid {
		t.Fatal("first-order checklist should be written for the first buy")
	}
	for _, want := range []string{`"mode":"live"`, `"session_id"`, `"caps_remaining"`, `"kill_switch"`, `"request_id"`} {
		if !strings.Contains(checklist.String, want) {
			t.Errorf("checklist missing %s: %s", want, checklist.String)
		}
	}
	// No secret material.
	for _, leak := range []string{"api_key", "api_secret", "encrypted", "passphrase"} {
		if strings.Contains(checklist.String, leak) {
			t.Errorf("checklist leaked %q", leak)
		}
	}
	// Idempotent: a second call does not overwrite/duplicate.
	before := checklist.String
	f.g.RecordFirstOrderChecklist(f.ctx, pc)
	var after sql.NullString
	f.db.QueryRow("SELECT first_order_checklist_json FROM live_run_sessions WHERE id=?", sessID).Scan(&after)
	if after.String != before {
		t.Error("first-order checklist must be written only once (idempotent)")
	}
}
