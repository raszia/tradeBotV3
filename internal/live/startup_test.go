package live

import (
	"encoding/json"
	"strings"
	"testing"

	"v3TradeBot/internal/clock"
)

func TestStartupSafetySummaryNoSecrets(t *testing.T) {
	f := setupL(t)
	f.readyAck() // canary controls + ack + active session + recent dry-run

	s := BuildSafetySummary(f.ctx, f.db, clock.NewSystem(), "live")
	if s.ExecutionMode != "live" || !s.LiveEnabled || s.KillSwitch {
		t.Errorf("summary mode/live/kill = %s/%v/%v", s.ExecutionMode, s.LiveEnabled, s.KillSwitch)
	}
	if s.CanaryExchange == "" || s.CanarySymbol == "" || !s.CapsConfigured {
		t.Errorf("summary canary/caps = %q/%q/%v", s.CanaryExchange, s.CanarySymbol, s.CapsConfigured)
	}
	if s.CredentialStatus != "active" || !s.ActiveSession || !s.NewLiveBuysAllowed {
		t.Errorf("summary cred/session/allowed = %q/%v/%v", s.CredentialStatus, s.ActiveSession, s.NewLiveBuysAllowed)
	}
	// No secrets anywhere in the summary (it carries only status/codes/booleans).
	b, _ := json.Marshal(s)
	for _, leak := range []string{"api_key", "api_secret", "encrypted", "passphrase", "master"} {
		if strings.Contains(strings.ToLower(string(b)), leak) {
			t.Errorf("startup summary leaked %q: %s", leak, b)
		}
	}
	// LogArgs are key/value pairs, no secrets.
	if len(s.LogArgs())%2 != 0 {
		t.Error("LogArgs must be key/value pairs")
	}
}

func TestStartupSafetySummaryOffMode(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	s := BuildSafetySummary(f.ctx, f.db, clock.NewSystem(), "off")
	if s.LiveEnabled || s.NewLiveBuysAllowed {
		t.Error("off mode must report live disabled + no live buys allowed")
	}
}

func TestEmergencyStopMatrix(t *testing.T) {
	f := setupL(t)
	f.readyAck()
	// Baseline: a live buy is allowed.
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); !d.Allow {
		t.Fatalf("baseline buy should be allowed: %s", d.Reason)
	}

	// Emergency stop via the session: new buys blocked; risk-reducing paths keep working.
	if err := StopSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "op", "emergency stop"); err != nil {
		t.Fatal(err)
	}
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Error("emergency stop must block new buys")
	}
	if d := f.g.CheckPlace(f.ctx, f.sell()); !d.Allow {
		t.Errorf("emergency stop must keep sell management available (filled-but-unsold exit): %s", d.Reason)
	}
	if d := f.g.CheckCancel(f.ctx, PlaceCheck{ExchangeID: f.ex}); !d.Allow {
		t.Errorf("emergency stop must keep cancel/status available: %s", d.Reason)
	}

	// The global kill switch is the broadest emergency stop: same asymmetry.
	f.set("kill_switch", 1)
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Error("kill switch must block new buys")
	}
	if d := f.g.CheckPlace(f.ctx, f.sell()); !d.Allow {
		t.Errorf("kill switch must keep sells available: %s", d.Reason)
	}
}

func TestStartSessionRequiresRecentDryRun(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	f.seedDynamicReady() // includes a recent dry-run
	f.insertAck(f.hash())
	// Age out the dry-run -> start must refuse.
	f.exec("UPDATE cycles SET closed_at = NOW(6) - INTERVAL 10 DAY WHERE dry_run=1 AND exchange_market_id=?", f.em)
	if _, err := StartSession(f.ctx, f.db, clock.NewSystem(), f.ex, f.em, "op", "go"); err != ErrNoRecentDryRun {
		t.Errorf("start without a recent dry-run err = %v, want ErrNoRecentDryRun", err)
	}
}
