package live

import (
	"testing"

	"v3TradeBot/internal/preflight"
)

// canaryControls writes a controls row that REQUIRES a canary acknowledgement, scoped to
// this fixture's exchange/market. Count caps are huge so shared-DB pollution never trips
// them — these tests isolate the PR23 ack gate.
func (f *lfix) canaryControls() {
	f.exec("DELETE FROM live_controls")
	f.exec(`INSERT INTO live_controls
		(id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty,
		 max_consecutive_failures, max_unresolved_reconcile, require_canary_ack, canary_exchange_id, canary_market_id)
		VALUES (1, 0, 1000000000, 1000000000, '1000000000000000', '1000', '10', 1000000000, 1000000000, 1, ?, ?)`, f.ex, f.em)
}

func (f *lfix) insertAck(hash string) {
	f.exec("INSERT INTO live_acknowledgements (operator, exchange_id, exchange_market_id, canonical_symbol, preflight_hash) VALUES ('op', ?, ?, 'S', ?)", f.ex, f.em, hash)
}

func TestCanaryAckGateRequiresValidAck(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	// No acknowledgement -> a live buy cycle is denied even though creds/caps/live-flags exist.
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("buy must be denied without a live acknowledgement")
	}
	// A buy PLACE is likewise denied.
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Error("buy place must be denied without a live acknowledgement")
	}
	// Record a matching acknowledgement -> allowed.
	hash, err := preflight.ConfigHash(f.ctx, f.db, f.ex, f.em, "live")
	if err != nil {
		t.Fatal(err)
	}
	f.insertAck(hash)
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		t.Errorf("buy must be allowed with a valid ack: %s", d.Reason)
	}
	// A config change makes the ack stale (hash no longer matches) -> denied again.
	f.set("max_order_notional", "999")
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("buy must be denied after a config change (ack stale)")
	}
}

func TestCanaryScopeRestrictsToOneMarket(t *testing.T) {
	f := setupL(t)
	f.canaryControls()
	hash, _ := preflight.ConfigHash(f.ctx, f.db, f.ex, f.em, "live")
	f.insertAck(hash)
	// A DIFFERENT live-enabled market is outside the canary scope -> denied.
	otherEx, otherEm := f.seedMarket(1, 1)
	if d := f.g.AllowNewBuyCycle(f.ctx, otherEx, otherEm); d.Allow {
		t.Error("a buy outside the canary exchange/symbol must be denied")
	}
	// The canary market itself is still allowed.
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		t.Errorf("canary market should be allowed: %s", d.Reason)
	}
}

func TestCanaryAckRequiredButScopeUnset(t *testing.T) {
	f := setupL(t)
	// require_canary_ack=1 but no canary scope configured -> deny (cannot trade live).
	f.exec("DELETE FROM live_controls")
	f.exec(`INSERT INTO live_controls
		(id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty,
		 max_consecutive_failures, max_unresolved_reconcile, require_canary_ack)
		VALUES (1, 0, 1000000000, 1000000000, '1000000000000000', '1000', '10', 1000000000, 1000000000, 1)`)
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("buy must be denied when canary ack is required but scope is unset")
	}
}
