package buyflow

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
)

// seedSignal inserts a signals row (as the engine's writeSignal would) and returns its id.
func (f *bfix) seedSignal(m configstore.MarketConfig) int64 {
	f.t.Helper()
	res, err := f.db.Exec(
		"INSERT INTO signals (exchange_id, canonical_symbol, accepted, signal_time) VALUES (?, ?, 1, NOW(6))",
		f.exID, m.CanonicalSymbol)
	if err != nil {
		f.t.Fatalf("seed signal: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

func (f *bfix) cycleSnapshot(cycleID int64) (binP, iranP, buySize string, spread, feeAdj int, cfgVer int64) {
	f.t.Helper()
	if err := f.db.QueryRow(`SELECT binance_price_at_signal, iranian_price_at_signal, buy_size,
		spread_bps, fee_adjusted_spread_bps, config_version FROM cycles WHERE id=?`, cycleID).
		Scan(&binP, &iranP, &buySize, &spread, &feeAdj, &cfgVer); err != nil {
		f.t.Fatalf("cycleSnapshot: %v", err)
	}
	return
}

// TestRefreshUpdatesCycleSnapshotConsistently (PR9 correction #5) — a superseding signal must
// refresh the CYCLE snapshot too, not just the order/request, so cycles/orders/exchange_requests
// all describe the same current buy intent.
func TestRefreshUpdatesCycleSnapshotConsistently(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 5, 10)) // stay maker across the refresh

	// Two real config versions (config_version is FK-constrained to config_versions.id).
	cs := configstore.New(f.db)
	v1, err := cs.ActivateVersion(f.ctx, "test", "v1")
	if err != nil {
		t.Fatal(err)
	}
	v2, err := cs.ActivateVersion(f.ctx, "test", "v2")
	if err != nil {
		t.Fatal(err)
	}

	sig1 := SignalContext{ConfigVersion: v1, BinancePrice: dec("101"), IranianPrice: dec("100"), SpreadBps: 100, FeeAdjustedBps: 90, QuoteUnit: "USDT"}
	r, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), sig1, 600)
	if err != nil {
		t.Fatal(err)
	}

	// A newer, better signal: ask 99, wider spread, different config version.
	sig2 := SignalContext{ConfigVersion: v2, BinancePrice: dec("102"), IranianPrice: dec("99"), SpreadBps: 200, FeeAdjustedBps: 190, QuoteUnit: "USDT"}
	if ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("99"), sig2); err != nil || !ok {
		t.Fatalf("refresh = %v, %v; want true", ok, err)
	}

	// (a) cycle snapshot reflects signal #2.
	binP, iranP, buySize, spread, feeAdj, cfgVer := f.cycleSnapshot(r.CycleID)
	if !dec(binP).Equal(dec("102")) || !dec(iranP).Equal(dec("99")) || spread != 200 || feeAdj != 190 || cfgVer != v2 {
		t.Errorf("cycle snapshot stale: bin=%s iran=%s spread=%d feeAdj=%d cfg=%d; want 102/99/200/190/%d", binP, iranP, spread, feeAdj, cfgVer, v2)
	}
	// (b) order + (c) request reflect signal #2 (limit = 99×(1−10bps) = 98.901, ask 99).
	_, _, _, _, limit, ask, payload := f.buyState(m)
	if !dec(ask).Equal(dec("99")) || !dec(limit).Equal(dec("98.901")) {
		t.Errorf("order ask/limit = %s/%s, want 99/98.901", ask, limit)
	}
	if !strings.Contains(payload, `"intended_price":"98.901"`) {
		t.Errorf("request payload not refreshed to signal #2: %s", payload)
	}
	// buy_size on the cycle stays consistent with the order quantity (base sizing = 0.5).
	if !dec(buySize).Equal(dec("0.5")) {
		t.Errorf("cycle buy_size = %s, want 0.5", buySize)
	}
}

// TestRefreshGuardedByCycleOrderLockStates (PR9 correction #6) — refresh must be a no-op unless
// the WHOLE execution state is still queued/active. Each sub-case moves ONE piece away and
// asserts no refresh happens and the prior intent is untouched. (CLAIMED/IN_FLIGHT request is
// covered by TestRefreshNoOpWhenClaimedOrInFlight.)
func TestRefreshGuardedByCycleOrderLockStates(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *bfix, r CreateResult)
	}{
		{"order-not-queued", func(f *bfix, r CreateResult) {
			f.db.Exec("UPDATE orders SET state='SUBMITTED' WHERE id=?", r.OrderID)
		}},
		{"cycle-not-buy-request-queued", func(f *bfix, r CreateResult) {
			f.db.Exec("UPDATE cycles SET state='BUY_SUBMITTED' WHERE id=?", r.CycleID)
		}},
		{"lock-not-active", func(f *bfix, r CreateResult) {
			f.db.Exec("UPDATE symbol_locks SET state='RELEASED', released_at=NOW(6) WHERE cycle_id=?", r.CycleID)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := setupB(t)
			m := f.market("USDT", "0.5", "base", policy(true, 5, 10))
			r, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(f, r)

			ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("200"), f.sig())
			if err != nil || ok {
				t.Fatalf("refresh after %s = %v, %v; want false,nil (guarded out)", tc.name, ok, err)
			}
			// The intent's price/ask are untouched.
			var ask, limit string
			f.db.QueryRow("SELECT ask_price_at_decision, limit_price FROM orders WHERE id=?", r.OrderID).Scan(&ask, &limit)
			if !dec(ask).Equal(dec("100")) || !dec(limit).Equal(dec("99.9")) {
				t.Errorf("%s: order changed (ask=%s limit=%s); must remain 100/99.9", tc.name, ask, limit)
			}
		})
	}
}

// TestSignalLinkedToCreatedCycle (PR9 correction #8) — a passing signal that creates a cycle is
// linked (signals.cycle_id = cycle.id), and a superseding signal on refresh links to the same cycle.
func TestSignalLinkedToCreatedCycle(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 5, 10))

	sigID := f.seedSignal(m)
	sig := f.sig()
	sig.SignalID = sigID
	r, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), sig, 600)
	if err != nil {
		t.Fatal(err)
	}
	var linked int64
	if err := f.db.QueryRow("SELECT cycle_id FROM signals WHERE id=?", sigID).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked != r.CycleID {
		t.Errorf("signals.cycle_id = %d, want created cycle %d", linked, r.CycleID)
	}

	sig2ID := f.seedSignal(m)
	sig2 := f.sig()
	sig2.SignalID = sig2ID
	if ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("101"), sig2); err != nil || !ok {
		t.Fatalf("refresh = %v, %v", ok, err)
	}
	var linked2 int64
	f.db.QueryRow("SELECT cycle_id FROM signals WHERE id=?", sig2ID).Scan(&linked2)
	if linked2 != r.CycleID {
		t.Errorf("refreshed signal cycle_id = %d, want %d", linked2, r.CycleID)
	}
}

// TestNonPositivePriceRejected (PR9 correction #7) — an invalid (non-positive) limit price/qty
// is never persisted or enqueued, so it can never be sent blindly. Market-rule (tick/step/min)
// conformance is enforced at the venue; a rejection is handled cleanly by the executor (PR7).
func TestNonPositivePriceRejected(t *testing.T) {
	f := setupB(t)
	// A maker offset of 10000 bps makes limit = ask×(1−1) = 0 (non-positive).
	m := f.market("USDT", "0.5", "base", policy(true, 5, 10000))
	if _, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600); err == nil {
		t.Fatal("CreateBuyCycle must reject a non-positive limit price")
	}
	if n := f.count("cycles", m); n != 0 {
		t.Errorf("cycles created = %d, want 0 (nothing persisted on non-positive price)", n)
	}
	if n := f.buyReqs(m); n != 0 {
		t.Errorf("buy requests created = %d, want 0", n)
	}

	// A refresh that would compute a non-positive price is a no-op leaving the valid prior intent.
	m2 := f.market("USDT", "0.5", "base", policy(true, 5, 10))
	r, err := CreateBuyCycle(f.ctx, f.store, f.q, m2, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatal(err)
	}
	m2.Maker.MakerPriceOffsetBps = 10000 // next decision → limit 0
	if ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m2, dec("100"), f.sig()); err != nil || ok {
		t.Fatalf("refresh into non-positive price = %v, %v; want false,nil", ok, err)
	}
	var ask, limit string
	f.db.QueryRow("SELECT ask_price_at_decision, limit_price FROM orders WHERE id=?", r.OrderID).Scan(&ask, &limit)
	if !dec(ask).Equal(dec("100")) || !dec(limit).Equal(dec("99.9")) {
		t.Errorf("prior intent changed (ask=%s limit=%s); must remain 100/99.9", ask, limit)
	}
	_ = decimal.Zero
}
