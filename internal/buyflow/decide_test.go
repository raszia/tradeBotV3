package buyflow

import (
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func policy(enabled bool, attempts, offset int) configstore.MakerPolicy {
	return configstore.MakerPolicy{
		MakerFirstEnabled:        enabled,
		MakerAttemptsBeforeTaker: attempts,
		MakerSignalWindowSeconds: 60,
		MakerPriceOffsetBps:      offset,
		TakerPriceMode:           "ASK",
	}
}

func TestDecideMakerFirst(t *testing.T) {
	d := Decide(policy(true, 2, 10), dec("100"), 0)
	if d.Mode != ModeMakerFirst || d.AttemptNumber != 1 || d.OffsetBps != 10 {
		t.Fatalf("decision = %+v, want MAKER_FIRST attempt 1 offset 10", d)
	}
	if !d.LimitPrice.Equal(dec("99.9")) { // 100 × (10000−10)/10000
		t.Errorf("maker limit = %s, want 99.9", d.LimitPrice)
	}
}

func TestDecideMakerRetry(t *testing.T) {
	d := Decide(policy(true, 2, 10), dec("100"), 1) // one prior maker attempt
	if d.Mode != ModeMakerRetry || d.AttemptNumber != 2 {
		t.Fatalf("decision = %+v, want MAKER_RETRY attempt 2", d)
	}
	if !d.LimitPrice.Equal(dec("99.9")) {
		t.Errorf("maker-retry limit = %s, want 99.9", d.LimitPrice)
	}
}

func TestDecideTakerFallbackAfterAttempts(t *testing.T) {
	d := Decide(policy(true, 2, 10), dec("100"), 2) // two prior maker attempts -> attempt 3 > 2
	if d.Mode != ModeTakerFallback || d.AttemptNumber != 3 || d.OffsetBps != 0 {
		t.Fatalf("decision = %+v, want TAKER_FALLBACK attempt 3 offset 0", d)
	}
	if !d.LimitPrice.Equal(dec("100")) { // taker buys at the ask
		t.Errorf("taker limit = %s, want 100 (ask)", d.LimitPrice)
	}
}

func TestDecideMakerDisabledIsAlwaysTaker(t *testing.T) {
	d := Decide(policy(false, 5, 10), dec("100"), 0)
	if d.Mode != ModeTakerFallback || !d.LimitPrice.Equal(dec("100")) {
		t.Errorf("maker-disabled = %+v, want TAKER_FALLBACK at ask", d)
	}
}

func TestDecideThresholdAndOffsetNormalized(t *testing.T) {
	// attempts<1 behaves as 1: first attempt is maker, second escalates.
	if d := Decide(policy(true, 0, 5), dec("100"), 0); d.Mode != ModeMakerFirst {
		t.Errorf("attempts=0 should still allow one maker attempt, got %s", d.Mode)
	}
	if d := Decide(policy(true, 0, 5), dec("100"), 1); d.Mode != ModeTakerFallback {
		t.Errorf("attempts=0 should escalate on the 2nd, got %s", d.Mode)
	}
	// negative offset clamps to 0 (limit == ask).
	if d := Decide(policy(true, 1, -50), dec("100"), 0); !d.LimitPrice.Equal(dec("100")) {
		t.Errorf("negative offset should clamp to 0, limit=%s", d.LimitPrice)
	}
}

func TestModeIsMaker(t *testing.T) {
	if !ModeMakerFirst.IsMaker() || !ModeMakerRetry.IsMaker() || ModeTakerFallback.IsMaker() {
		t.Error("IsMaker classification wrong")
	}
}

func TestResolveQuantity(t *testing.T) {
	if q := resolveQuantity(dec("0.5"), "base", dec("100")); !q.Equal(dec("0.5")) {
		t.Errorf("base size = %s, want 0.5", q)
	}
	if q := resolveQuantity(dec("1000"), "quote", dec("100")); !q.Equal(dec("10")) {
		t.Errorf("quote size 1000 / price 100 = %s, want 10", q)
	}
	if q := resolveQuantity(dec("1000"), "quote", dec("0")); !q.IsZero() {
		t.Errorf("quote size with non-positive price should be 0, got %s", q)
	}
}
