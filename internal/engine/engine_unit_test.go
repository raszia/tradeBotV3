package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/events"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeSubscriber is a marketSubscriber whose behaviour per call is scriptable, and which
// counts how many times SubscribeMarketEvents was called (so a test can observe reconnects).
type fakeSubscriber struct {
	mu     sync.Mutex
	calls  int
	behave func(call int) (<-chan events.MarketEvent, error)
}

func (f *fakeSubscriber) SubscribeMarketEvents(context.Context) (<-chan events.MarketEvent, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	return f.behave(n)
}
func (f *fakeSubscriber) count() int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

// TestSignalLoopReconnectsOnUnexpectedClose (PR8 correction) — if the market_events channel
// closes while ctx is still active, runSignalLoop must NOT return nil; it must resubscribe
// with backoff and only stop (with a non-nil ctx error) when ctx is cancelled.
func TestSignalLoopReconnectsOnUnexpectedClose(t *testing.T) {
	fake := &fakeSubscriber{behave: func(int) (<-chan events.MarketEvent, error) {
		ch := make(chan events.MarketEvent)
		close(ch) // immediately closed → an unexpected close while ctx is active
		return ch, nil
	}}
	e := &Engine{
		subSource: fake,
		cache:     configstore.NewCache(),
		log:       quietLog(),
		cfg:       Config{SubscribeMinBackoff: time.Millisecond, SubscribeMaxBackoff: 5 * time.Millisecond},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.runSignalLoop(ctx) }()

	// It must keep resubscribing (never return) while ctx is active.
	deadline := time.After(2 * time.Second)
	for fake.count() < 3 {
		select {
		case <-deadline:
			t.Fatalf("did not resubscribe after unexpected close (calls=%d)", fake.count())
		case err := <-done:
			t.Fatalf("runSignalLoop returned early (%v) instead of resubscribing on close", err)
		case <-time.After(time.Millisecond):
		}
	}

	// Cancellation stops it cleanly with a NON-NIL ctx error.
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("runSignalLoop returned nil; want a non-nil ctx error on cancel")
		} else if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runSignalLoop did not stop after ctx cancel")
	}
}

// TestSignalLoopRetriesFailedSubscribe (PR8 correction) — a failing SubscribeMarketEvents is
// retried with backoff (not returned) while ctx is active.
func TestSignalLoopRetriesFailedSubscribe(t *testing.T) {
	fake := &fakeSubscriber{behave: func(int) (<-chan events.MarketEvent, error) {
		return nil, errors.New("redis down")
	}}
	e := &Engine{
		subSource: fake, cache: configstore.NewCache(), log: quietLog(),
		cfg: Config{SubscribeMinBackoff: time.Millisecond, SubscribeMaxBackoff: 5 * time.Millisecond},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.runSignalLoop(ctx) }()

	deadline := time.After(2 * time.Second)
	for fake.count() < 3 {
		select {
		case <-deadline:
			t.Fatalf("subscribe was not retried (calls=%d)", fake.count())
		case err := <-done:
			t.Fatalf("runSignalLoop returned early (%v) instead of retrying", err)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err == nil {
		t.Error("want non-nil ctx error on cancel")
	}
}

// TestTargetsReevaluatesIRTMarketsOnQuoteRateEvent (PR8 correction) — a USDT/IRT (quote-rate)
// tick on an exchange must re-evaluate ALL signal-enabled rial-quoted markets on that SAME
// exchange; it must NOT pull in USDT-quoted markets, other exchanges, or disabled markets.
func TestTargetsReevaluatesIRTMarketsOnQuoteRateEvent(t *testing.T) {
	e := &Engine{cfg: Config{ReferenceExchange: "binance", QuoteRateSymbol: "USDT/IRT"}}
	mk := func(id int64, code, sym string, sig bool) configstore.MarketConfig {
		return configstore.MarketConfig{ExchangeMarketID: id, ExchangeCode: code, CanonicalSymbol: sym, EnabledForSignal: sig}
	}
	snap := &configstore.Snapshot{
		Version: 1,
		MarketsByID: map[int64]configstore.MarketConfig{
			1: mk(1, "nobitex", "BTC/IRT", true),
			2: mk(2, "nobitex", "ETH/IRT", true),
			3: mk(3, "nobitex", "USDT/IRT", true),
			4: mk(4, "nobitex", "DOGE/USDT", true), // USDT-quoted: NOT rate-dependent
			5: mk(5, "wallex", "BTC/IRT", true),    // different exchange
			6: mk(6, "nobitex", "SOL/IRT", false),  // signal-disabled
		},
		MarketsBySymbol: map[string][]configstore.MarketConfig{
			"USDT/IRT": {mk(3, "nobitex", "USDT/IRT", true)},
			"BTC/IRT":  {mk(1, "nobitex", "BTC/IRT", true), mk(5, "wallex", "BTC/IRT", true)},
		},
	}

	has := func(ms []configstore.MarketConfig) map[int64]bool {
		m := map[int64]bool{}
		for _, x := range ms {
			m[x.ExchangeMarketID] = true
		}
		return m
	}

	// USDT/IRT tick on nobitex → BTC/IRT, ETH/IRT (and USDT/IRT itself); not 4/5/6.
	got := has(e.targets(snap, events.MarketEvent{Exchange: "nobitex", Symbol: "USDT/IRT"}))
	for _, id := range []int64{1, 2, 3} {
		if !got[id] {
			t.Errorf("USDT/IRT tick: market %d should be re-evaluated", id)
		}
	}
	for _, id := range []int64{4, 5, 6} {
		if got[id] {
			t.Errorf("USDT/IRT tick: market %d must NOT be re-evaluated", id)
		}
	}

	// A non-quote-rate Iranian tick (BTC/IRT) must NOT broaden to other IRT markets.
	got2 := has(e.targets(snap, events.MarketEvent{Exchange: "nobitex", Symbol: "BTC/IRT"}))
	if !got2[1] || got2[2] || got2[3] {
		t.Errorf("BTC/IRT tick should re-evaluate only nobitex BTC/IRT, got %v", got2)
	}
}
