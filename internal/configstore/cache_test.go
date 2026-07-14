package configstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeLoader returns a configurable snapshot/error.
type fakeLoader struct {
	mu   sync.Mutex
	snap *Snapshot
	err  error
	hits int
}

func (f *fakeLoader) LoadSnapshot(context.Context) (*Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
	return f.snap, f.err
}

func snapWithVersion(v int64) *Snapshot {
	s := emptySnapshot()
	s.Version = v
	s.MarketsByID[1] = MarketConfig{ExchangeMarketID: 1, CanonicalSymbol: "BTC/IRT", EnabledForTrading: true}
	return s
}

func TestCacheInitialEmpty(t *testing.T) {
	c := NewCache()
	if c.Snapshot() == nil {
		t.Fatal("cache should start with a non-nil empty snapshot")
	}
	if c.Snapshot().ConfigVersion() != 0 {
		t.Errorf("empty snapshot version = %d", c.Snapshot().ConfigVersion())
	}
}

func TestCacheReloadSwapsSnapshot(t *testing.T) {
	c := NewCache()
	loader := &fakeLoader{snap: snapWithVersion(3)}
	if err := c.Reload(context.Background(), loader); err != nil {
		t.Fatal(err)
	}
	if c.Snapshot().ConfigVersion() != 3 {
		t.Errorf("version after reload = %d, want 3", c.Snapshot().ConfigVersion())
	}
}

func TestCacheReloadIsCopyOnWrite(t *testing.T) {
	c := NewCache()
	loader := &fakeLoader{snap: snapWithVersion(1)}
	_ = c.Reload(context.Background(), loader)

	// A reader captures the current snapshot.
	old := c.Snapshot()

	// Reload to a new snapshot.
	loader.mu.Lock()
	loader.snap = snapWithVersion(2)
	loader.mu.Unlock()
	_ = c.Reload(context.Background(), loader)

	// The captured old snapshot is unchanged (copy-on-write): readers holding it
	// keep seeing version 1 even though the cache now serves version 2.
	if old.ConfigVersion() != 1 {
		t.Errorf("old snapshot mutated: version = %d", old.ConfigVersion())
	}
	if c.Snapshot().ConfigVersion() != 2 {
		t.Errorf("cache should now serve version 2, got %d", c.Snapshot().ConfigVersion())
	}
}

func TestCacheReloadErrorRetainsPreviousSnapshot(t *testing.T) {
	c := NewCache()
	loader := &fakeLoader{snap: snapWithVersion(5)}
	_ = c.Reload(context.Background(), loader)

	// Next reload fails: the cache must keep the good snapshot.
	loader.mu.Lock()
	loader.snap = nil
	loader.err = errors.New("db blip")
	loader.mu.Unlock()
	if err := c.Reload(context.Background(), loader); err == nil {
		t.Fatal("expected reload error")
	}
	if c.Snapshot().ConfigVersion() != 5 {
		t.Errorf("failed reload wiped config: version = %d, want 5", c.Snapshot().ConfigVersion())
	}
}

func TestCacheConcurrentReadsDuringReload(t *testing.T) {
	c := NewCache()
	loader := &fakeLoader{snap: snapWithVersion(1)}
	_ = c.Reload(context.Background(), loader)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Readers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = c.Snapshot().ConfigVersion() // must never panic/block
				}
			}
		}()
	}
	// Reloader
	for v := int64(2); v < 200; v++ {
		loader.mu.Lock()
		loader.snap = snapWithVersion(v)
		loader.mu.Unlock()
		_ = c.Reload(context.Background(), loader)
	}
	close(stop)
	wg.Wait()
}

// PR20 round-4 #1: every reload — startup and periodic — is validated before it replaces the
// active snapshot, and an invalid reload keeps the last known-good snapshot.

func TestReloadValidatedKeepsLastGoodOnInvalid(t *testing.T) {
	good := snapWithVersion(5)
	f := &fakeLoader{snap: good}
	c := NewCache()

	// A valid snapshot swaps in.
	validate := func(s *Snapshot) error {
		if s.Version == 0 {
			return errors.New("incomplete snapshot")
		}
		return nil
	}
	if err := c.ReloadValidated(context.Background(), f, validate); err != nil {
		t.Fatalf("valid reload: %v", err)
	}
	if c.Snapshot().ConfigVersion() != 5 {
		t.Fatalf("active version = %d, want 5", c.Snapshot().ConfigVersion())
	}

	// The NEXT reload is invalid → it must be rejected and the previous snapshot retained.
	f.mu.Lock()
	f.snap = snapWithVersion(0) // incomplete
	f.mu.Unlock()
	if err := c.ReloadValidated(context.Background(), f, validate); err == nil {
		t.Error("an invalid reload must return an error")
	}
	if c.Snapshot().ConfigVersion() != 5 {
		t.Errorf("active version after invalid reload = %d, want 5 (last known-good retained)", c.Snapshot().ConfigVersion())
	}
}

func TestRunValidatedDoesNoImmediateReload(t *testing.T) {
	// RunValidated must NOT reload at start (the caller already did a validated initial load);
	// a redundant, potentially-unvalidated second load right after startup is the bug.
	f := &fakeLoader{snap: snapWithVersion(9)}
	c := NewCache()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.RunValidated(ctx, f, time.Hour, nil, nil); close(done) }()
	// Give the goroutine a moment; with an hour interval and no immediate reload, hits stays 0.
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done
	f.mu.Lock()
	hits := f.hits
	f.mu.Unlock()
	if hits != 0 {
		t.Errorf("RunValidated performed %d immediate reload(s), want 0", hits)
	}
}

func TestReloadValidatedRejectsMissingWiredExchange(t *testing.T) {
	// Mirrors the executor's live validator: a wired exchange missing from the reloaded
	// snapshot makes the reload invalid, so pacing values are never silently lost.
	wired := map[string]struct{}{"nobitex": {}, "wallex": {}}
	validate := func(s *Snapshot) error {
		for code := range wired {
			if _, ok := s.Exchanges[code]; !ok {
				return errors.New("wired exchange missing from snapshot: " + code)
			}
		}
		return nil
	}
	full := emptySnapshot()
	full.Version = 1
	full.Exchanges["nobitex"] = ExchangeConfig{ExchangeCode: "nobitex", RateLimitPerSec: 5}
	full.Exchanges["wallex"] = ExchangeConfig{ExchangeCode: "wallex", RateLimitPerSec: 3}
	f := &fakeLoader{snap: full}
	c := NewCache()
	if err := c.ReloadValidated(context.Background(), f, validate); err != nil {
		t.Fatalf("full snapshot should validate: %v", err)
	}
	// Now wallex's row is gone → invalid → keep the previous snapshot.
	partial := emptySnapshot()
	partial.Version = 2
	partial.Exchanges["nobitex"] = ExchangeConfig{ExchangeCode: "nobitex", RateLimitPerSec: 5}
	f.mu.Lock()
	f.snap = partial
	f.mu.Unlock()
	if err := c.ReloadValidated(context.Background(), f, validate); err == nil {
		t.Error("a snapshot missing a wired exchange must be rejected")
	}
	if c.Snapshot().ConfigVersion() != 1 {
		t.Errorf("active version = %d, want 1 (partial reload rejected)", c.Snapshot().ConfigVersion())
	}
}
