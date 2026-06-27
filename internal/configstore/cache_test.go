package configstore

import (
	"context"
	"errors"
	"sync"
	"testing"
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
