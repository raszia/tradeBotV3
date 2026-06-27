package configstore

import (
	"context"
	"sync/atomic"
	"time"
)

// Loader produces a fresh Snapshot. Store implements it; tests fake it.
type Loader interface {
	LoadSnapshot(ctx context.Context) (*Snapshot, error)
}

// Cache holds the active config Snapshot behind an atomic pointer, so the trading
// hot path reads config from memory with NO lock and NO database query
// (rule #4). Reloads are copy-on-write: a new immutable Snapshot is built off the
// trading path and swapped in atomically; in-flight readers keep using the old
// (still-valid) Snapshot.
type Cache struct {
	snap atomic.Pointer[Snapshot]
}

// NewCache returns a cache seeded with an empty snapshot (safe to read before the
// first successful load).
func NewCache() *Cache {
	c := &Cache{}
	c.snap.Store(emptySnapshot())
	return c
}

// Snapshot returns the current immutable snapshot. Never blocks. Callers MUST NOT
// mutate it.
func (c *Cache) Snapshot() *Snapshot { return c.snap.Load() }

// Reload loads a new snapshot and swaps it in. On load error the previous
// snapshot is RETAINED (a transient DB blip must not wipe good config).
func (c *Cache) Reload(ctx context.Context, loader Loader) error {
	s, err := loader.LoadSnapshot(ctx)
	if err != nil {
		return err
	}
	c.snap.Store(s)
	return nil
}

// Run performs an initial load then reloads every interval until ctx is
// cancelled. Reload errors are passed to onErr (if non-nil) and the old snapshot
// is kept. This runs OFF the trading path.
func (c *Cache) Run(ctx context.Context, loader Loader, interval time.Duration, onErr func(error)) {
	if err := c.Reload(ctx, loader); err != nil && onErr != nil {
		onErr(err)
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Reload(ctx, loader); err != nil && onErr != nil {
				onErr(err)
			}
		}
	}
}
