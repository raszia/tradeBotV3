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
	return c.ReloadValidated(ctx, loader, nil)
}

// ReloadValidated loads a new snapshot, runs `validate` against it, and swaps it in ONLY if
// it is valid (PR20 correction: EVERY reload — startup and periodic — is validated before it
// replaces the active snapshot). On a load error or a validation failure the active snapshot
// is LEFT UNCHANGED (the last known-good config keeps serving) and the error is returned so
// the caller can log it safely. A nil validator accepts any successfully-loaded snapshot.
func (c *Cache) ReloadValidated(ctx context.Context, loader Loader, validate func(*Snapshot) error) error {
	s, err := loader.LoadSnapshot(ctx)
	if err != nil {
		return err
	}
	if validate != nil {
		if err := validate(s); err != nil {
			return err // keep the previous snapshot; an invalid reload must never take effect
		}
	}
	c.snap.Store(s)
	return nil
}

// Run periodically reloads with NO validation. Prefer RunValidated on the execution path.
func (c *Cache) Run(ctx context.Context, loader Loader, interval time.Duration, onErr func(error)) {
	if err := c.Reload(ctx, loader); err != nil && onErr != nil {
		onErr(err)
	}
	c.RunValidated(ctx, loader, interval, nil, onErr)
}

// RunValidated runs the periodic reload loop, validating each reload before it is swapped in.
// It performs NO immediate reload at start — the caller has already done a validated initial
// load, so a second (potentially unvalidated) reload right after startup is both redundant and
// unsafe. A failed/invalid reload keeps the last known-good snapshot and calls onErr.
func (c *Cache) RunValidated(ctx context.Context, loader Loader, interval time.Duration, validate func(*Snapshot) error, onErr func(error)) {
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
			if err := c.ReloadValidated(ctx, loader, validate); err != nil && onErr != nil {
				onErr(err)
			}
		}
	}
}
