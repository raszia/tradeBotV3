// Command trade-engine consumes market events and market data, computes the
// (owner-defined) spread/signal, and writes comparison_events / signals. It NEVER
// calls an exchange API directly (no private clients, no PlaceOrder/CancelOrder)
// and in PR8 does NOT create cycles/orders (that is PR9). It may refresh/remove an
// existing not-yet-claimed QUEUED buy intent to avoid duplicate pending intent.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/engine"
	redisx "v3TradeBot/internal/redis"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("trade-engine", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		rc, err := redisx.New(ctx, base.Cfg.Redis)
		if err != nil {
			return fmt.Errorf("connect redis: %w", err)
		}
		defer rc.Close()

		// DB-backed, versioned trading config served from an in-memory copy-on-write
		// cache so the signal hot path never queries the database. Reloaded off-path.
		cache := configstore.NewCache()
		cfgStore := configstore.New(store.DB())
		go cache.Run(ctx, cfgStore, 30*time.Second, func(e error) {
			base.Log.Warn("config reload failed (keeping previous snapshot)", "err", e)
		})

		eng := engine.New(store, rc, cache, clock.NewSystem(), base.Log, engine.Config{})
		base.Log.Info("trade-engine starting", "redis", base.Cfg.Redis.Addr)
		return eng.Run(ctx) // blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "trade-engine: "+err.Error())
		os.Exit(1)
	}
}
