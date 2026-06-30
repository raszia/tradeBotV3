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
	"v3TradeBot/internal/live"
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

		// In dry-run mode the engine stamps created cycles as dry-run so the dashboard/
		// reconciler can identify them (the executor wires the simulated client). In
		// LIVE mode the live guard gates new buy-cycle creation (kill switch + caps).
		var liveGuard *live.Guard
		if base.Cfg.Execution.IsLive() {
			liveGuard = live.NewGuard(store.DB(), clock.NewSystem(), base.Log)
		}
		eng := engine.New(store, rc, cache, clock.NewSystem(), base.Log, engine.Config{
			DryRun:    base.Cfg.Execution.IsDryRun(),
			LiveGuard: liveGuard,
			// PR9+: the full trade-engine binary prepares buy cycles on passing signals.
			// (The engine library itself is signal-only by default — PR8 boundary.)
			PrepareBuyCycles: true,
		})
		base.Log.Info("trade-engine starting", "redis", base.Cfg.Redis.Addr, "mode", base.Cfg.Execution.Mode)
		// Startup live-safety summary (no secrets).
		base.Log.Info("live startup safety", live.BuildSafetySummary(ctx, store.DB(), clock.NewSystem(), base.Cfg.Execution.Mode).LogArgs()...)
		return eng.Run(ctx) // blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "trade-engine: "+err.Error())
		os.Exit(1)
	}
}
