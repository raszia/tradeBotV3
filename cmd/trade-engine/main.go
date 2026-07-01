// Command trade-engine consumes market events and market data, computes the
// (owner-defined) spread/signal, and writes comparison_events / signals. It NEVER calls
// an exchange API (no private clients, no PlaceOrder/CancelOrder) — the order-executor
// is the only sender. In PR9 buy-cycle preparation is ENABLED
// (engine.Config.PrepareBuyCycles = true): on a passing signal for a trading-enabled
// market it prepares the buy transactionally via internal/buyflow (cycle + symbol lock +
// order + QUEUED PLACE_ORDER request) and links the signal to the created cycle. Buyflow
// stays gated behind that flag (the engine library defaults it false — PR8's boundary).
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

		// DryRun / LiveGuard are wired forward but INERT in PR8: they only take effect once
		// buy-cycle preparation is enabled (PR9). DryRun later stamps created cycles for the
		// dashboard/reconciler; in LIVE mode the guard gates new buy-cycle creation. In PR8 no
		// cycle is ever created, so neither has any effect.
		var liveGuard *live.Guard
		if base.Cfg.Execution.IsLive() {
			liveGuard = live.NewGuard(store.DB(), clock.NewSystem(), base.Log)
		}
		eng := engine.New(store, rc, cache, clock.NewSystem(), base.Log, engine.Config{
			DryRun:    base.Cfg.Execution.IsDryRun(),
			LiveGuard: liveGuard,
			// PR9: buy-cycle preparation is ENABLED. On a passing signal for a trading-enabled
			// market the engine prepares the buy transactionally via internal/buyflow (cycle +
			// symbol lock + order + QUEUED PLACE_ORDER request). The engine still never calls an
			// exchange; the order-executor remains the only sender. The buyflow path stays gated
			// behind this flag (the engine library defaults it FALSE — PR8's signal-only boundary).
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
