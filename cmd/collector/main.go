// Command collector normalizes Binance and Iranian-exchange market data into
// Redis and publishes price-change events. It uses ONLY public market-data
// clients (no credentials), makes no trading decisions, and places no orders.
// Redis is a cache: on restart the collector reconnects and repopulates it.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/collector"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	redisx "v3TradeBot/internal/redis"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("collector", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		rc, err := redisx.New(ctx, base.Cfg.Redis)
		if err != nil {
			return fmt.Errorf("connect redis: %w", err)
		}
		defer rc.Close()

		// Raw API request/response logging (secrets masked centrally) for the
		// public market-data calls.
		iolog := exchanges.NewIOLogger(exchanges.IOLogConfig{Enabled: true, Source: "collector"}, store.DB())
		defer iolog.Close()

		// The set of (exchange, symbol) to collect is the DB's source of truth.
		markets, err := collector.LoadCollectionMarkets(ctx, store.DB())
		if err != nil {
			return err
		}
		targets, err := collector.BuildTargets(markets, iolog)
		if err != nil {
			return err
		}

		health := collector.NewDBHealthRecorder(store.DB())
		coll := collector.New(targets, rc, health, base.Log, clock.NewSystem(), collector.Config{PollInterval: time.Second})
		base.Log.Info("collector starting", "exchanges", len(targets), "markets", len(markets), "redis", base.Cfg.Redis.Addr)
		return coll.Run(ctx) // blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "collector: "+err.Error())
		os.Exit(1)
	}
}
