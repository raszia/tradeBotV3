// Command collector normalizes Binance and Iranian-exchange market data into
// Redis and publishes price-change events. It makes no trading decisions and
// places no orders. In the PR1 skeleton it boots, verifies the schema, connects
// Redis, and idles; market-data ingestion lands in PR5.
package main

import (
	"context"
	"fmt"
	"os"

	"v3TradeBot/internal/db"
	redisx "v3TradeBot/internal/redis"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("collector", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		// Collector additionally needs Redis (the live-market-data cache). Redis
		// is never the source of truth: losing it must not lose trading state.
		rc, err := redisx.New(ctx, base.Cfg.Redis)
		if err != nil {
			return fmt.Errorf("connect redis: %w", err)
		}
		defer rc.Close()

		base.Log.Info("collector ready (market-data wiring lands in PR5)", "redis", base.Cfg.Redis.Addr)
		<-ctx.Done()
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "collector: "+err.Error())
		os.Exit(1)
	}
}
