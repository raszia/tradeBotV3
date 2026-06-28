// Command health-monitor tracks per-exchange health (PR14): public REST via a
// read-only GetMarkets probe, with latency / error-category / rate-limit / auth /
// timeout classification recorded into exchange_health_current + exchange_health_samples.
//
// It calls ONLY read-only APIs (no PlaceOrder/CancelOrder), makes no trading
// decisions, and creates no cycles/orders. Private (authenticated) health needs
// decrypted credentials — a later PR — so until then private health stays UNKNOWN
// and the binary runs public probes only (never panicking on missing credentials).
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/health"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("health-monitor", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		// Raw API request/response logging (secrets masked centrally) for the probes.
		iolog := exchanges.NewIOLogger(exchanges.IOLogConfig{Enabled: true, Source: "health-monitor"}, store.DB())
		defer iolog.Close()

		var targets []health.Target
		for _, code := range loadEnabledExchanges(ctx, store.DB()) {
			client, err := exchanges.NewPublicClient(exchanges.ClientConfig{Code: code}, iolog)
			if err != nil {
				base.Log.Warn("no public client for exchange; skipping health probe", "exchange", code, "err", err)
				continue
			}
			c := client
			targets = append(targets, health.Target{
				ExchangeCode: code,
				Public:       func(ctx context.Context) error { _, e := c.GetMarkets(ctx); return e },
				Private:      nil, // no credentials yet -> private health stays UNKNOWN
			})
		}

		rec := health.NewRecorder(store.DB())
		mon := health.NewMonitor(rec, targets, clock.NewSystem(), base.Log, health.Config{})
		base.Log.Info("health-monitor booted", "targets", len(targets))
		return mon.Run(ctx) // idles when there are no targets; blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "health-monitor: "+err.Error())
		os.Exit(1)
	}
}

// loadEnabledExchanges returns the codes of enabled exchanges (best-effort; a query
// error yields no targets rather than a crash).
func loadEnabledExchanges(ctx context.Context, sqlDB *sql.DB) []string {
	rows, err := sqlDB.QueryContext(ctx, "SELECT code FROM exchanges WHERE enabled=1")
	if err != nil {
		return nil
	}
	defer rows.Close()
	var codes []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err == nil {
			codes = append(codes, code)
		}
	}
	return codes
}
