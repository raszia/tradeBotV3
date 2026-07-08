// Command health-monitor tracks per-exchange health (PR14): public REST via a
// read-only GetMarkets probe, with latency / error-category / rate-limit / auth /
// timeout classification recorded into exchange_health_current + exchange_health_samples.
//
// It calls ONLY read-only APIs (no PlaceOrder/CancelOrder), makes no trading
// decisions, and creates no cycles/orders.
//
// Private health is wired (Option A): the authenticated probe is a READ-ONLY balance
// check via credentials.ProbePrivateHealth, which takes the narrow read-only
// BalanceReader — the same read-only credential-building path balance-sync uses (PR13).
// No place/cancel is reachable through it. Crucially, ProbePrivateHealth is the
// CONTINUOUS-monitoring path: it marks a credential 'invalid' ONLY on a definite auth
// error, so a temporary exchange/network incident (timeout/rate-limit/5xx) can never
// invalidate a healthy credential — it only affects the reported health status. A
// missing/invalid master key or an exchange with no active credential falls back to
// public-only: private health for that exchange stays UNKNOWN (a deliberate safe
// fallback, not an accidental omission) and the monitor never panics.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/credentials"
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

		// Optional credential provider for the read-only private probe (no/invalid
		// master key → no private probes, public-only).
		var builder *credentials.Builder
		var provider *credentials.Provider
		if p, perr := credentials.NewProvider(store.DB(), base.Cfg.Security.MasterKey, clock.NewSystem(), base.Log); perr != nil {
			base.Log.Warn("health-monitor: private probes disabled (no/invalid master key); public-only", "err", perr)
		} else {
			provider = p
			builder = credentials.NewBuilder(store.DB(), provider, iolog)
		}

		var targets []health.Target
		for _, code := range loadEnabledExchanges(ctx, store.DB()) {
			client, err := exchanges.NewPublicClient(exchanges.ClientConfig{Code: code}, iolog)
			if err != nil {
				base.Log.Warn("no public client for exchange; skipping health probe", "exchange", code, "err", err)
				continue
			}
			c := client
			t := health.Target{
				ExchangeCode: code,
				Public:       func(ctx context.Context) error { _, e := c.GetMarkets(ctx); return e },
				// Private is wired below when a decrypted credential exists; it stays nil
				// (⇒ private health UNKNOWN) only as the deliberate public-only fallback.
				Private: nil,
			}
			// Option A: wire a READ-ONLY authenticated probe. ProbePrivateHealth takes the
			// narrow read-only BalanceReader (GetBalances only) — no place/cancel is
			// reachable. It is the CONTINUOUS-monitoring path: it only marks a credential
			// 'invalid' on a DEFINITE auth error, so a temporary exchange/network incident
			// (timeout/rate-limit/5xx) never disables a valid credential — it surfaces only
			// as private health status. (The strict Provider.Validate is for one-shot
			// operator-initiated checks, not for this loop.)
			if builder != nil {
				if pc, perr := builder.BuildPrivate(ctx, code); perr == nil {
					ec, pv := code, provider
					rc := pc
					t.Private = func(ctx context.Context) error { return pv.ProbePrivateHealth(ctx, ec, rc) }
				}
			}
			targets = append(targets, t)
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
