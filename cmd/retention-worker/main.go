// Command retention-worker cleans up high-volume tables (api_call_logs,
// comparison_events, exchange_health_samples, app_logs, market_regime_history)
// using batched/partition-aware deletes. In the PR1 skeleton it only boots,
// verifies the schema, and idles; the cleanup loops land in PR18.
package main

import (
	"fmt"
	"os"

	"v3TradeBot/internal/service"
)

func main() {
	if err := service.RunWithDB("retention-worker", service.ConfigFlag(), nil); err != nil {
		fmt.Fprintln(os.Stderr, "retention-worker: "+err.Error())
		os.Exit(1)
	}
}
