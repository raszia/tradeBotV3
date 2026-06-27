// Command health-monitor tracks per-exchange REST/WebSocket availability,
// latency, and error/auth/rate-limit counts. In the PR1 skeleton it only boots,
// verifies the schema, and idles; the probes land in PR14.
package main

import (
	"fmt"
	"os"

	"v3TradeBot/internal/service"
)

func main() {
	if err := service.RunWithDB("health-monitor", service.ConfigFlag(), nil); err != nil {
		fmt.Fprintln(os.Stderr, "health-monitor: "+err.Error())
		os.Exit(1)
	}
}
