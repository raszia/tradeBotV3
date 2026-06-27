// Command trade-engine consumes price events and market data, evaluates the
// (owner-defined) signal, and enqueues exchange requests. It NEVER calls an
// exchange API directly. In the PR1 skeleton it only boots, verifies the schema,
// and idles; the event loop lands in PR8+.
package main

import (
	"fmt"
	"os"

	"v3TradeBot/internal/service"
)

func main() {
	if err := service.RunWithDB("trade-engine", service.ConfigFlag(), nil); err != nil {
		fmt.Fprintln(os.Stderr, "trade-engine: "+err.Error())
		os.Exit(1)
	}
}
