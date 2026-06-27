// Command balance-sync continuously polls exchange balances into current and
// history tables with hash-based duplicate prevention. In the PR1 skeleton it
// only boots, verifies the schema, and idles; the sync loop lands in PR13.
package main

import (
	"fmt"
	"os"

	"v3TradeBot/internal/service"
)

func main() {
	if err := service.RunWithDB("balance-sync", service.ConfigFlag(), nil); err != nil {
		fmt.Fprintln(os.Stderr, "balance-sync: "+err.Error())
		os.Exit(1)
	}
}
