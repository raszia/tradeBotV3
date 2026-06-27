// Command reconciler makes the system safe after restart/timeout/partial-fill/
// disconnect by comparing database state with exchange state. In the PR1 skeleton
// it only boots, verifies the schema, and idles; startup reconciliation lands in
// PR12.
package main

import (
	"fmt"
	"os"

	"v3TradeBot/internal/service"
)

func main() {
	if err := service.RunWithDB("reconciler", service.ConfigFlag(), nil); err != nil {
		fmt.Fprintln(os.Stderr, "reconciler: "+err.Error())
		os.Exit(1)
	}
}
