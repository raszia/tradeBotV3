// Command order-executor is the ONLY component that issues order-mutating
// exchange API calls. It claims requests from the database-backed queue and
// records responses. In the PR1 skeleton it only boots, verifies the schema, and
// idles; the queue consumer lands in PR7.
package main

import (
	"fmt"
	"os"

	"v3TradeBot/internal/service"
)

func main() {
	if err := service.RunWithDB("order-executor", service.ConfigFlag(), nil); err != nil {
		fmt.Fprintln(os.Stderr, "order-executor: "+err.Error())
		os.Exit(1)
	}
}
