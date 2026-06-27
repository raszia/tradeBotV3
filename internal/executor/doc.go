// Package executor is the order-executor service: the ONLY component that issues
// order-mutating exchange API calls. It claims requests from internal/queue
// respecting per-exchange concurrency, sends them via internal/exchanges, writes
// raw (secret-masked) API logs, records responses, and updates order/cycle state
// and the queue row transactionally.
//
// It enforces the crash-after-send safety rule: mark IN_FLIGHT (committed) before
// calling the API; on recovery never blindly re-send a PLACE/CANCEL — probe the
// exchange and escalate to NEEDS_RECONCILE when the outcome cannot be proven.
//
// Implemented in PR7 (foundation) and PR10/PR11. Placeholder for the PR1 skeleton.
package executor
