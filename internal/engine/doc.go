// Package engine is the trade-engine: it consumes price events, reads the latest
// Binance and Iranian books/prices from Redis, reads config and regime from an
// in-memory cache, computes spread and fee-adjusted spread, checks the symbol
// lock, writes comparison_events/signals, and — when a signal passes — creates a
// cycle, acquires the lock, registers the initial order, and ENQUEUES the buy
// request, all transactionally.
//
// HARD RULE: the engine never calls an exchange API. It only writes to MariaDB
// (and reads Redis/cache). Actual API calls are made exclusively by the
// order-executor reading the queue.
//
// Implemented in PR8/PR9/PR11. Placeholder for the PR1 skeleton.
package engine
