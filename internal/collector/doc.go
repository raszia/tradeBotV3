// Package collector normalizes market data from Binance and the Iranian
// exchanges and writes it to Redis (latest order books, latest prices) while
// publishing price-change/book-update events. It records its own health.
//
// The collector makes NO trading decisions and places NO orders — it only feeds
// the Redis cache that the trade-engine and regime module read. Redis is a cache
// only; the collector never writes authoritative state.
//
// Implemented in PR5. Placeholder for the PR1 skeleton.
package collector
