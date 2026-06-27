// Package regime computes configurable market-regime baskets from Binance market
// data. Baskets (symbols, weights, timeframe windows, thresholds, update
// interval) are configured from the dashboard and stored in MariaDB. The module
// reads Binance books/prices from REDIS (it must NOT call Binance directly), and
// stores a normalized regime result (direction, level, confidence, basket score,
// per-timeframe and per-symbol contributions, config version) as both current
// state and history.
//
// The regime is decoupled from the trade-engine: the engine reads the latest
// computed regime from cache/DB and may let it influence configurable parameters
// (accept/reject, signal score, buy size, min spread, sell offset, ...). Each
// cycle stores a regime snapshot at signal time. The exact formula may start as a
// placeholder; the data model and flow are correct from the start.
//
// Implemented in PR15. Placeholder for the PR1 skeleton.
package regime
