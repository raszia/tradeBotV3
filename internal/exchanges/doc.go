// Package exchanges is the exchange integration layer. It defines the normalized,
// exchange-agnostic interfaces the rest of the system codes against and the
// concrete per-venue adapters (ported & adapted from the sibling system).
//
// Two interfaces keep market data and trading separate (so the collector never
// needs credentials): PublicClient (GetMarkets / GetOrderBook / SubscribeOrderBook)
// and PrivateClient (GetBalances / PlaceOrder / CancelOrder / GetOrder /
// GetOpenOrders / SubscribeOrderUpdates). Adapters self-register in init() via the
// factory with a Capabilities matrix; unsupported operations return a typed
// ErrUnsupported.
//
// CRITICAL: real order-mutating calls are made ONLY by the order-executor (and
// read-only GET_* by reconciler/balance-sync/health-monitor); the trade-engine
// never calls an exchange. Order type / time-in-force are request fields set by
// the owner's logic — this layer imposes no strategy. All raw API request/response
// logging is secret-masked at a single choke point (mask.go + iolog.go) before it
// reaches api_call_logs.
//
// Adapters: binance (public price reference), nobitex/wallex/bitpin (public +
// private), ramzinex/tabdeal/exir (public price sources). See PROJECT_ARCHITECTURE.md
// §12 for the capability matrix and per-venue limitations. WebSocket subscriptions
// for the Iranian venues are deferred (polling is the supported path).
package exchanges
