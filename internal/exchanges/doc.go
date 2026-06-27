// Package exchanges is the exchange integration layer: concrete clients for
// Binance (public) and the Iranian exchanges (Nobitex/Wallex/Bitpin private,
// Ramzinex/Tabdeal/Exir public), a factory, the masked request/response I/O
// logger, and market-rules fetchers. It is ported and adapted from the sibling
// system's exchange package.
//
// A normalized interface (GetMarkets, GetBalances, PlaceOrder, CancelOrder,
// GetOrder, GetOpenOrders, SubscribeOrderBook, SubscribeOrderUpdates) hides
// per-exchange differences. CRITICAL: real exchange ORDER calls are made ONLY by
// the order-executor (and read-only GET_* calls by the reconciler/balance-sync/
// health-monitor); the trade-engine never calls an exchange.
//
// Not every Iranian exchange supports client-provided order IDs, so the system
// always keeps an internal local_client_order_id and passes the idempotency key
// to the exchange only when supported; recovery after a crash-after-send is
// otherwise conservative (identify via recent orders/fills, else NEEDS_RECONCILE).
//
// Implemented in PR4. Placeholder for the PR1 skeleton.
package exchanges
