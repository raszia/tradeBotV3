// Package events defines the normalized internal event model and the Redis
// pub/sub envelopes. Two distinct event families live here:
//
//   - Market events published by collectors (price-change / book-update) that the
//     trade-engine consumes from Redis.
//   - The normalized order-event model (PR10): a single internal shape that BOTH
//     exchange WebSocket updates AND polling results are converted into, so the
//     order-status processor has one code path regardless of how an exchange
//     reports (some push via WS, some only answer polls).
//
// Implemented across PR5 (market events) and PR10 (order events). Placeholder
// for the PR1 skeleton.
package events
