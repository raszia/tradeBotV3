// Package queue is the database-backed priority queue for exchange requests
// (PLACE_ORDER, CANCEL_ORDER, GET_ORDER, GET_OPEN_ORDERS, GET_BALANCE, ...).
// The trade-engine ENQUEUES; the order-executor CLAIMS and sends. This decouples
// decision-making from API calls and guarantees a request is persisted before it
// is ever sent.
//
// Status values are fixed for the whole system: QUEUED, CLAIMED, IN_FLIGHT,
// SUCCEEDED, FAILED, RETRY_SCHEDULED, DEAD. Each request carries timeout,
// retry_count/max_retries, next_retry_at, priority and a UNIQUE idempotency_key.
//
// Safety points implemented here in PR7:
//   - Claims respect a per-exchange concurrency limit. Because that limit must
//     hold even across MULTIPLE executor processes, the count+claim is protected
//     at the DB level (per-exchange GET_LOCK / slot leasing); until distributed
//     slot leasing exists, only one executor instance per exchange is supported
//     and that constraint is documented, not assumed away.
//   - A request is marked IN_FLIGHT (committed) before the API call. After a
//     crash, a stuck IN_FLIGHT PLACE/CANCEL is NEVER blindly re-sent: recovery
//     probes the exchange (GetOrder, or identify via recent orders/fills) and
//     escalates to NEEDS_RECONCILE when it cannot positively determine outcome.
//   - On a successful response the queue status and the order state/event are
//     updated in the SAME transaction.
//
// Implemented in PR7. Placeholder for the PR1 skeleton.
package queue
