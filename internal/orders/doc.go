// Package orders handles order registration and order-event processing. Its hard
// rule: an order is REGISTERED in MariaDB (with an internal local_client_order_id)
// before any exchange request to place it is enqueued — no order is ever sent to
// an exchange that does not already have a database record.
//
// It also turns normalized order events (from WebSocket or polling) into order
// state transitions, fill rows, and fee rows, all through internal/state so the
// lifecycle stays deterministic.
//
// Implemented in PR9/PR10. Placeholder for the PR1 skeleton.
package orders
