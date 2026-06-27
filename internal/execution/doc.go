// Package execution holds the normalized order-execution contracts ported from
// the sibling system: the private-client order models (OrderRequest, OrderAck,
// OrderStatus, Fill) and the sentinel errors (unknown order, rate limited, ack
// timeout, auth failure) that the order-executor and reconciler interpret.
//
// These are the values that flow between the order-executor and the concrete
// exchange clients. Order TYPE and time-in-force are fields on OrderRequest set
// by the (owner-defined) trading logic — this package does not impose IOC or any
// strategy.
//
// Implemented in PR4. Placeholder for the PR1 skeleton.
package execution
