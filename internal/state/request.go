package state

// RequestStatus is the status of an exchange_requests queue row. The constants
// are defined here so the whole system (queue code in PR7, dashboard, tests)
// shares ONE source of truth that matches the database ENUM exactly. The queue's
// own status transition rules live with the queue implementation (PR7); this file
// only fixes the vocabulary for consistency.
type RequestStatus string

const (
	RequestQueued         RequestStatus = "QUEUED"
	RequestClaimed        RequestStatus = "CLAIMED"
	RequestInFlight       RequestStatus = "IN_FLIGHT"
	RequestSucceeded      RequestStatus = "SUCCEEDED"
	RequestFailed         RequestStatus = "FAILED"
	RequestRetryScheduled RequestStatus = "RETRY_SCHEDULED"
	RequestDead           RequestStatus = "DEAD"
)

// AllRequestStatuses lists every queue status, in the same order as the database
// ENUM definition (see migration 005). A test asserts this stays in sync.
var AllRequestStatuses = []RequestStatus{
	RequestQueued, RequestClaimed, RequestInFlight, RequestSucceeded,
	RequestFailed, RequestRetryScheduled, RequestDead,
}

// terminalRequestStatuses are the queue end states (no further processing).
var terminalRequestStatuses = map[RequestStatus]bool{
	RequestSucceeded: true,
	RequestDead:      true,
}

// IsTerminalRequest reports whether a queue row is in a terminal status.
func IsTerminalRequest(s RequestStatus) bool { return terminalRequestStatuses[s] }
