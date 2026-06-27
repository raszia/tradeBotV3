// Package health tracks per-exchange health: API key status, REST availability,
// WebSocket availability, latency, timeout/error counts, last successful and
// last failed request, rate-limit errors and authentication errors. It maintains
// a current-state row and high-volume health samples (subject to retention), both
// surfaced in the dashboard.
//
// Implemented in PR14. Placeholder for the PR1 skeleton.
package health
