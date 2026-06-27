// Package models holds the plain Go row structs that mirror the MariaDB tables
// (Cycle, Order, Fill, ExchangeRequest, SymbolLock, Balance, Signal,
// ComparisonEvent, ConfigVersion, ...) plus their column/enum string constants.
//
// It is the shared vocabulary between the persistence layer and the services.
// Keeping models free of behaviour (no DB calls, no business logic) avoids
// import cycles: the state machine (internal/state), queue (internal/queue) and
// repositories all depend on models, not the reverse.
//
// Implemented in PR2/PR3 alongside the schema. This file is a placeholder so the
// package exists in the PR1 skeleton.
package models
