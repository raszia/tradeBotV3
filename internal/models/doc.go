// Package models holds the plain Go row structs that mirror the MariaDB tables.
// It is the shared vocabulary between the persistence layer and the services and
// holds NO behaviour (no DB calls, no business logic), so it can be imported
// freely without cycles.
//
// State-typed fields use the enums from internal/state (models depends on state,
// never the reverse). Structs are introduced lean and grown by the PRs that need
// them; PR3 adds Cycle, Order, and StateEvent for the state-machine work.
package models
