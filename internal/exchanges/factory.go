package exchanges

import (
	"fmt"
	"sort"
	"sync"
)

// The factory is a registry of exchange adapters. Each adapter registers itself
// in an init() with its capability matrix and (optionally) a public and/or
// private constructor. Services build clients by exchange code without importing
// the concrete adapters directly.

// PublicConstructor builds a PublicClient (read-only market data; no credentials).
type PublicConstructor func(cfg ClientConfig, logger *IOLogger) (PublicClient, error)

// PrivateConstructor builds a PrivateClient (authenticated). It requires
// cfg.Creds to be set.
type PrivateConstructor func(cfg ClientConfig, logger *IOLogger) (PrivateClient, error)

// Registration describes one exchange adapter.
type Registration struct {
	Code         string
	Capabilities Capabilities
	NewPublic    PublicConstructor  // nil if the venue has no public adapter here
	NewPrivate   PrivateConstructor // nil if the venue has no private adapter here
}

var (
	regMu    sync.RWMutex
	registry = map[string]Registration{}
)

// Register adds an adapter to the registry. Called from adapter init() funcs.
// Panics on a duplicate or empty code (a programming error caught at startup).
func Register(r Registration) {
	if r.Code == "" {
		panic("exchanges: Register with empty code")
	}
	regMu.Lock()
	defer regMu.Unlock()
	if _, exists := registry[r.Code]; exists {
		panic("exchanges: duplicate registration for " + r.Code)
	}
	registry[r.Code] = r
}

// Lookup returns the registration for an exchange code.
func Lookup(code string) (Registration, bool) {
	regMu.RLock()
	defer regMu.RUnlock()
	r, ok := registry[code]
	return r, ok
}

// All returns every registration sorted by code (used for the capability matrix).
func All() []Registration {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]Registration, 0, len(registry))
	for _, r := range registry {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

// CapabilitiesFor returns the capability matrix for a code without constructing
// a client.
func CapabilitiesFor(code string) (Capabilities, bool) {
	r, ok := Lookup(code)
	if !ok {
		return Capabilities{}, false
	}
	return r.Capabilities, true
}

// NewPublicClient constructs the public adapter for cfg.Code.
func NewPublicClient(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	r, ok := Lookup(cfg.Code)
	if !ok {
		return nil, fmt.Errorf("exchanges: no adapter registered for %q", cfg.Code)
	}
	if r.NewPublic == nil {
		return nil, Unsupported(cfg.Code, "public market-data client")
	}
	return r.NewPublic(cfg, logger)
}

// NewPrivateClient constructs the private adapter for cfg.Code.
func NewPrivateClient(cfg ClientConfig, logger *IOLogger) (PrivateClient, error) {
	r, ok := Lookup(cfg.Code)
	if !ok {
		return nil, fmt.Errorf("exchanges: no adapter registered for %q", cfg.Code)
	}
	if r.NewPrivate == nil {
		return nil, Unsupported(cfg.Code, "private trading client")
	}
	if cfg.Creds == nil {
		return nil, fmt.Errorf("exchanges: %s private client requires credentials", cfg.Code)
	}
	return r.NewPrivate(cfg, logger)
}
