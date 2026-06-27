// Package domain holds the normalized, exchange-agnostic market types
// (OrderBook, Level, SymbolRules, canonical symbol helpers). It is ported and
// adapted from the sibling system's domain package.
//
// "Canonical symbol" is the BASE/QUOTE form used internally; each exchange maps
// its own symbol strings to canonical ones. Quote units may be USDT or IRT/IRR;
// the system tracks both and the per-exchange USDT price.
//
// Implemented in PR4 (exchange abstraction). Placeholder for the PR1 skeleton.
package domain
