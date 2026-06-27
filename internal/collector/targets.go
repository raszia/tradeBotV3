package collector

import (
	"context"
	"database/sql"
	"fmt"

	"v3TradeBot/internal/exchanges"
)

// CollectionMarket is one (exchange, symbol) the collector should ingest, read
// from the DB. The set of markets to collect is the source of truth (the
// dashboard toggles enabled_for_collection on exchange_markets); Redis only
// caches the resulting data.
type CollectionMarket struct {
	ExchangeCode    string
	CanonicalSymbol string
	ExchangeSymbol  string
}

// LoadCollectionMarkets returns the markets enabled for collection on enabled
// exchanges (exchange_markets.enabled_for_collection = 1 AND exchanges.enabled).
func LoadCollectionMarkets(ctx context.Context, db *sql.DB) ([]CollectionMarket, error) {
	const q = `
		SELECT e.code, em.canonical_symbol, em.exchange_symbol
		FROM exchange_markets em
		JOIN exchanges e ON e.id = em.exchange_id
		WHERE em.enabled_for_collection = 1 AND e.enabled = 1
		ORDER BY e.code, em.canonical_symbol`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("collector: load collection markets: %w", err)
	}
	defer rows.Close()
	var out []CollectionMarket
	for rows.Next() {
		var m CollectionMarket
		if err := rows.Scan(&m.ExchangeCode, &m.CanonicalSymbol, &m.ExchangeSymbol); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// BuildTargets groups markets by exchange and constructs a PublicClient for each
// via the exchange factory (using each adapter's default URLs; no credentials —
// these are PUBLIC clients only). Exchanges without a registered public adapter
// are skipped with no error so one missing adapter doesn't stop the rest.
func BuildTargets(markets []CollectionMarket, logger *exchanges.IOLogger) ([]Target, error) {
	type group struct {
		symbols []string
		mapping map[string]string // canonical -> venue
	}
	byExchange := map[string]*group{}
	order := []string{}
	for _, m := range markets {
		g, ok := byExchange[m.ExchangeCode]
		if !ok {
			g = &group{mapping: map[string]string{}}
			byExchange[m.ExchangeCode] = g
			order = append(order, m.ExchangeCode)
		}
		if _, seen := g.mapping[m.CanonicalSymbol]; !seen {
			g.symbols = append(g.symbols, m.CanonicalSymbol)
			g.mapping[m.CanonicalSymbol] = m.ExchangeSymbol
		}
	}

	var targets []Target
	for _, code := range order {
		g := byExchange[code]
		reg, ok := exchanges.Lookup(code)
		if !ok || reg.NewPublic == nil {
			// No public adapter for this exchange code; skip (do not fail the fleet).
			continue
		}
		client, err := exchanges.NewPublicClient(exchanges.ClientConfig{Code: code, Symbols: g.mapping}, logger)
		if err != nil {
			return nil, fmt.Errorf("collector: build public client for %s: %w", code, err)
		}
		targets = append(targets, Target{ExchangeCode: code, Client: client, Symbols: g.symbols})
	}
	return targets, nil
}
