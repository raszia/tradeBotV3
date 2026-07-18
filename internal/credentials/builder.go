package credentials

import (
	"context"
	"database/sql"
	"time"

	"v3TradeBot/internal/exchanges"
)

// Builder constructs REAL private clients through the existing exchange factory,
// injecting the Provider as the credential source and the per-exchange symbol map from
// the DB. It never hardcodes exchange-specific construction (the factory owns that), so
// an unsupported exchange simply yields no client. Construction performs no network I/O
// (adapters connect lazily on first call), so wiring at boot is safe.
type Builder struct {
	db            *sql.DB
	provider      *Provider
	iolog         *exchanges.IOLogger
	rateLimitSink exchanges.RateLimitSink
}

// NewBuilder builds a Builder around a Provider (the credential source).
func NewBuilder(db *sql.DB, provider *Provider, iolog *exchanges.IOLogger) *Builder {
	return &Builder{db: db, provider: provider, iolog: iolog}
}

// WithRateLimitSink attaches the sink that receives throttle signals seen on SUCCESSFUL
// responses (PR20 correction #6), so exhausted-quota headers on an HTTP 200 pause FUTURE
// requests without disturbing the successful result. Returns the builder for chaining.
func (b *Builder) WithRateLimitSink(sink exchanges.RateLimitSink) *Builder {
	b.rateLimitSink = sink
	return b
}

// BuildPrivate constructs the private client for code via the factory, ONLY when an
// active credential exists for it. Returns ErrNoActiveCredential (no client) otherwise,
// or the factory's Unsupported error when the exchange has no private adapter.
func (b *Builder) BuildPrivate(ctx context.Context, code string) (exchanges.PrivateClient, error) {
	if !b.provider.HasActiveCredential(ctx, code) {
		return nil, ErrNoActiveCredential
	}
	return exchanges.NewPrivateClient(exchanges.ClientConfig{
		Code:           code,
		Creds:          b.provider, // decrypted in memory, on demand, per call
		Symbols:        b.loadSymbols(ctx, code),
		ClientTimeout:  10 * time.Second,
		RequestTimeout: 10 * time.Second,
		RateLimitSink:  b.rateLimitSink,
	}, b.iolog)
}

// fixedCreds is a CredentialProvider that always returns ONE specific decrypted credential,
// regardless of the exchange code asked for. Used by BuildForCredential so validation targets the
// exact credential row an operator selected (PR22 requirement 2), never the auto-selected active one.
type fixedCreds struct{ c exchanges.Credentials }

func (f fixedCreds) Credentials(context.Context, string) (exchanges.Credentials, error) {
	return f.c, nil
}

// BuildForCredential constructs a private client bound to the ONE credential identified by
// credentialID (by decrypting only that row), for READ-ONLY validation. The returned client is a
// full PrivateClient, but validation only ever calls its read path (GetBalances via BalanceReader)
// — it never calls PlaceOrder/CancelOrder. A missing row / unsupported algorithm / decrypt failure
// returns an error (no plaintext).
func (b *Builder) BuildForCredential(ctx context.Context, credentialID int64) (exchanges.PrivateClient, error) {
	code, creds, err := b.provider.CredentialsByID(ctx, credentialID)
	if err != nil {
		return nil, err
	}
	return exchanges.NewPrivateClient(exchanges.ClientConfig{
		Code:           code,
		Creds:          fixedCreds{creds},
		Symbols:        b.loadSymbols(ctx, code),
		ClientTimeout:  10 * time.Second,
		RequestTimeout: 10 * time.Second,
		RateLimitSink:  b.rateLimitSink,
	}, b.iolog)
}

// loadSymbols returns the canonical->venue symbol map for an exchange (empty on error;
// balance/health/reconcile reads do not need it, only order placement does).
func (b *Builder) loadSymbols(ctx context.Context, code string) map[string]string {
	rows, err := b.db.QueryContext(ctx,
		"SELECT em.canonical_symbol, em.exchange_symbol FROM exchange_markets em JOIN exchanges e ON e.id=em.exchange_id WHERE e.code=?",
		code)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var canonical, venue string
		if rows.Scan(&canonical, &venue) == nil {
			out[canonical] = venue
		}
	}
	return out
}

// LiveEnabledCodes returns enabled + live-enabled exchange codes (the order-executor's
// candidate set for live private clients).
func (b *Builder) LiveEnabledCodes(ctx context.Context) []string {
	return b.codes(ctx, "SELECT code FROM exchanges WHERE enabled=1 AND live_enabled=1")
}

// EnabledCodes returns enabled exchange codes (the read-only services' candidate set —
// balance-sync / health / reconciler need credentials regardless of live trading).
func (b *Builder) EnabledCodes(ctx context.Context) []string {
	return b.codes(ctx, "SELECT code FROM exchanges WHERE enabled=1")
}

func (b *Builder) codes(ctx context.Context, q string) []string {
	rows, err := b.db.QueryContext(ctx, q)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if rows.Scan(&c) == nil {
			out = append(out, c)
		}
	}
	return out
}
