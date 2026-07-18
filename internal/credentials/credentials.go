// Package credentials loads encrypted exchange API credentials from
// exchange_credentials, decrypts them ONLY in memory with the bootstrap master key,
// and serves them as an exchanges.CredentialProvider so the existing factory can build
// real private clients (PR20a). It never writes plaintext back, never logs plaintext,
// never returns plaintext in errors, and never exposes the encrypted blob. A missing or
// invalid master key disables credential loading safely (no panic, no live execution),
// and any decryption/algorithm failure marks the credential unusable instead of guessing.
package credentials

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/secrets"
)

var (
	// ErrNoActiveCredential means no enabled, active credential exists for the exchange.
	ErrNoActiveCredential = errors.New("credentials: no active credential for exchange")
)

// BalanceReader is the narrow read-only surface the credential validator uses: only a
// balance read, so validation can NEVER place or cancel an order. The real
// exchanges.PrivateClient satisfies it.
type BalanceReader interface {
	GetBalances(ctx context.Context) ([]domain.Balance, error)
}

// Provider reads + decrypts credentials on demand. It is safe for concurrent use.
type Provider struct {
	db     *sql.DB
	cipher *secrets.Cipher
	clk    clock.Clock
	log    *slog.Logger
}

// NewProvider builds a Provider. An empty master key returns secrets.ErrNoMasterKey so
// the caller can disable credential loading + live execution safely (it never panics).
func NewProvider(db *sql.DB, masterKey string, clk clock.Clock, log *slog.Logger) (*Provider, error) {
	cipher, err := secrets.NewCipher(masterKey) // ErrNoMasterKey on empty key
	if err != nil {
		return nil, err
	}
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Provider{db: db, cipher: cipher, clk: clk, log: log}, nil
}

// Credentials implements exchanges.CredentialProvider. It selects the single clearly-
// chosen credential — enabled AND status='active', highest key_version (then newest id)
// — for the exchange code, then decrypts each field in memory. Disabled/non-active/
// other-version rows are ignored. On unsupported algorithm or decryption failure it
// marks the row unusable (status='error', a non-secret note) and returns an error that
// carries no plaintext.
func (p *Provider) Credentials(ctx context.Context, code string) (exchanges.Credentials, error) {
	var (
		id             int64
		encKey, encSec []byte
		encPass        []byte
		algorithm      string
		keyVersion     int
	)
	err := p.db.QueryRowContext(ctx, `
SELECT ec.id, ec.encrypted_api_key, ec.encrypted_api_secret, ec.encrypted_passphrase, ec.encryption_algorithm, ec.key_version
FROM exchange_credentials ec
JOIN exchanges e ON e.id = ec.exchange_id
WHERE e.code = ? AND ec.enabled = 1 AND ec.status = 'active'
ORDER BY ec.key_version DESC, ec.id DESC
LIMIT 1`, code).Scan(&id, &encKey, &encSec, &encPass, &algorithm, &keyVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return exchanges.Credentials{}, ErrNoActiveCredential
	}
	if err != nil {
		return exchanges.Credentials{}, fmt.Errorf("credentials: load %s: %w", code, err)
	}

	if !secrets.SupportedAlgorithm(algorithm) {
		p.markUnusable(ctx, id, "unsupported encryption algorithm")
		return exchanges.Credentials{}, secrets.ErrUnsupportedAlgorithm
	}

	apiKey, err := p.decryptField(encKey)
	if err != nil {
		p.markUnusable(ctx, id, "decryption failed")
		return exchanges.Credentials{}, err
	}
	apiSecret, err := p.decryptField(encSec)
	if err != nil {
		p.markUnusable(ctx, id, "decryption failed")
		return exchanges.Credentials{}, err
	}
	passphrase, err := p.decryptField(encPass)
	if err != nil {
		p.markUnusable(ctx, id, "decryption failed")
		return exchanges.Credentials{}, err
	}
	// key_version is respected by selection (highest active) and travels with the row;
	// rotation just inserts a higher-version active credential and disables the old one.
	return exchanges.Credentials{APIKey: apiKey, APISecret: apiSecret, Passphrase: passphrase}, nil
}

// CredentialsByID decrypts the ONE credential row identified by credentialID (regardless of its
// enabled/status), returning the exchange code and the decrypted secrets. It exists so validation
// can check the EXACT credential an operator selected — NOT the auto-selected active one (PR22
// requirement 2). Errors carry no plaintext; an unsupported algorithm / decrypt failure marks the
// row unusable and returns an error.
func (p *Provider) CredentialsByID(ctx context.Context, credentialID int64) (string, exchanges.Credentials, error) {
	var (
		code, algorithm string
		encKey, encSec  []byte
		encPass         []byte
	)
	err := p.db.QueryRowContext(ctx, `
SELECT e.code, ec.encrypted_api_key, ec.encrypted_api_secret, ec.encrypted_passphrase, ec.encryption_algorithm
FROM exchange_credentials ec JOIN exchanges e ON e.id = ec.exchange_id
WHERE ec.id = ?`, credentialID).Scan(&code, &encKey, &encSec, &encPass, &algorithm)
	if errors.Is(err, sql.ErrNoRows) {
		return "", exchanges.Credentials{}, fmt.Errorf("credentials: credential %d not found", credentialID)
	}
	if err != nil {
		return "", exchanges.Credentials{}, fmt.Errorf("credentials: load id %d: %w", credentialID, err)
	}
	if !secrets.SupportedAlgorithm(algorithm) {
		p.markUnusable(ctx, credentialID, "unsupported encryption algorithm")
		return "", exchanges.Credentials{}, secrets.ErrUnsupportedAlgorithm
	}
	apiKey, err := p.decryptField(encKey)
	if err != nil {
		p.markUnusable(ctx, credentialID, "decryption failed")
		return "", exchanges.Credentials{}, err
	}
	apiSecret, err := p.decryptField(encSec)
	if err != nil {
		p.markUnusable(ctx, credentialID, "decryption failed")
		return "", exchanges.Credentials{}, err
	}
	passphrase, err := p.decryptField(encPass)
	if err != nil {
		p.markUnusable(ctx, credentialID, "decryption failed")
		return "", exchanges.Credentials{}, err
	}
	return code, exchanges.Credentials{APIKey: apiKey, APISecret: apiSecret, Passphrase: passphrase}, nil
}

// IsDefiniteAuthError reports whether err is a DEFINITE authentication/authorization failure — the
// only class allowed to invalidate a credential (PR22 requirement 2). Transient errors (timeout,
// network, rate limit, 5xx) return false so a brief incident never permanently invalidates a good
// credential.
func IsDefiniteAuthError(err error) bool { return isDefiniteAuthError(err) }

// decryptField returns "" for a NULL/empty blob (e.g. venues with no passphrase) and
// the decrypted plaintext otherwise. Errors carry no sensitive data.
func (p *Provider) decryptField(blob []byte) (string, error) {
	if len(blob) == 0 {
		return "", nil
	}
	pt, err := p.cipher.Decrypt(blob)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

// markUnusable best-effort flags a credential row as errored with a NON-SECRET note so
// the guard/health stop using it. It never writes plaintext and never panics.
func (p *Provider) markUnusable(ctx context.Context, id int64, reason string) {
	if _, err := p.db.ExecContext(ctx,
		"UPDATE exchange_credentials SET status='error', last_auth_error=?, last_checked_at=? WHERE id=?",
		reason, p.clk.Now().UTC(), id); err != nil {
		p.log.Warn("credentials: mark unusable failed", "id", id, "err", err)
	}
}

// Validate performs the ONLY allowed MANUAL credential check: a read-only balance read.
// It can never place or cancel (the BalanceReader interface lacks those). It is STRICT —
// an operator explicitly asked "is this credential good right now?" — so ANY error stamps
// status='invalid'. This strictness is correct for a one-shot, operator-initiated check
// but is UNSAFE for continuous polling: use ProbePrivateHealth for the health-monitor.
// On success it stamps status='active' + last_checked_at; on failure status='invalid'
// with a non-secret note. Returns the underlying error (which must not contain secrets).
func (p *Provider) Validate(ctx context.Context, code string, r BalanceReader) error {
	_, err := r.GetBalances(ctx)
	if err != nil {
		p.setStatusByCode(ctx, code, "invalid", "balance validation failed")
		return err
	}
	p.setStatusByCode(ctx, code, "active", "")
	return nil
}

// ProbePrivateHealth is the CONTINUOUS-monitoring credential probe used by the
// health-monitor. Like Validate it performs ONLY a read-only balance read (it can never
// place or cancel), but its credential-status policy is SAFE FOR REPEATED USE: it marks a
// credential 'invalid' ONLY on a DEFINITE auth/permission/signature error. A temporary
// exchange/network problem (timeout, deadline, network reset, rate limit / HTTP 429,
// exchange 5xx, service unavailable) leaves the credential 'active' and untouched, so a
// short incident can never permanently disable a valid credential — it surfaces only as a
// health status (UNAVAILABLE/DEGRADED/RATE_LIMITED via health.Classify). It always returns
// the underlying error unchanged so the health layer classifies it correctly.
//
//   - success              → status='active', last_auth_error cleared, nil error
//   - definite auth error  → status='invalid' (non-secret note), returns the error
//   - temporary/other error→ credential row UNTOUCHED (stays whatever it was), returns err
func (p *Provider) ProbePrivateHealth(ctx context.Context, code string, r BalanceReader) error {
	_, err := r.GetBalances(ctx)
	if err == nil {
		p.setStatusByCode(ctx, code, "active", "")
		return nil
	}
	if isDefiniteAuthError(err) {
		p.setStatusByCode(ctx, code, "invalid", "auth error on private health probe")
	}
	// Any non-auth error: do NOT modify credential status. A transient failure must never
	// invalidate a healthy credential (that would let a brief outage disable live trading).
	return err
}

// isDefiniteAuthError reports whether err is a DEFINITE authentication/authorization
// failure — the only class of error allowed to invalidate a credential. It mirrors the
// auth arm of health.Classify: the execution auth sentinel and a NormalizedAPIError whose
// adapter-assigned category is auth (invalid key/secret, unauthorized, permission denied,
// signature error). Everything else — timeout, network, rate limit, 5xx, unknown — is
// treated as NON-auth (fail-safe: when unsure, do not invalidate).
func isDefiniteAuthError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, execution.ErrAuthFailed) {
		return true
	}
	var apiErr *exchanges.NormalizedAPIError
	if errors.As(err, &apiErr) {
		return apiErr.Category == exchanges.CatAuth
	}
	return false
}

func (p *Provider) setStatusByCode(ctx context.Context, code, status, note string) {
	var noteVal any
	if note == "" {
		noteVal = nil
	} else {
		noteVal = note
	}
	if _, err := p.db.ExecContext(ctx, `
UPDATE exchange_credentials ec JOIN exchanges e ON e.id = ec.exchange_id
SET ec.status=?, ec.last_auth_error=?, ec.last_checked_at=?
WHERE e.code=? AND ec.enabled=1`,
		status, noteVal, p.clk.Now().UTC(), code); err != nil {
		p.log.Warn("credentials: set status failed", "code", code, "err", err)
	}
}

// HasActiveCredential reports whether an enabled, active credential row exists for the
// exchange (without decrypting). Used by wiring to decide whether to build a live client.
func (p *Provider) HasActiveCredential(ctx context.Context, code string) bool {
	var n int
	p.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM exchange_credentials ec JOIN exchanges e ON e.id=ec.exchange_id WHERE e.code=? AND ec.enabled=1 AND ec.status='active'",
		code).Scan(&n)
	return n > 0
}
