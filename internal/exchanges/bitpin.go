package exchanges

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/execution"
)

// credFingerprint is a NON-SECRET, non-reversible fingerprint of a credential (a truncated SHA-256
// of its fields). It is used ONLY to detect that the active credential changed (a rotation) so a
// cached auth token can be invalidated; it is never logged and never sent anywhere.
func credFingerprint(c Credentials) string {
	h := sha256.Sum256([]byte(c.APIKey + "\x00" + c.APISecret + "\x00" + c.Passphrase))
	return hex.EncodeToString(h[:8])
}

// Bitpin adapter — public market data + private trading. Ported/adapted from the
// sibling iranArb bitpin package. Bitpin is an Iranian exchange that quotes in
// toman (IRT); this layer treats IRT as the canonical quote unit (priceMultiplier
// = 1, no rial→toman rescale needed because the REST orderbook is already toman).
//
// Capabilities: GetMarkets (mkt/markets), GetOrderBook (REST mth/orderbook),
// GetBalances, PlaceOrder, CancelOrder, GetOrder, GetOpenOrders. Order status is
// POLLING-ONLY here — Bitpin's order push feed is a Centrifugo WebSocket that is
// deferred (OrderBookWS / OrderUpdatesWS both false; OrderStatusPoll true).
//
// AUTH — JWT access/refresh token flow:
//   - There is no per-request HMAC signature. Instead the adapter exchanges the
//     api_key/secret_key pair for a short-lived JWT access token via
//     POST /api/v1/usr/authenticate/ ({"api_key","secret_key"} -> {"access","refresh"}).
//   - Every authenticated request sends "Authorization: Bearer <access>".
//   - The access token lives ~15 minutes on Bitpin's side; we cache it for 14
//     minutes (bitpinTokenTTL) to keep a 1-minute safety margin, then refresh.
//     Refresh prefers POST /api/v1/usr/refresh_token/ ({"refresh"} -> {"access"})
//     when we hold a refresh token, otherwise re-authenticates from scratch.
//
// Known limitations / quirks:
//   - RATE-LIMIT SENSITIVE. Bitpin aggregates auth + order + wallet calls under a
//     tight RPM budget and returns HTTP 429 ("Request was throttled. Expected
//     available in N seconds.") aggressively — including on the auth endpoints. We
//     honor that: a 429 on the token endpoint arms bitpinAuthThrottledUntil and
//     subsequent callers reuse the cached (slightly-stale, still-valid) token
//     rather than hammering auth. Callers should not place orders in tight loops.
//   - Symbol form is UNDERSCORE: canonical "BTC/IRT" -> venue "BTC_IRT".
//   - Status is POLLING-ONLY (no order WebSocket here): SubscribeOrderUpdates
//     returns ErrUnsupported; use GetOrder / GetOpenOrders.
//   - ClientOrderID: Bitpin accepts an "identifier" field on placement; we pass
//     req.ClientOrderID as the identifier (ClientOrderID capability = true). Cancel
//     and GetOrder accept EITHER a numeric exchange order id OR the identifier
//     string (different URL forms — see CancelOrder/GetOrder).
//   - Error format: non-2xx responses carry a JSON body (often {"detail":"..."}).
//     We surface non-2xx as *NormalizedAPIError + classifyHTTPStatus and wrap the
//     execution sentinels where classifiable (auth/rate-limit/not-found).
//   - NO forced IOC/market/post-only: PlaceOrder passes req.OrderType straight
//     through as the Bitpin "type" and never injects a time-in-force.

const (
	bitpinCode        = "bitpin"
	bitpinDefaultREST = "https://api.bitpin.ir"

	// bitpinTokenTTL is how long a cached access token is treated as fresh
	// before a refresh. Bitpin tokens last ~15m; 14m keeps a 1m margin.
	bitpinTokenTTL = 14 * time.Minute

	// bitpinDefaultThrottle is the fallback backoff when a 429 from the auth
	// endpoint carries no parseable Retry-After / body hint.
	bitpinDefaultThrottle = 30 * time.Second
)

func init() {
	Register(Registration{
		Code: bitpinCode,
		Capabilities: Capabilities{
			MarketMetadata:        true,
			OrderBookREST:         true,
			OrderBookWS:           false, // Centrifugo WS deferred
			BalanceFetch:          true,
			PlaceOrder:            true,
			CancelByOrderID:       true,
			FetchByOrderID:        true,
			FetchOpenOrders:       true,
			RecentFills:           false, // GetRecentFills not implemented in this adapter
			OrderUpdatesWS:        false, // order push feed deferred
			OrderStatusPoll:       true,
			ClientOrderID:         true, // Bitpin accepts an "identifier" field on placement
			LookupByClientOrderID: true, // GET /odr/orders/?identifier=<id> resolves by client id
		},
		NewPublic:  newBitpinPublic,
		NewPrivate: newBitpinPrivate,
	})
}

// bitpinCapabilities returns the registered capability matrix.
func bitpinCapabilities() Capabilities {
	r, _ := Lookup(bitpinCode)
	return r.Capabilities
}

// ─── Public client ─────────────────────────────────────────────────────────

type bitpinPublic struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
}

func newBitpinPublic(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	cfg.Code = bitpinCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = bitpinDefaultREST
	}
	return &bitpinPublic{cfg: cfg, http: hc, restURL: strings.TrimRight(restURL, "/")}, nil
}

func (b *bitpinPublic) Name() string               { return bitpinCode }
func (b *bitpinPublic) Capabilities() Capabilities { return bitpinCapabilities() }

// --- GetMarkets ---

// bitpinMarketSpec mirrors one entry of /api/v1/mkt/markets/. Price/qty precision
// give us tick/step (10^-precision); min notional/qty are not exposed.
type bitpinMarketSpec struct {
	Symbol              string `json:"symbol"` // e.g. "BTC_IRT"
	Code                string `json:"code"`
	Base                string `json:"base"`
	Quote               string `json:"quote"`
	Tradable            bool   `json:"tradable"`
	Suspended           bool   `json:"suspended"`
	PricePrecision      int32  `json:"price_precision"`
	BaseAmountPrecision int32  `json:"base_amount_precision"`
}

func (b *bitpinPublic) GetMarkets(ctx context.Context) ([]NormalizedMarket, error) {
	ctx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()

	raw, err := bitpinGet(ctx, b.http, b.restURL, "/api/v1/mkt/markets/")
	if err != nil {
		return nil, err
	}
	var markets []bitpinMarketSpec
	if err := json.Unmarshal(raw, &markets); err != nil {
		return nil, fmt.Errorf("bitpin markets decode: %w", err)
	}

	out := make([]NormalizedMarket, 0, len(markets))
	for _, m := range markets {
		base := strings.ToUpper(strings.TrimSpace(m.Base))
		quote := strings.ToUpper(strings.TrimSpace(m.Quote))
		venueSym := firstNonEmptyBitpin(m.Symbol, m.Code)
		if (base == "" || quote == "") && venueSym != "" {
			if bb, qq, ok := strings.Cut(venueSym, "_"); ok {
				base, quote = strings.ToUpper(bb), strings.ToUpper(qq)
			}
		}
		if base == "" || quote == "" {
			continue
		}
		// Canonical normalizes IRR->IRT; Bitpin quotes toman so IRT stays IRT.
		canonical := base + "/" + quote
		if norm, err := domain.NormalizeSymbol(canonical); err == nil {
			canonical = norm
		}
		m2 := NormalizedMarket{
			Exchange:        bitpinCode,
			ExchangeSymbol:  strings.ToUpper(firstNonEmptyBitpin(venueSym, base+"_"+quote)),
			CanonicalSymbol: canonical,
			BaseAsset:       base,
			QuoteAsset:      quote,
			QuoteAssetType:  domain.QuoteAssetType(canonical),
			Tradable:        m.Tradable && !m.Suspended,
		}
		if m.PricePrecision >= 0 {
			m2.PricePrecision = int(m.PricePrecision)
			m2.TickSize = decimal.New(1, -m.PricePrecision) // 10^-price_precision
		}
		if m.BaseAmountPrecision >= 0 {
			m2.QuantityPrecision = int(m.BaseAmountPrecision)
			m2.StepSize = decimal.New(1, -m.BaseAmountPrecision) // 10^-base_amount_precision
		}
		out = append(out, m2)
	}
	return out, nil
}

// --- GetOrderBook (REST) ---

func (b *bitpinPublic) GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	venueSym, ok := VenueSymbol(b.cfg, symbol)
	if !ok {
		venueSym = bitpinNativeSymbol(symbol)
	} else {
		venueSym = bitpinNativeSymbol(venueSym)
	}
	canonical, _ := domain.NormalizeSymbol(symbol)
	if canonical == "" {
		canonical = strings.ToUpper(symbol)
	}

	ctx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()
	raw, err := bitpinGet(ctx, b.http, b.restURL, "/api/v1/mth/orderbook/"+url.PathEscape(venueSym)+"/")
	if err != nil {
		return domain.OrderBook{}, err
	}
	var d struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return domain.OrderBook{}, fmt.Errorf("bitpin orderbook decode: %w", err)
	}
	// Bitpin quotes in toman (IRT) already — priceMultiplier = 1, no rescale.
	book := BuildOrderBook(bitpinCode, canonical,
		bitpinLevelsFromPairs(d.Bids), bitpinLevelsFromPairs(d.Asks),
		decimal.NewFromInt(1), "rest", time.Now())
	return book, nil
}

func (b *bitpinPublic) SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error) {
	// Bitpin's order book arrives over a Centrifugo WebSocket; that transport is
	// deferred (OrderBookWS = false), so this is unsupported here.
	return nil, Unsupported(bitpinCode, "order-book WebSocket")
}

// ─── Private client ────────────────────────────────────────────────────────

type bitpinPrivate struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string

	tokenMu            sync.Mutex
	accessToken        string
	refreshToken       string
	tokenAt            time.Time
	authThrottledUntil time.Time
	// tokenCredFP is a NON-SECRET fingerprint of the credential the cached token was minted with. If
	// the active credential is rotated, the fingerprint changes and the cached token/refresh token are
	// dropped so the client re-authenticates with the NEW credential (PR22 round-2 blocker 3). Never
	// logged.
	tokenCredFP string
}

func newBitpinPrivate(cfg ClientConfig, logger *IOLogger) (PrivateClient, error) {
	cfg.Code = bitpinCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = bitpinDefaultREST
	}
	return &bitpinPrivate{cfg: cfg, http: hc, restURL: strings.TrimRight(restURL, "/")}, nil
}

func (b *bitpinPrivate) Name() string               { return bitpinCode }
func (b *bitpinPrivate) Capabilities() Capabilities { return bitpinCapabilities() }

// --- auth / JWT token flow ---

// bitpinAuthThrottleRe extracts N from Bitpin's standard 429 body shape:
// {"detail":"Request was throttled. Expected available in 29 seconds."}.
var bitpinAuthThrottleRe = regexp.MustCompile(`available in (\d+) second`)

// bearerToken returns a valid JWT access token, acquiring or refreshing it as
// needed. Faithful port of iranArb's bitpin token flow: cache for bitpinTokenTTL,
// prefer the cached token while a 429 backoff is active, refresh via
// /usr/refresh_token/ when a refresh token is held, else /usr/authenticate/.
// bitpinAuthRateLimited is the ErrNotSent-wrapped rate-limit an auth interaction reports for a
// MUTATION (strict), so the order endpoint is never called in the same invocation and the
// executor arms the cooldown (PR20 correction #3).
func (b *bitpinPrivate) bitpinAuthRateLimited(msg string, retryAfter time.Duration, source string) error {
	if source == "" {
		source = RLSourceStatus
	}
	if retryAfter < 0 {
		retryAfter = 0
	}
	return execution.NotSent(&NormalizedAPIError{
		Exchange: bitpinCode, Op: "auth", StatusCode: http.StatusTooManyRequests,
		Category: CatRateLimit, Retryable: true, Message: msg, Err: execution.ErrRateLimited,
		// Carry the REAL venue throttle deadline (round 9 #4) so the executor schedules the queue
		// retry at the actual cooldown, not a short configured fallback that would burn the retry
		// budget before the throttle window expires. An auth throttle proves the order was NOT sent.
		RateLimit: &RateLimitInfo{RetryAfter: retryAfter, Source: source, Code: "auth_throttled", DefiniteRejection: true},
	})
}

// bearerToken returns a valid JWT. `strict` (used for MUTATIONS) means: any auth rate limit —
// an active throttle window, a fresh 429, or an auth 200 with X-RateLimit-Remaining:0 — is a
// definitely-not-sent rate limit, NOT a licence to send the order with a cached token. Lenient
// (reads) prefers a cached token during a throttle window.
func (b *bitpinPrivate) bearerToken(ctx context.Context, strict bool) (string, error) {
	// Resolve the CURRENT active credential FIRST (round-2 blocker 3). The cached token is bound to a
	// non-secret fingerprint of the credential it was minted with; if the active credential was
	// rotated, the fingerprint differs and the whole token cache is dropped so we re-authenticate
	// with the NEW credential. Rotation is therefore effective immediately, never served from the
	// old credential's cached token.
	creds, credErr := b.cfg.Creds.Credentials(ctx, bitpinCode)
	fp := ""
	if credErr == nil {
		fp = credFingerprint(creds)
	}

	b.tokenMu.Lock()
	// Active credential changed since the cached token was minted → drop the cache (token + refresh
	// + throttle) so we cannot reuse the previous credential's token or refresh it.
	if credErr == nil && b.tokenCredFP != "" && b.tokenCredFP != fp {
		b.accessToken, b.refreshToken, b.tokenCredFP, b.authThrottledUntil = "", "", "", time.Time{}
	}
	// Fast path: still-fresh cached token for the SAME credential.
	if b.accessToken != "" && b.tokenCredFP == fp && time.Since(b.tokenAt) < bitpinTokenTTL {
		tok := b.accessToken
		b.tokenMu.Unlock()
		return tok, nil
	}
	// Cached token past our internal TTL. Honor an active 429 backoff on the
	// auth endpoint: prefer the (slightly-stale, ~1m grace) cached token over an
	// auth attempt we know would 429 again — but only for the SAME credential.
	inBackoff := !b.authThrottledUntil.IsZero() && time.Now().Before(b.authThrottledUntil)
	if inBackoff {
		wait := time.Until(b.authThrottledUntil)
		hasCache := b.accessToken != "" && b.tokenCredFP == fp
		tok := b.accessToken
		b.tokenMu.Unlock()
		if strict {
			// A MUTATION must NOT be sent while auth is throttled, even with a cached token. Carry
			// the REMAINING throttle window (round 9 #4) so the executor waits the real deadline and
			// does not re-attempt (and burn a retry) before it expires.
			return "", b.bitpinAuthRateLimited(fmt.Sprintf("auth throttled, retry after %s", wait.Round(time.Second)), wait, RLSourceStatus)
		}
		if hasCache {
			return tok, nil // reads may proceed with the cached token (same credential)
		}
		return "", b.authError(fmt.Sprintf("auth throttled, retry after %s", wait), http.StatusTooManyRequests)
	}
	refresh := b.refreshToken
	b.tokenMu.Unlock()

	if err := credErr; err != nil {
		// PR20 correction #3: pre-network credential failure — definitely not sent, transient.
		return "", execution.NotSent(err)
	}
	if creds.APIKey == "" || creds.APISecret == "" {
		return "", b.authError("missing api credentials", http.StatusUnauthorized)
	}

	path := "/api/v1/usr/authenticate/"
	body, _ := json.Marshal(map[string]string{"api_key": creds.APIKey, "secret_key": creds.APISecret})
	if refresh != "" {
		path = "/api/v1/usr/refresh_token/"
		body, _ = json.Marshal(map[string]string{"refresh": refresh})
	}

	// Round 8 #3: reserve a pacing slot for the auth/refresh HTTP call — but ONLY here, at the
	// point we are actually about to hit the network. The still-fresh-cached-token and
	// throttle-backoff fast paths above returned WITHOUT a network call and therefore consumed no
	// slot, so pacing reservations equal actual HTTP calls. A cancellation while waiting for the
	// slot is definitely-not-sent (the auth request was never built or sent).
	if perr := paceNetwork(ctx); perr != nil {
		return "", execution.NotSent(perr)
	}

	reqCtx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, b.restURL+path, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("bitpin token %s: %w", path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusTooManyRequests {
		// Arm the throttle window so subsequent callers reuse the cached token
		// rather than re-hitting auth. Prefer Retry-After, then the body hint.
		header := resp.Header.Get("Retry-After")
		wait := parseBitpinAuthThrottle(header, raw)
		src := RLSourceHeader
		if wait <= 0 {
			wait = bitpinDefaultThrottle
			src = "fallback"
		} else if ParseRetryAfter(header, time.Now()) <= 0 {
			src = RLSourceBody // the header did not yield it, so the body did
		}
		b.tokenMu.Lock()
		b.authThrottledUntil = time.Now().Add(wait)
		tok := b.accessToken
		b.tokenMu.Unlock()
		if strict {
			// Auth is rate-limited → do NOT send the order this invocation (PR20 correction #3).
			// Carry the REAL parsed wait (round 9 #4) so the retry waits the venue deadline.
			return "", b.bitpinAuthRateLimited("auth endpoint rate-limited", wait, src)
		}
		if tok != "" {
			return tok, nil // reads may reuse the cached token
		}
		return "", b.apiError(path, resp.StatusCode, raw)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", b.apiError(path, resp.StatusCode, raw)
	}
	var payload struct {
		Access  string `json:"access"`
		Refresh string `json:"refresh"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", fmt.Errorf("bitpin token decode: %w", err)
	}
	if payload.Access == "" {
		return "", b.authError("token response missing access token", http.StatusUnauthorized)
	}

	// The active credential may have been ROTATED while this authentication was in flight (round-3
	// blocker 2A). Re-resolve it and, if its fingerprint no longer matches the credential we
	// authenticated with, do NOT cache or return this (now stale) token — a stale auth response must
	// never repopulate the cache after a rotation. Fail definitely-not-sent so the caller retries and
	// re-authenticates with the new credential.
	if creds2, cerr := b.cfg.Creds.Credentials(ctx, bitpinCode); cerr == nil && credFingerprint(creds2) != fp {
		b.tokenMu.Lock()
		if b.tokenCredFP != credFingerprint(creds2) {
			// Drop anything tied to the old credential so no stale token/refresh survives.
			b.accessToken, b.refreshToken, b.tokenCredFP, b.authThrottledUntil = "", "", "", time.Time{}
		}
		b.tokenMu.Unlock()
		return "", execution.NotSent(fmt.Errorf("bitpin: active credential rotated during authentication — not sent, retry with the new credential"))
	}

	b.tokenMu.Lock()
	b.accessToken = payload.Access
	if payload.Refresh != "" {
		b.refreshToken = payload.Refresh
	}
	b.tokenAt = time.Now()
	b.authThrottledUntil = time.Time{}
	b.tokenCredFP = fp // bind the cached token to the credential it was minted with (round-2 blocker 3)
	b.tokenMu.Unlock()

	// Auth succeeded, but if it reports the quota is now exhausted, future Bitpin calls must
	// pause. For a MUTATION the order is the NEXT network call, so it must not be sent this
	// invocation — fail strict as a definitely-not-sent rate limit (PR20 correction #3). The
	// token is cached, so the retry after the cooldown can reuse it.
	if strict {
		if reset, src, ok := RetryAfterFromHeaders(resp.Header, time.Now()); ok {
			// Derive the real reset deadline (round 9 #4); fall back to the default only when the
			// venue gave a remaining=0 signal with no parseable reset.
			wait := reset
			if wait <= 0 {
				wait = bitpinDefaultThrottle
				src = "fallback"
			}
			b.tokenMu.Lock()
			if b.authThrottledUntil.Before(time.Now().Add(wait)) {
				b.authThrottledUntil = time.Now().Add(wait)
			}
			b.tokenMu.Unlock()
			return "", b.bitpinAuthRateLimited("auth succeeded but quota exhausted (X-RateLimit-Remaining: 0)", wait, src)
		}
	}
	return payload.Access, nil
}

// doJSON performs an authenticated Bitpin request: acquires a bearer token, sends
// the JSON body (if any), and returns the raw response, decoding into out when
// provided. Non-2xx becomes a *NormalizedAPIError.
func (b *bitpinPrivate) doJSON(ctx context.Context, op, method, path string, in, out any) ([]byte, error) {
	// bearerToken does ALL pre-order work — it may call the auth/refresh endpoint but NEVER the
	// order endpoint. Any failure here is definitely-not-sent (wrapped ErrNotSent). Lenient token
	// for reads; mutations use PreparePlace/PrepareCancel with the STRICT token.
	tok, err := b.bearerToken(ctx, false)
	if err != nil {
		if !execution.IsNotSent(err) {
			err = execution.NotSent(err)
		}
		return nil, err
	}
	var body []byte
	if in != nil {
		buf, jerr := json.Marshal(in)
		if jerr != nil {
			return nil, execution.NotSentPermanent(jerr)
		}
		body = buf
	}
	return b.sendAuthed(ctx, op, method, path, tok, body, out)
}

// sendAuthed performs an authenticated Bitpin HTTP call with an ALREADY-acquired token, for the
// READ path only (doJSON). Mutations do NOT go through this: they pre-build their immutable
// request during preparation and send it via bitpinPrepared.Send (PR20 correction round 8 #2), so
// no request construction happens at their send boundary.
func (b *bitpinPrivate) sendAuthed(ctx context.Context, op, method, path, token string, body []byte, out any) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	reqCtx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, b.restURL+path, rdr)
	if err != nil {
		return nil, execution.NotSentPermanent(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bitpin %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return raw, bitpinAPIErrorH(op, resp.StatusCode, resp.Header, raw)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return raw, fmt.Errorf("bitpin %s decode: %w", op, err)
		}
	}
	return raw, nil
}

// buildAuthedRequest constructs the FINAL immutable authenticated request for a Bitpin mutation
// with an already-acquired token. This is the last fallible pre-network step, and it runs during
// preparation — BEFORE the executor commits MarkInFlight (PR20 correction round 8 #2). The request
// carries a placeholder context; Send binds the real send context via doPreparedRequest.
func (b *bitpinPrivate) buildAuthedRequest(method, path, token string, body []byte) (*http.Request, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, b.restURL+path, rdr)
	if err != nil {
		return nil, execution.NotSentPermanent(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// bitpinPrepared is a fully-prepared order/cancel whose FINAL http.Request is already built (token
// acquired, body attached, headers set). Send performs ONLY the send-boundary work: bind the send
// context and perform the single order/cancel HTTP call. No token, credential, symbol, payload, or
// request construction happens here (PR20 correction round 8 #2) — the type deliberately holds no
// material from which a request could be rebuilt.
type bitpinPrepared struct {
	b      *bitpinPrivate
	op     string
	req    *http.Request
	credFP string // fingerprint of the credential the request (and its bearer token) were built with
	decode func(raw []byte) (execution.OrderAck, error)
}

func (p *bitpinPrepared) Send(ctx context.Context) (execution.OrderAck, error) {
	// Refuse to transmit if the active credential was ROTATED after this mutation was prepared
	// (round-3 blocker 2B). The request carries a bearer token minted for the old credential; sending
	// it would place/cancel through the wrong (now-disabled) credential. The check runs BEFORE the
	// network call, so this is DEFINITELY-not-sent — the executor re-prepares with the new credential
	// (never a blind resend of a possibly-sent request).
	if p.credFP != "" {
		creds, cerr := p.b.cfg.Creds.Credentials(ctx, bitpinCode)
		if cerr != nil {
			return execution.OrderAck{}, execution.NotSent(cerr)
		}
		if credFingerprint(creds) != p.credFP {
			return execution.OrderAck{}, execution.NotSent(fmt.Errorf("bitpin: active credential rotated after preparation — not sent, re-prepare with the new credential"))
		}
	}
	status, hdr, raw, err := doPreparedRequest(ctx, p.b.http, p.req)
	if err != nil {
		return execution.OrderAck{}, fmt.Errorf("bitpin %s %s: %w", p.req.Method, p.req.URL.Path, err)
	}
	if status < 200 || status >= 300 {
		return execution.OrderAck{}, bitpinAPIErrorH(p.op, status, hdr, raw)
	}
	if p.decode == nil {
		return execution.OrderAck{}, nil
	}
	return p.decode(raw)
}

// PreparePlace does all pre-send work for a place — symbol validation, payload construction, and
// (the network) token acquisition — returning a prepared order whose Send is the only remaining
// network call. A rate limit during auth fails here as a definitely-not-sent rate limit, so the
// order endpoint is never reached (PR20 correction #2/#3).
func (b *bitpinPrivate) PreparePlace(ctx context.Context, req execution.OrderRequest) (PreparedMutation, error) {
	venueSym := bitpinNativeSymbol(req.Symbol)
	if v, ok := VenueSymbol(b.cfg, req.Symbol); ok {
		venueSym = bitpinNativeSymbol(v)
	}
	if strings.TrimSpace(venueSym) == "" {
		return nil, execution.NotSentPermanent(fmt.Errorf("bitpin: invalid symbol %q", req.Symbol))
	}
	orderType := strings.ToLower(strings.TrimSpace(req.OrderType))
	if orderType == "" {
		orderType = "limit"
	}
	payload := bitpinPlaceOrderRequest{
		Symbol: venueSym, Type: orderType, Side: strings.ToLower(req.Side),
		BaseAmount: req.Quantity.String(), Identifier: req.ClientOrderID,
	}
	if req.LimitPrice.IsPositive() {
		payload.Price = req.LimitPrice.String()
	}
	if tif := strings.TrimSpace(req.TimeInForce); tif != "" {
		payload.TimeInForce = tif
	}
	body, jerr := json.Marshal(payload)
	if jerr != nil {
		return nil, execution.NotSentPermanent(jerr)
	}
	tok, err := b.bearerToken(ctx, true) // STRICT: auth rate limit → definitely-not-sent, no order call
	if err != nil {
		if !execution.IsNotSent(err) {
			err = execution.NotSent(err)
		}
		return nil, err
	}
	// Build the FINAL order request NOW (round 8 #2) — fallible construction before MarkInFlight.
	httpReq, rerr := b.buildAuthedRequest(http.MethodPost, "/api/v1/odr/orders/", tok, body)
	if rerr != nil {
		return nil, rerr
	}
	return &bitpinPrepared{b: b, op: "place", req: httpReq, credFP: b.tokenFP(),
		decode: func(raw []byte) (execution.OrderAck, error) { return b.decodePlaceAck(raw, req) }}, nil
}

// PrepareCancel does all pre-send work for a cancel (token acquisition) and returns a prepared
// cancel whose Send is the only remaining network call (PR20 correction #2/#3).
func (b *bitpinPrivate) PrepareCancel(ctx context.Context, exchangeOrderID string) (PreparedMutation, error) {
	if exchangeOrderID == "" {
		return nil, execution.NotSentPermanent(fmt.Errorf("bitpin CancelOrder: empty order id"))
	}
	path := "/api/v1/odr/orders/" + url.PathEscape(exchangeOrderID) + "/"
	if !bitpinIsDecimalString(exchangeOrderID) {
		path = "/api/v1/odr/orders/identifier/" + url.PathEscape(exchangeOrderID) + "/"
	}
	tok, err := b.bearerToken(ctx, true)
	if err != nil {
		if !execution.IsNotSent(err) {
			err = execution.NotSent(err)
		}
		return nil, err
	}
	// Build the FINAL cancel request NOW (round 8 #2) — fallible construction before MarkInFlight.
	httpReq, rerr := b.buildAuthedRequest(http.MethodDelete, path, tok, nil)
	if rerr != nil {
		return nil, rerr
	}
	return &bitpinPrepared{b: b, op: "cancel", req: httpReq, credFP: b.tokenFP()}, nil
}

// tokenFP returns the fingerprint of the credential the current cached bearer token was minted with
// (set by bearerToken). A prepared mutation captures it so Send can refuse to transmit after a
// credential rotation (round-3 blocker 2B).
func (b *bitpinPrivate) tokenFP() string {
	b.tokenMu.Lock()
	defer b.tokenMu.Unlock()
	return b.tokenCredFP
}

// --- GetBalances ---

type bitpinWalletItem struct {
	Asset            string          `json:"asset"`
	Currency         string          `json:"currency"`
	Code             string          `json:"code"`
	Service          string          `json:"service"`
	Balance          json.RawMessage `json:"balance"`
	Available        json.RawMessage `json:"available"`
	AvailableBalance json.RawMessage `json:"available_balance"`
	Frozen           json.RawMessage `json:"frozen"`
	Blocked          json.RawMessage `json:"blocked"`
}

func (b *bitpinPrivate) GetBalances(ctx context.Context) ([]domain.Balance, error) {
	raw, err := b.doJSON(ctx, "balances", http.MethodGet, "/api/v1/wlt/wallets/?limit=300", nil, nil)
	if err != nil {
		return nil, err
	}
	wallets, err := parseBitpinWallets(raw)
	if err != nil {
		return nil, fmt.Errorf("bitpin wallets decode: %w", err)
	}
	now := time.Now().UTC()
	out := make([]domain.Balance, 0, len(wallets))
	for _, w := range wallets {
		if w.Service != "" && w.Service != "main" && w.Service != "spot" {
			continue
		}
		asset := strings.ToUpper(strings.TrimSpace(firstNonEmptyBitpin(w.Asset, w.Currency, w.Code)))
		if asset == "" {
			continue
		}
		switch asset {
		case "RIAL", "IRR", "RLS":
			asset = domain.CurrencyIRT
		}
		available := bitpinDecRaw(w.Available)
		if available.IsZero() {
			available = bitpinDecRaw(w.AvailableBalance)
		}
		if available.IsZero() {
			available = bitpinDecRaw(w.Balance)
		}
		locked := bitpinDecRaw(w.Frozen)
		if locked.IsZero() {
			locked = bitpinDecRaw(w.Blocked)
		}
		total := bitpinDecRaw(w.Balance)
		if total.IsZero() {
			total = available.Add(locked)
		}
		out = append(out, domain.Balance{
			Exchange:  bitpinCode,
			Asset:     asset,
			Available: available,
			Locked:    locked,
			Total:     total,
			UpdatedAt: now,
		})
	}
	return out, nil
}

// --- PlaceOrder ---

type bitpinPlaceOrderRequest struct {
	Symbol      string `json:"symbol"`
	Type        string `json:"type"`
	Side        string `json:"side"`
	BaseAmount  string `json:"base_amount"`
	Price       string `json:"price,omitempty"`
	Identifier  string `json:"identifier,omitempty"`
	TimeInForce string `json:"time_in_force,omitempty"`
}

// PlaceOrder = PreparePlace(...).Send(...). The executor uses the two-stage MutationPreparer
// directly so the token/auth work happens before MarkInFlight; PlaceOrder keeps the one-call
// contract for any non-executor caller (PR20 correction #2).
func (b *bitpinPrivate) PlaceOrder(ctx context.Context, req execution.OrderRequest) (execution.OrderAck, error) {
	prepared, err := b.PreparePlace(ctx, req)
	if err != nil {
		return execution.OrderAck{}, err
	}
	return prepared.Send(ctx)
}

// decodePlaceAck parses a Bitpin place response into an OrderAck.
func (b *bitpinPrivate) decodePlaceAck(raw []byte, req execution.OrderRequest) (execution.OrderAck, error) {
	var order bitpinOrder
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &order)
	}
	// Some responses wrap the order under {"order": {...}}.
	if len(order.ID) == 0 && order.Identifier == "" {
		var wrapped struct {
			Order bitpinOrder     `json:"order"`
			ID    json.RawMessage `json:"id"`
		}
		if jerr := json.Unmarshal(raw, &wrapped); jerr == nil && (len(wrapped.Order.ID) > 0 || wrapped.Order.Identifier != "") {
			order = wrapped.Order
			if len(order.ID) == 0 {
				order.ID = wrapped.ID
			}
		}
	}
	if order.Identifier == "" {
		order.Identifier = req.ClientOrderID
	}
	if order.State == "" {
		order.State = "active"
	}

	status := bitpinOrderToStatus(order, req.Symbol, strings.ToLower(req.Side))
	ack := execution.OrderAck{
		ExchangeOrderID: status.ExchangeOrderID,
		ClientOrderID:   firstNonEmptyBitpin(status.ClientOrderID, req.ClientOrderID),
		Symbol:          req.Symbol,
		Side:            strings.ToLower(req.Side),
		RequestedQty:    req.Quantity,
		LimitPrice:      req.LimitPrice,
		Status:          status.Status,
		RawStatus:       order.State,
		FilledQty:       status.FilledQty,
		AvgPrice:        status.AvgPrice,
		ExecutedQuote:   status.ExecutedQuote,
		Fee:             status.Fee,
		FeeAsset:        status.FeeAsset,
		Active:          bitpinStateActive(order.State),
		AckedAt:         bitpinPickTime(status.UpdatedAt),
		Raw:             MaskBody(string(raw)),
	}
	return ack, nil
}

// --- CancelOrder ---

// CancelOrder accepts EITHER a numeric exchange order id OR an identifier string
// (the ClientOrderID passed at placement); the two use different URL forms.
func (b *bitpinPrivate) CancelOrder(ctx context.Context, exchangeOrderID string) error {
	prepared, err := b.PrepareCancel(ctx, exchangeOrderID)
	if err != nil {
		return err
	}
	_, err = prepared.Send(ctx)
	return err
}

// --- GetOrder ---

func (b *bitpinPrivate) GetOrder(ctx context.Context, exchangeOrderID string) (execution.OrderStatus, error) {
	if exchangeOrderID == "" {
		return execution.OrderStatus{}, fmt.Errorf("bitpin GetOrder: empty order id")
	}
	var order bitpinOrder
	if bitpinIsDecimalString(exchangeOrderID) {
		_, err := b.doJSON(ctx, "order", http.MethodGet, "/api/v1/odr/orders/"+url.PathEscape(exchangeOrderID)+"/", nil, &order)
		if err != nil {
			return execution.OrderStatus{}, err
		}
	} else {
		return b.getOrderByIdentifier(ctx, exchangeOrderID)
	}
	if len(order.ID) == 0 && order.Identifier == "" {
		return execution.OrderStatus{}, fmt.Errorf("bitpin GetOrder %q: %w", exchangeOrderID, execution.ErrOrderUnknown)
	}
	return bitpinOrderToStatus(order, "", ""), nil
}

// GetOrderByClientOrderID implements exchanges.ClientOrderLookup: it resolves an order by the
// "identifier" (our client order id) via GET /odr/orders/?identifier=<id> — ALWAYS the identifier
// endpoint, never the numeric-id path, so an all-digit client id can never be misrouted. Used by
// the ambiguous-place recovery when the exchange order id is unknown.
func (b *bitpinPrivate) GetOrderByClientOrderID(ctx context.Context, clientOrderID string) (execution.OrderStatus, error) {
	if clientOrderID == "" {
		return execution.OrderStatus{}, fmt.Errorf("bitpin GetOrderByClientOrderID: empty identifier")
	}
	return b.getOrderByIdentifier(ctx, clientOrderID)
}

func (b *bitpinPrivate) getOrderByIdentifier(ctx context.Context, identifier string) (execution.OrderStatus, error) {
	// The ?identifier= endpoint returns a top-level array (not {"results":[]}).
	raw, err := b.doJSON(ctx, "order", http.MethodGet, "/api/v1/odr/orders/?identifier="+url.QueryEscape(identifier), nil, nil)
	if err != nil {
		return execution.OrderStatus{}, err
	}
	var order bitpinOrder
	var direct []bitpinOrder
	if jerr := json.Unmarshal(raw, &direct); jerr == nil && len(direct) > 0 {
		order = direct[0]
	} else {
		var payload bitpinOrdersResponse
		if jerr := json.Unmarshal(raw, &payload); jerr == nil && len(payload.Results) > 0 {
			order = payload.Results[0]
		}
	}
	if len(order.ID) == 0 && order.Identifier == "" {
		return execution.OrderStatus{}, fmt.Errorf("bitpin identifier %q: %w", identifier, execution.ErrOrderUnknown)
	}
	return bitpinOrderToStatus(order, "", ""), nil
}

// --- GetOpenOrders ---

func (b *bitpinPrivate) GetOpenOrders(ctx context.Context, symbol string) ([]execution.OrderStatus, error) {
	path := "/api/v1/odr/orders/?state=active&limit=100"
	if symbol != "" {
		venueSym := bitpinNativeSymbol(symbol)
		if v, ok := VenueSymbol(b.cfg, symbol); ok {
			venueSym = bitpinNativeSymbol(v)
		}
		path += "&symbol=" + url.QueryEscape(venueSym)
	}
	raw, err := b.doJSON(ctx, "open_orders", http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	orders := bitpinDecodeOrders(raw)
	out := make([]execution.OrderStatus, 0, len(orders))
	for _, o := range orders {
		st := bitpinOrderToStatus(o, "", "")
		if !bitpinStateActive(o.State) {
			continue
		}
		out = append(out, st)
	}
	return out, nil
}

func (b *bitpinPrivate) SubscribeOrderUpdates(ctx context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	// Bitpin order updates arrive over a Centrifugo WebSocket that is deferred;
	// this adapter is polling-only (OrderStatusPoll = true, OrderUpdatesWS = false).
	return nil, Unsupported(bitpinCode, "order-update WebSocket")
}

// ─── shared order model + parsing ──────────────────────────────────────────

type bitpinOrder struct {
	ID                 json.RawMessage `json:"id"`
	Identifier         string          `json:"identifier"`
	State              string          `json:"state"`
	Type               string          `json:"type"`
	Side               string          `json:"side"`
	Symbol             string          `json:"symbol"`
	Price              json.RawMessage `json:"price"`
	AveragePrice       json.RawMessage `json:"average_price"`
	BaseAmount         json.RawMessage `json:"base_amount"`
	QuoteAmount        json.RawMessage `json:"quote_amount"`
	RemainAmount       json.RawMessage `json:"remain_amount"`
	DealedBaseAmount   json.RawMessage `json:"dealed_base_amount"`
	DealedQuoteAmount  json.RawMessage `json:"dealed_quote_amount"`
	Commission         json.RawMessage `json:"commission"`
	CommissionCurrency string          `json:"commission_currency"`
	Fee                json.RawMessage `json:"fee"`
	CreatedAt          string          `json:"created_at"`
	ClosedAt           string          `json:"closed_at"`
}

type bitpinOrdersResponse struct {
	Results []bitpinOrder `json:"results"`
}

// bitpinOrderToStatus maps a raw Bitpin order onto a normalized OrderStatus.
// fallbackSymbol/fallbackSide are used when the order body omits them.
func bitpinOrderToStatus(o bitpinOrder, fallbackSymbol, fallbackSide string) execution.OrderStatus {
	numericID := bitpinStringFromRaw(o.ID)
	identifier := strings.TrimSpace(o.Identifier)
	exchangeID := numericID
	if exchangeID == "" {
		exchangeID = identifier
	}
	symbol := fallbackSymbol
	if symbol == "" && o.Symbol != "" {
		symbol = bitpinInternalSymbol(o.Symbol)
	}
	side := strings.ToLower(fallbackSide)
	if o.Side != "" {
		side = strings.ToLower(o.Side)
	}

	requested := bitpinDecRaw(o.BaseAmount)
	filled := bitpinDecRaw(o.DealedBaseAmount)
	executedQuote := bitpinDecRaw(o.DealedQuoteAmount)
	remaining := bitpinDecRaw(o.RemainAmount)
	if requested.IsZero() && filled.IsPositive() {
		requested = filled.Add(remaining)
	}
	if remaining.IsZero() && requested.IsPositive() {
		remaining = requested.Sub(filled)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}
	}
	avg := bitpinDecRaw(o.AveragePrice)
	if avg.IsZero() {
		avg = bitpinDecRaw(o.Price)
	}
	if avg.IsZero() && filled.IsPositive() && executedQuote.IsPositive() {
		avg = executedQuote.Div(filled)
	}
	fee := bitpinDecRaw(o.Commission)
	if fee.IsZero() {
		fee = bitpinDecRaw(o.Fee)
	}
	feeAsset := bitpinInternalAsset(o.CommissionCurrency)
	at := bitpinParseTime(firstNonEmptyBitpin(o.ClosedAt, o.CreatedAt))
	created := bitpinParseTime(o.CreatedAt)

	return execution.OrderStatus{
		ExchangeOrderID: exchangeID,
		ClientOrderID:   identifier,
		Symbol:          symbol,
		Side:            side,
		Status:          mapBitpinOrderState(o.State, filled, requested),
		IntendedQty:     requested,
		FilledQty:       filled,
		RemainingQty:    remaining,
		AvgPrice:        avg,
		ExecutedQuote:   executedQuote,
		Fee:             fee,
		FeeAsset:        feeAsset,
		CreatedAt:       created,
		UpdatedAt:       at,
	}
}

// mapBitpinOrderState maps Bitpin's raw "state" string (plus fill amounts) onto a
// normalized execution.State*. Faithful port of iranArb's mapBitpinOrderState.
func mapBitpinOrderState(status string, filled, requested decimal.Decimal) execution.NormalizedOrderState {
	status = strings.ToLower(strings.TrimSpace(status))
	if requested.IsPositive() && filled.Equal(requested) {
		return execution.StateFilled
	}
	switch status {
	case "closed", "done", "filled":
		if filled.IsPositive() {
			if requested.IsPositive() && filled.LessThan(requested) {
				return execution.StatePartiallyCanceled
			}
			return execution.StateFilled
		}
		return execution.StateCanceled
	case "active", "open":
		if filled.IsPositive() {
			return execution.StatePartiallyFilled
		}
		return execution.StateOpen
	case "canceled", "cancelled", "cancel_requested":
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled
	default:
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled
	}
}

func bitpinStateActive(status string) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	return s == "active" || s == "open"
}

// bitpinDecodeOrders tolerates both {"results":[...]} and a bare [...] array.
func bitpinDecodeOrders(raw []byte) []bitpinOrder {
	var payload bitpinOrdersResponse
	if err := json.Unmarshal(raw, &payload); err == nil && len(payload.Results) > 0 {
		return payload.Results
	}
	var direct []bitpinOrder
	if err := json.Unmarshal(raw, &direct); err == nil {
		return direct
	}
	return nil
}

// parseBitpinWallets tolerates a bare array or a {results|wallets|data:[...]} envelope.
func parseBitpinWallets(raw []byte) ([]bitpinWalletItem, error) {
	var direct []bitpinWalletItem
	if err := json.Unmarshal(raw, &direct); err == nil {
		return direct, nil
	}
	var env struct {
		Results []bitpinWalletItem `json:"results"`
		Wallets []bitpinWalletItem `json:"wallets"`
		Data    []bitpinWalletItem `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	switch {
	case env.Results != nil:
		return env.Results, nil
	case env.Wallets != nil:
		return env.Wallets, nil
	case env.Data != nil:
		return env.Data, nil
	default:
		return nil, fmt.Errorf("bitpin wallets response has no wallets/results/data")
	}
}

// ─── error helpers ─────────────────────────────────────────────────────────

// apiError builds a *NormalizedAPIError from a non-2xx response, wrapping the
// classifiable execution sentinel. The body is secret-masked before storage.
func (b *bitpinPrivate) apiError(op string, status int, body []byte) error {
	return bitpinAPIError(op, status, body)
}

func (b *bitpinPrivate) authError(msg string, status int) error {
	return &NormalizedAPIError{
		Exchange: bitpinCode, Op: "auth", StatusCode: status,
		Category: classifyHTTPStatus(status), Retryable: status == http.StatusTooManyRequests,
		Message: msg, Err: bitpinSentinelFor(status),
	}
}

func bitpinAPIError(op string, status int, body []byte) error {
	return bitpinAPIErrorH(op, status, nil, body)
}

// bitpinAPIErrorH is bitpinAPIError with the response headers, so throttle metadata
// (Retry-After, DRF "available in N seconds" body) is preserved (PR20 #3/#6). Bitpin has
// no iranArb-verified pre-execution rejection contract for order-path 429s, so
// DefiniteRejection stays FALSE — a rate-limited mutation is AMBIGUOUS.
func bitpinAPIErrorH(op string, status int, hdr http.Header, body []byte) error {
	e := &NormalizedAPIError{
		Exchange:   bitpinCode,
		Op:         op,
		StatusCode: status,
		Category:   classifyHTTPStatus(status),
		Retryable:  status >= 500 || status == http.StatusTooManyRequests,
		Message:    MaskBody(string(body)),
		Err:        bitpinSentinelFor(status),
	}
	if status == http.StatusTooManyRequests {
		retryAfter := ""
		if hdr != nil {
			retryAfter = hdr.Get("Retry-After")
		}
		wait := parseBitpinAuthThrottle(retryAfter, body) // proven parser: header seconds, then body regex
		src := RLSourceStatus
		if wait > 0 {
			src = RLSourceHeader
			if retryAfter == "" {
				src = RLSourceBody
			}
		}
		e.RateLimit = &RateLimitInfo{RetryAfter: wait, Source: src}
	}
	return e
}

// bitpinSentinelFor maps an HTTP status to an execution sentinel for errors.Is.
func bitpinSentinelFor(status int) error {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return execution.ErrAuthFailed
	case status == http.StatusTooManyRequests:
		return execution.ErrRateLimited
	case status == http.StatusNotFound:
		return execution.ErrOrderUnknown
	default:
		return nil
	}
}

// parseBitpinAuthThrottle returns how long to wait before retrying the auth
// endpoint after a 429. Prefers the Retry-After header (seconds), then the
// integer in Bitpin's body. Returns 0 if neither yields a usable duration.
func parseBitpinAuthThrottle(retryAfterHeader string, body []byte) time.Duration {
	if v := strings.TrimSpace(retryAfterHeader); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	if m := bitpinAuthThrottleRe.FindSubmatch(body); len(m) == 2 {
		if n, err := strconv.Atoi(string(m[1])); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 0
}

// ─── small shared helpers ──────────────────────────────────────────────────

// bitpinGet performs an unauthenticated GET, returning *NormalizedAPIError on non-2xx.
func bitpinGet(ctx context.Context, hc *http.Client, baseURL, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bitpin GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, bitpinAPIError(path, resp.StatusCode, body)
	}
	return body, nil
}

// bitpinLevelsFromPairs parses [[price, qty], ...] rows into levels, skipping
// malformed/zero rows. (binance.levelsFromPairs takes [2]string; Bitpin's REST
// uses variable-length []string rows, hence a dedicated parser.)
func bitpinLevelsFromPairs(rows [][]string) []domain.Level {
	out := make([]domain.Level, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		price := parseDec(row[0])
		qty := parseDec(row[1])
		if price.IsZero() && qty.IsZero() {
			continue
		}
		out = append(out, domain.Level{Price: price, Quantity: qty})
	}
	return out
}

// bitpinNativeSymbol converts canonical "BTC/IRT" (or any underscore/dash form)
// to Bitpin's underscore venue symbol "BTC_IRT".
func bitpinNativeSymbol(s string) string {
	n := strings.ToUpper(strings.TrimSpace(s))
	n = strings.ReplaceAll(n, "/", "_")
	n = strings.ReplaceAll(n, "-", "_")
	for strings.Contains(n, "__") {
		n = strings.ReplaceAll(n, "__", "_")
	}
	return strings.Trim(n, "_")
}

// bitpinInternalSymbol reverses bitpinNativeSymbol: "BTC_IRT" -> "BTC/IRT".
func bitpinInternalSymbol(native string) string {
	n := bitpinNativeSymbol(native)
	parts := strings.SplitN(n, "_", 2)
	if len(parts) != 2 {
		return n
	}
	canonical := parts[0] + "/" + parts[1]
	if norm, err := domain.NormalizeSymbol(canonical); err == nil {
		return norm
	}
	return canonical
}

// bitpinInternalAsset normalizes a Bitpin asset/commission currency code.
func bitpinInternalAsset(asset string) string {
	a := strings.ToUpper(strings.TrimSpace(asset))
	a = strings.ReplaceAll(a, "_", "")
	switch a {
	case "TOMAN", "IRT", "IRR", "RLS", "RIAL":
		return domain.CurrencyIRT
	default:
		return a
	}
}

func bitpinIsDecimalString(s string) bool {
	if s == "" {
		return false
	}
	_, err := decimal.NewFromString(s)
	return err == nil
}

// bitpinStringFromRaw extracts a string from a json.RawMessage that may be a
// quoted string or a bare number.
func bitpinStringFromRaw(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return ""
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var out string
		if err := json.Unmarshal(raw, &out); err == nil {
			return out
		}
		return strings.Trim(s, `"`)
	}
	return s
}

// bitpinDecRaw parses a json.RawMessage (quoted or bare number) into a decimal,
// returning zero on any problem.
func bitpinDecRaw(raw json.RawMessage) decimal.Decimal {
	s := bitpinStringFromRaw(raw)
	return parseDec(s)
}

func bitpinParseTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000000Z"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func bitpinPickTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t
}

func firstNonEmptyBitpin(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
