package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
)

// Exir adapter — PUBLIC market data ONLY. Ported/adapted from the sibling iranArb
// system's exir package (an HollaEx-style API). There is no Exir private client
// here (rule #8 keeps market-data and trading clients separate); NewPrivate is
// nil.
//
// Capabilities: GetOrderBook (REST /v2/orderbooks). GetMarkets is NOT supported:
// the source had no markets/symbols list endpoint (it built its symbol set from
// config), so we report MarketMetadata=false and return Unsupported.
// SubscribeOrderBook is unsupported: the source streamed over a WebSocket
// (wss://api.exir.io/stream, {"op":"subscribe","args":["orderbook"]}), deferred
// in this port.
//
// Known limitations / quirks:
//   - QUOTE UNIT: Exir quotes in TOMAN (IRT), the system's canonical Iranian
//     quote, so the priceMultiplier is 1 (no rial->toman rescale).
//   - WHOLE-BOOK ENDPOINT: /v2/orderbooks returns a JSON object keyed by native
//     symbol containing EVERY market's book. GetOrderBook fetches that object
//     and selects the requested symbol's entry; the venue has no per-symbol REST
//     order-book path, so a single-symbol request still pulls the full payload.
//   - SYMBOL FORMAT: native symbols are hyphen-joined lowercase, e.g. "usdt-irt".
//     We normalize both sides (strip hyphens, upper-case) when matching, so the
//     map key matches canonical BASE/QUOTE regardless of separator/case. cfg.Symbols
//     may map canonical "USDT/IRT" -> "usdt-irt" to override the derived native.
//   - LEVELS: bids/asks are [[price, qty], ...] arrays of JSON numbers (floats).
//   - WS-ONLY IN SOURCE: the live update path was a WebSocket; it is deferred.
//   - ERROR FORMAT: errors surface as non-2xx HTTP statuses (no structured venue
//     error envelope), mapped via classifyHTTPStatus.

const (
	exirCode        = "exir"
	exirDefaultREST = "https://api.exir.io"
)

func init() {
	Register(Registration{
		Code: exirCode,
		Capabilities: Capabilities{
			MarketMetadata: false, // no markets/symbols list endpoint in source
			OrderBookREST:  true,  // GetOrderBook via /v2/orderbooks
			OrderBookWS:    false, // WS present in source but deferred
			// All private/order capabilities are false: public-only adapter.
		},
		NewPublic: newExirPublic,
		// NewPrivate intentionally nil — no trading surface for Exir here.
	})
}

type exirPublic struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
}

func newExirPublic(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	cfg.Code = exirCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = exirDefaultREST
	}
	return &exirPublic{
		cfg:     cfg,
		http:    hc,
		restURL: strings.TrimRight(restURL, "/"),
	}, nil
}

func (c *exirPublic) Name() string { return exirCode }
func (c *exirPublic) Capabilities() Capabilities {
	r, _ := Lookup(exirCode)
	return r.Capabilities
}

// --- GetMarkets (unsupported) ---

func (c *exirPublic) GetMarkets(ctx context.Context) ([]NormalizedMarket, error) {
	// The source had no markets/symbols list endpoint; symbols came from config.
	return nil, Unsupported(exirCode, "GetMarkets")
}

// --- GetOrderBook (REST) ---

// Prices/quantities are decoded as json.Number (the literal digits the venue sent), NOT
// float64 — so they convert to decimal EXACTLY (NewFromString), never via lossy
// NewFromFloat. (PR26 precision hardening: no float for price/quantity.)
type exirOrderBook struct {
	Bids      [][]json.Number `json:"bids"`
	Asks      [][]json.Number `json:"asks"`
	Timestamp time.Time       `json:"timestamp"`
}

// exirNativeSymbol resolves the venue-native symbol for a canonical symbol:
// cfg.Symbols mapping if present, else BASE/QUOTE -> base-quote (lower-cased,
// hyphen-joined). Matching is separator/case-insensitive via exirNormalizeNative.
func exirNativeSymbol(cfg ClientConfig, canonical string) string {
	if native, ok := VenueSymbol(cfg, canonical); ok && native != "" {
		return native
	}
	return strings.ToLower(strings.ReplaceAll(canonical, "/", "-"))
}

// exirNormalizeNative strips hyphens and uppercases, e.g. "usdt-irt" -> "USDTIRT".
func exirNormalizeNative(native string) string {
	return strings.ToUpper(strings.ReplaceAll(native, "-", ""))
}

func (c *exirPublic) GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	canonical, err := domain.NormalizeSymbol(symbol)
	if err != nil {
		canonical = strings.ToUpper(symbol)
	}
	native := exirNativeSymbol(c.cfg, canonical)
	wantKey := exirNormalizeNative(native)

	ctx, cancel := RequestContext(ctx, c.cfg)
	defer cancel()

	raw, err := c.get(ctx, "/v2/orderbooks")
	if err != nil {
		return domain.OrderBook{}, err
	}
	var allBooks map[string]exirOrderBook
	if err := json.Unmarshal(raw, &allBooks); err != nil {
		return domain.OrderBook{}, fmt.Errorf("exir orderbooks decode: %w", err)
	}

	ob, ok := allBooks[native]
	if !ok {
		// fall back to separator/case-insensitive match
		for k, v := range allBooks {
			if exirNormalizeNative(k) == wantKey {
				ob, ok = v, true
				break
			}
		}
	}
	if !ok {
		return domain.OrderBook{}, fmt.Errorf("exir: no order book for symbol %q (native %q)", canonical, native)
	}

	updatedAt := ob.Timestamp
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}

	// priceMultiplier 1: Exir already quotes in toman (IRT). BuildOrderBook sorts.
	book := BuildOrderBook(exirCode, canonical,
		exirPairsToLevels(ob.Bids), exirPairsToLevels(ob.Asks),
		decimal.NewFromInt(1), "rest", updatedAt)
	return book, nil
}

// --- SubscribeOrderBook (WS — unsupported) ---

func (c *exirPublic) SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error) {
	// WebSocket (wss://api.exir.io/stream) is present in the source but deferred
	// in this port; callers should poll GetOrderBook instead.
	return nil, Unsupported(exirCode, "SubscribeOrderBook")
}

// --- helpers ---

func exirPairsToLevels(pairs [][]json.Number) []domain.Level {
	levels := make([]domain.Level, 0, len(pairs))
	for _, p := range pairs {
		if len(p) < 2 {
			continue
		}
		// Exact decimal parse from the venue's literal digits (no float round-trip).
		price, perr := decimal.NewFromString(string(p[0]))
		qty, qerr := decimal.NewFromString(string(p[1]))
		if perr != nil || qerr != nil {
			continue // skip a malformed level rather than fabricate a price
		}
		levels = append(levels, domain.Level{Price: price, Quantity: qty})
	}
	return levels
}

func (c *exirPublic) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.restURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("exir GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, &NormalizedAPIError{
			Exchange: exirCode, Op: path, StatusCode: resp.StatusCode,
			Category:  classifyHTTPStatus(resp.StatusCode),
			Retryable: resp.StatusCode >= 500 || resp.StatusCode == 429,
			Message:   MaskBody(string(body)),
		}
	}
	return body, nil
}
