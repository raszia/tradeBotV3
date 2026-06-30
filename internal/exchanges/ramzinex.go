package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
)

// Ramzinex adapter — PUBLIC market data ONLY. Ported/adapted from the sibling
// iranArb system's ramzinex package. There is no Ramzinex private client here
// (rule #8 keeps market-data and trading clients separate); NewPrivate is nil.
//
// Capabilities: GetOrderBook (REST buys_sells snapshot). GetMarkets is supported
// via the public pairs endpoint (it doubles as Ramzinex's market list AND the
// pair-ID resolver). SubscribeOrderBook is intentionally unsupported here: the
// source streams over a Centrifuge WebSocket (channel "orderbook:{pair_id}"),
// which is deferred for this port.
//
// Known limitations / quirks:
//   - PAIR IDs: Ramzinex's order-book endpoint is keyed by a numeric pair ID,
//     not a symbol. GetOrderBook therefore first resolves the pair ID for the
//     requested canonical symbol from the pairs list endpoint
//     (/exchange/api/v2.0/exchange/pairs) and then fetches the book. Resolution
//     happens per call (the source resolved once at construction; here we keep
//     the adapter stateless and resolve on demand). Configure cfg.Symbols with
//     the venue-native pair label (the value compared against
//     pair.trading_chart_settings.ramzinex), e.g. "USDT/IRT" -> "usdtirt".
//   - QUOTE UNIT: Ramzinex quotes in RIAL (IRR). We pass priceMultiplier 0.1 to
//     BuildOrderBook to convert rial -> toman (IRT), the system's canonical quote.
//   - SYMBOL FORMAT: pairs are matched case-insensitively against
//     trading_chart_settings.ramzinex (a lowercase token like "usdtirt").
//   - LEVELS: buys/sells rows are heterogeneous JSON arrays ([price, qty, ...]);
//     the first two elements are price and qty and may be numbers or strings.
//     Sells are returned highest-first by the venue; BuildOrderBook re-sorts.
//   - ERROR FORMAT: the venue wraps payloads as {"status":int,"data":...}; a
//     non-zero status field (with HTTP 200) is treated as a venue error.

const (
	ramzinexCode        = "ramzinex"
	ramzinexDefaultREST = "https://api.ramzinex.com"
	// Ramzinex serves order books from a dedicated public-API host.
	ramzinexOrderBookHost = "https://publicapi.ramzinex.com"
)

// ramzinexRialToToman converts rial (IRR) prices to toman (IRT): IRR * 0.1 = IRT.
// Built from an exact decimal literal — never decimal.NewFromFloat — so no money value
// passes through a float64 (PR4 precision rule).
var ramzinexRialToToman = decimal.RequireFromString("0.1")

func init() {
	Register(Registration{
		Code: ramzinexCode,
		Capabilities: Capabilities{
			MarketMetadata: true,  // GetMarkets via pairs endpoint
			OrderBookREST:  true,  // GetOrderBook via buys_sells
			OrderBookWS:    false, // WS (Centrifuge) deferred in this port
			// All private/order capabilities are false: public-only adapter.
		},
		NewPublic: newRamzinexPublic,
		// NewPrivate intentionally nil — no trading surface for Ramzinex here.
	})
}

type ramzinexPublic struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string // pairs host (api.ramzinex.com by default)
	obURL   string // order-book host (publicapi.ramzinex.com by default)
}

func newRamzinexPublic(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	cfg.Code = ramzinexCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	// In production the pairs list and the order book live on two different
	// hosts. When cfg.BaseURL is set (tests pointing at a fake server) both are
	// served from that single base so tests never touch the network.
	restURL := ramzinexDefaultREST
	obURL := ramzinexOrderBookHost
	if cfg.BaseURL != "" {
		restURL = cfg.BaseURL
		obURL = cfg.BaseURL
	}
	return &ramzinexPublic{
		cfg:     cfg,
		http:    hc,
		restURL: strings.TrimRight(restURL, "/"),
		obURL:   strings.TrimRight(obURL, "/"),
	}, nil
}

func (c *ramzinexPublic) Name() string { return ramzinexCode }
func (c *ramzinexPublic) Capabilities() Capabilities {
	r, _ := Lookup(ramzinexCode)
	return r.Capabilities
}

// --- pairs / GetMarkets ---

type ramzinexPairsResponse struct {
	Status int `json:"status"`
	Data   struct {
		Pairs []ramzinexPairMeta `json:"pairs"`
	} `json:"data"`
}

type ramzinexPairMeta struct {
	ID                   int               `json:"id"`
	BaseCurrencySymbol   ramzinexLocalized `json:"base_currency_symbol"`
	QuoteCurrencySymbol  ramzinexLocalized `json:"quote_currency_symbol"`
	TradingChartSettings struct {
		Ramzinex string `json:"ramzinex"`
	} `json:"trading_chart_settings"`
}

// ramzinexLocalized captures the en field of a localized {en,fa} object; if the
// JSON value is a bare string it is used directly.
type ramzinexLocalized struct {
	En string `json:"en"`
}

func (l *ramzinexLocalized) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		l.En = s
		return nil
	}
	var obj struct {
		En string `json:"en"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}
	l.En = obj.En
	return nil
}

func (c *ramzinexPublic) loadPairs(ctx context.Context) ([]ramzinexPairMeta, error) {
	raw, err := c.get(ctx, c.restURL+"/exchange/api/v2.0/exchange/pairs", "/exchange/api/v2.0/exchange/pairs")
	if err != nil {
		return nil, err
	}
	var result ramzinexPairsResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("ramzinex pairs decode: %w", err)
	}
	return result.Data.Pairs, nil
}

func (c *ramzinexPublic) GetMarkets(ctx context.Context) ([]NormalizedMarket, error) {
	ctx, cancel := RequestContext(ctx, c.cfg)
	defer cancel()

	pairs, err := c.loadPairs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NormalizedMarket, 0, len(pairs))
	for _, p := range pairs {
		base := strings.ToUpper(strings.TrimSpace(p.BaseCurrencySymbol.En))
		quote := strings.ToUpper(strings.TrimSpace(p.QuoteCurrencySymbol.En))
		if base == "" || quote == "" {
			continue
		}
		// Ramzinex quotes in rial (IRR); normalize the quote asset to IRT.
		if quote == domain.CurrencyIRR {
			quote = domain.CurrencyIRT
		}
		canonical := base + "/" + quote
		out = append(out, NormalizedMarket{
			Exchange:        ramzinexCode,
			ExchangeSymbol:  p.TradingChartSettings.Ramzinex,
			CanonicalSymbol: canonical,
			BaseAsset:       base,
			QuoteAsset:      quote,
			QuoteAssetType:  domain.QuoteAssetType(canonical),
			Tradable:        true,
		})
	}
	return out, nil
}

// --- GetOrderBook (REST) ---

type ramzinexOrderBookResponse struct {
	Status int `json:"status"`
	Data   struct {
		Buys  [][]json.RawMessage `json:"buys"`
		Sells [][]json.RawMessage `json:"sells"`
	} `json:"data"`
}

// resolvePairID maps a canonical symbol to a Ramzinex numeric pair ID. It uses
// cfg.Symbols (canonical -> native pair token, e.g. "USDT/IRT" -> "usdtirt") to
// pick the native token, falling back to the compact lowercased canonical
// (BTC/IRT -> "btcirt"), then matches it against trading_chart_settings.ramzinex.
func (c *ramzinexPublic) resolvePairID(ctx context.Context, canonical string) (int, error) {
	native, ok := VenueSymbol(c.cfg, canonical)
	if !ok {
		native = strings.ToLower(strings.ReplaceAll(canonical, "/", ""))
	}
	native = strings.ToLower(native)

	pairs, err := c.loadPairs(ctx)
	if err != nil {
		return 0, err
	}
	for _, p := range pairs {
		if strings.ToLower(p.TradingChartSettings.Ramzinex) == native {
			return p.ID, nil
		}
	}
	return 0, fmt.Errorf("ramzinex: no pair ID for symbol %q (native %q)", canonical, native)
}

func (c *ramzinexPublic) GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	canonical, err := domain.NormalizeSymbol(symbol)
	if err != nil {
		canonical = strings.ToUpper(symbol)
	}

	ctx, cancel := RequestContext(ctx, c.cfg)
	defer cancel()

	pairID, err := c.resolvePairID(ctx, canonical)
	if err != nil {
		return domain.OrderBook{}, err
	}

	path := fmt.Sprintf("/exchange/api/v1.0/exchange/orderbooks/%d/buys_sells", pairID)
	raw, err := c.get(ctx, c.obURL+path, path)
	if err != nil {
		return domain.OrderBook{}, err
	}
	var payload ramzinexOrderBookResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return domain.OrderBook{}, fmt.Errorf("ramzinex orderbook decode: %w", err)
	}
	if payload.Status != 0 {
		return domain.OrderBook{}, &NormalizedAPIError{
			Exchange: ramzinexCode, Op: path, StatusCode: http.StatusOK,
			Code: strconv.Itoa(payload.Status), Category: CatUnknown,
			Message: fmt.Sprintf("ramzinex status %d", payload.Status),
		}
	}

	bids, err := ramzinexParseLevels(payload.Data.Buys)
	if err != nil {
		return domain.OrderBook{}, err
	}
	asks, err := ramzinexParseLevels(payload.Data.Sells)
	if err != nil {
		return domain.OrderBook{}, err
	}

	// priceMultiplier 0.1 converts rial (IRR) -> toman (IRT); BuildOrderBook sorts.
	book := BuildOrderBook(ramzinexCode, canonical, bids, asks,
		ramzinexRialToToman, "rest", time.Now())
	return book, nil
}

// --- SubscribeOrderBook (WS) ---

func (c *ramzinexPublic) SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error) {
	// WebSocket (Centrifuge "orderbook:{pair_id}") is present in the source but
	// deferred in this port; callers should poll GetOrderBook instead.
	return nil, Unsupported(ramzinexCode, "SubscribeOrderBook")
}

// --- helpers ---

// ramzinexParseLevels parses [[price, qty, ...], ...] rows where the first two
// elements are price and qty and may be encoded as numbers or strings.
func ramzinexParseLevels(rows [][]json.RawMessage) ([]domain.Level, error) {
	levels := make([]domain.Level, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			return nil, fmt.Errorf("ramzinex level: expected [price, qty], got %d elements", len(row))
		}
		price := ramzinexDecimalFromRaw(row[0])
		qty := ramzinexDecimalFromRaw(row[1])
		if price.IsZero() && qty.IsZero() {
			continue
		}
		levels = append(levels, domain.Level{Price: price, Quantity: qty})
	}
	return levels, nil
}

// ramzinexDecimalFromRaw decodes a JSON value that may be a quoted string or a
// bare number into a decimal, returning zero on failure.
func ramzinexDecimalFromRaw(raw json.RawMessage) decimal.Decimal {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return parseDec(s)
	}
	return parseDec(strings.TrimSpace(string(raw)))
}

func (c *ramzinexPublic) get(ctx context.Context, fullURL, op string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ramzinex GET %s: %w", op, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, &NormalizedAPIError{
			Exchange: ramzinexCode, Op: op, StatusCode: resp.StatusCode,
			Category:  classifyHTTPStatus(resp.StatusCode),
			Retryable: resp.StatusCode >= 500 || resp.StatusCode == 429,
			Message:   MaskBody(string(body)),
		}
	}
	return body, nil
}
