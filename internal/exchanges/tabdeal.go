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

// Tabdeal adapter — PUBLIC market data ONLY. Ported/adapted from the sibling
// iranArb system's tabdeal package. There is no Tabdeal private client here
// (rule #8 keeps market-data and trading clients separate); NewPrivate is nil.
//
// Capabilities: GetOrderBook (REST order_book snapshot). GetMarkets is NOT
// supported: the source had no markets/symbols list endpoint (it built its
// symbol set from config), so we report MarketMetadata=false and return
// Unsupported. SubscribeOrderBook is unsupported: the source was REST-poll only
// (no order-book WebSocket), so it is unsupported here too.
//
// Known limitations / quirks:
//   - QUOTE UNIT: Tabdeal's /r/api/order_book quotes in TOMAN (IRT), the system's
//     canonical Iranian quote, so the priceMultiplier is 1 (no rial->toman
//     rescale). Contrast Ramzinex/Nobitex which quote in rial.
//   - SYMBOL FORMAT: the venue uses an underscore-joined, upper-cased native
//     symbol, e.g. "USDT_IRT". cfg.Symbols may map canonical "USDT/IRT" ->
//     "USDT_IRT"; otherwise we derive the native symbol by replacing "/" with
//     "_" and upper-casing.
//   - LEVELS: asks/bids are objects {"price":"...","amount":"..."} with decimal
//     strings; rows that fail to parse are skipped (the source skipped them too).
//   - NO WEBSOCKET: the source polled REST only; there is no order-book stream.
//   - ERROR FORMAT: errors surface as non-2xx HTTP statuses (no structured venue
//     error envelope), mapped via classifyHTTPStatus.

const (
	tabdealCode        = "tabdeal"
	tabdealDefaultREST = "https://api1.tabdeal.org"
)

func init() {
	Register(Registration{
		Code: tabdealCode,
		Capabilities: Capabilities{
			MarketMetadata: false, // no markets/symbols list endpoint in source
			OrderBookREST:  true,  // GetOrderBook via /r/api/order_book
			OrderBookWS:    false, // REST-poll only in source; no WS
			// All private/order capabilities are false: public-only adapter.
		},
		NewPublic: newTabdealPublic,
		// NewPrivate intentionally nil — no trading surface for Tabdeal here.
	})
}

type tabdealPublic struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
}

func newTabdealPublic(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	cfg.Code = tabdealCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = tabdealDefaultREST
	}
	return &tabdealPublic{
		cfg:     cfg,
		http:    hc,
		restURL: strings.TrimRight(restURL, "/"),
	}, nil
}

func (c *tabdealPublic) Name() string { return tabdealCode }
func (c *tabdealPublic) Capabilities() Capabilities {
	r, _ := Lookup(tabdealCode)
	return r.Capabilities
}

// --- GetMarkets (unsupported) ---

func (c *tabdealPublic) GetMarkets(ctx context.Context) ([]NormalizedMarket, error) {
	// The source had no markets/symbols list endpoint; symbols came from config.
	return nil, Unsupported(tabdealCode, "GetMarkets")
}

// --- GetOrderBook (REST) ---

type tabdealOrderBookResponse struct {
	Asks []tabdealLevel `json:"asks"`
	Bids []tabdealLevel `json:"bids"`
}

type tabdealLevel struct {
	Price  string `json:"price"`
	Amount string `json:"amount"`
}

// tabdealVenueSymbol resolves the venue-native symbol for a canonical symbol:
// cfg.Symbols mapping if present, else BASE/QUOTE -> BASE_QUOTE upper-cased.
func tabdealVenueSymbol(cfg ClientConfig, canonical string) string {
	if native, ok := VenueSymbol(cfg, canonical); ok && native != "" {
		return strings.ToUpper(native)
	}
	return strings.ToUpper(strings.ReplaceAll(canonical, "/", "_"))
}

func (c *tabdealPublic) GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	canonical, err := domain.NormalizeSymbol(symbol)
	if err != nil {
		canonical = strings.ToUpper(symbol)
	}
	native := tabdealVenueSymbol(c.cfg, canonical)

	ctx, cancel := RequestContext(ctx, c.cfg)
	defer cancel()

	path := "/r/api/order_book?symbol=" + native
	raw, err := c.get(ctx, path)
	if err != nil {
		return domain.OrderBook{}, err
	}
	var payload tabdealOrderBookResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return domain.OrderBook{}, fmt.Errorf("tabdeal order_book decode: %w", err)
	}

	bids := tabdealParseLevels(payload.Bids)
	asks := tabdealParseLevels(payload.Asks)

	// priceMultiplier 1: Tabdeal already quotes in toman (IRT). BuildOrderBook sorts.
	book := BuildOrderBook(tabdealCode, canonical, bids, asks,
		decimal.NewFromInt(1), "rest", time.Now())
	return book, nil
}

// --- SubscribeOrderBook (WS — unsupported) ---

func (c *tabdealPublic) SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error) {
	// The source had no order-book WebSocket (REST poll only).
	return nil, Unsupported(tabdealCode, "SubscribeOrderBook")
}

// --- helpers ---

// tabdealParseLevels converts {price,amount} entries to levels, skipping any
// row that fails to parse (matching the source's lenient behavior).
func tabdealParseLevels(rows []tabdealLevel) []domain.Level {
	levels := make([]domain.Level, 0, len(rows))
	for _, r := range rows {
		price, perr := decimal.NewFromString(r.Price)
		qty, qerr := decimal.NewFromString(r.Amount)
		if perr != nil || qerr != nil {
			continue
		}
		levels = append(levels, domain.Level{Price: price, Quantity: qty})
	}
	return levels
}

func (c *tabdealPublic) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.restURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tabdeal GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, ApplyRateLimitSignals(&NormalizedAPIError{
			Exchange: tabdealCode, Op: path, StatusCode: resp.StatusCode,
			Category:  classifyHTTPStatus(resp.StatusCode),
			Retryable: resp.StatusCode >= 500 || resp.StatusCode == 429,
			Message:   MaskBody(string(body)),
		}, resp.Header)
	}
	return body, nil
}
