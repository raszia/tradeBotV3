package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
)

// Binance adapter — PUBLIC market data ONLY. Binance is the price REFERENCE in
// this system; the strategy never places orders on it, so there is no Binance
// private client (rule #8 keeps market-data and trading clients separate). It is
// ported/adapted from the sibling system's binance package.
//
// Capabilities: GetMarkets (exchangeInfo), GetOrderBook (REST depth),
// SubscribeOrderBook (combined WS depth stream). No private trading surface.
//
// Known limitations / quirks:
//   - exchangeInfo is a heavy call; intended for startup + periodic refresh, not
//     per-tick. GetMarkets filters to TRADING spot symbols only.
//   - The WS stream delivers partial-book depth snapshots (depth20@100ms); it is
//     not a maintained full-depth diff stream. Good enough for top-of-book pricing.

const (
	binanceCode          = "binance"
	binanceDefaultREST   = "https://api.binance.com"
	binanceDefaultWS     = "wss://stream.binance.com:9443"
	binanceDepthWSStream = "depth20@100ms"
)

func init() {
	Register(Registration{
		Code: binanceCode,
		Capabilities: Capabilities{
			MarketMetadata: true,
			OrderBookREST:  true,
			OrderBookWS:    true,
			// All private/order capabilities are false: Binance is read-only here.
		},
		NewPublic: newBinancePublic,
	})
}

type binancePublic struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
	wsURL   string
}

func newBinancePublic(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	cfg.Code = binanceCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = binanceDefaultREST
	}
	wsURL := cfg.WSURL
	if wsURL == "" {
		wsURL = binanceDefaultWS
	}
	return &binancePublic{cfg: cfg, http: hc, restURL: strings.TrimRight(restURL, "/"), wsURL: strings.TrimRight(wsURL, "/")}, nil
}

func (b *binancePublic) Name() string { return binanceCode }
func (b *binancePublic) Capabilities() Capabilities {
	r, _ := Lookup(binanceCode)
	return r.Capabilities
}

// --- GetMarkets ---

type binanceExchangeInfo struct {
	Symbols []struct {
		Symbol               string `json:"symbol"`
		Status               string `json:"status"`
		BaseAsset            string `json:"baseAsset"`
		BaseAssetPrecision   int    `json:"baseAssetPrecision"`
		QuoteAsset           string `json:"quoteAsset"`
		QuoteAssetPrecision  int    `json:"quoteAssetPrecision"`
		IsSpotTradingAllowed bool   `json:"isSpotTradingAllowed"`
		Filters              []struct {
			FilterType  string `json:"filterType"`
			TickSize    string `json:"tickSize"`
			StepSize    string `json:"stepSize"`
			MinQty      string `json:"minQty"`
			MinNotional string `json:"minNotional"`
			Notional    string `json:"notional"`
		} `json:"filters"`
	} `json:"symbols"`
}

func (b *binancePublic) GetMarkets(ctx context.Context) ([]NormalizedMarket, error) {
	ctx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()

	raw, err := b.get(ctx, "/api/v3/exchangeInfo")
	if err != nil {
		return nil, err
	}
	var info binanceExchangeInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("binance exchangeInfo decode: %w", err)
	}

	out := make([]NormalizedMarket, 0, len(info.Symbols))
	for _, s := range info.Symbols {
		canonical := strings.ToUpper(s.BaseAsset) + "/" + strings.ToUpper(s.QuoteAsset)
		m := NormalizedMarket{
			Exchange:          binanceCode,
			ExchangeSymbol:    strings.ToUpper(s.Symbol),
			CanonicalSymbol:   canonical,
			BaseAsset:         strings.ToUpper(s.BaseAsset),
			QuoteAsset:        strings.ToUpper(s.QuoteAsset),
			QuoteAssetType:    domain.QuoteAssetType(canonical),
			QuantityPrecision: s.BaseAssetPrecision,
			PricePrecision:    s.QuoteAssetPrecision,
			Tradable:          s.Status == "TRADING" && s.IsSpotTradingAllowed,
		}
		for _, f := range s.Filters {
			switch f.FilterType {
			case "PRICE_FILTER":
				m.TickSize = parseDec(f.TickSize)
			case "LOT_SIZE":
				m.StepSize = parseDec(f.StepSize)
				m.MinOrderQuantity = parseDec(f.MinQty)
			case "MIN_NOTIONAL", "NOTIONAL":
				if f.MinNotional != "" {
					m.MinOrderAmount = parseDec(f.MinNotional)
				} else if f.Notional != "" {
					m.MinOrderAmount = parseDec(f.Notional)
				}
			}
		}
		out = append(out, m)
	}
	return out, nil
}

// --- GetOrderBook (REST) ---

type binanceDepth struct {
	Bids [][2]string `json:"bids"`
	Asks [][2]string `json:"asks"`
}

func (b *binancePublic) GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	venueSym, ok := VenueSymbol(b.cfg, symbol)
	if !ok {
		// fall back to compact canonical (BTC/USDT -> BTCUSDT)
		venueSym = strings.ReplaceAll(strings.ToUpper(symbol), "/", "")
	}
	canonical, _ := domain.NormalizeSymbol(symbol)
	if canonical == "" {
		canonical = strings.ToUpper(symbol)
	}

	ctx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()
	raw, err := b.get(ctx, "/api/v3/depth?limit=20&symbol="+venueSym)
	if err != nil {
		return domain.OrderBook{}, err
	}
	var d binanceDepth
	if err := json.Unmarshal(raw, &d); err != nil {
		return domain.OrderBook{}, fmt.Errorf("binance depth decode: %w", err)
	}
	book := BuildOrderBook(binanceCode, canonical,
		levelsFromPairs(d.Bids), levelsFromPairs(d.Asks),
		decimal.NewFromInt(1), "rest", time.Now())
	return book, nil
}

// --- SubscribeOrderBook (WS) ---

type binanceCombinedMsg struct {
	Stream string       `json:"stream"`
	Data   binanceDepth `json:"data"`
}

func (b *binancePublic) SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("binance SubscribeOrderBook: no symbols")
	}
	streams := make([]string, 0, len(symbols))
	venueToCanonical := map[string]string{}
	for _, s := range symbols {
		venueSym, ok := VenueSymbol(b.cfg, s)
		if !ok {
			venueSym = strings.ReplaceAll(strings.ToUpper(s), "/", "")
		}
		canonical, _ := domain.NormalizeSymbol(s)
		lower := strings.ToLower(venueSym)
		streams = append(streams, lower+"@"+binanceDepthWSStream)
		venueToCanonical[lower] = canonical
	}
	url := b.wsURL + "/stream?streams=" + strings.Join(streams, "/")

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("binance ws dial: %w", err)
	}

	out := make(chan domain.OrderBook, 64)
	go func() {
		defer close(out)
		defer conn.Close()
		go func() { <-ctx.Done(); conn.Close() }()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var msg binanceCombinedMsg
			if json.Unmarshal(data, &msg) != nil {
				continue
			}
			streamSym := strings.SplitN(msg.Stream, "@", 2)[0]
			canonical := venueToCanonical[streamSym]
			book := BuildOrderBook(binanceCode, canonical,
				levelsFromPairs(msg.Data.Bids), levelsFromPairs(msg.Data.Asks),
				decimal.NewFromInt(1), "ws", time.Now())
			select {
			case out <- book:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// --- helpers ---

func (b *binancePublic) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.restURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binance GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, ApplyRateLimitSignals(&NormalizedAPIError{
			Exchange: binanceCode, Op: path, StatusCode: resp.StatusCode,
			Category: classifyHTTPStatus(resp.StatusCode), Retryable: resp.StatusCode >= 500 || resp.StatusCode == 429,
			Message: MaskBody(string(body)),
		}, resp.Header)
	}
	return body, nil
}

func levelsFromPairs(pairs [][2]string) []domain.Level {
	out := make([]domain.Level, 0, len(pairs))
	for _, p := range pairs {
		price := parseDec(p[0])
		qty := parseDec(p[1])
		if price.IsZero() && qty.IsZero() {
			continue
		}
		out = append(out, domain.Level{Price: price, Quantity: qty})
	}
	return out
}

func parseDec(s string) decimal.Decimal {
	if s == "" {
		return decimal.Zero
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero
	}
	return d
}

// classifyHTTPStatus maps an HTTP status to an ErrorCategory (shared by adapters).
func classifyHTTPStatus(status int) ErrorCategory {
	switch {
	case status == 401 || status == 403:
		return CatAuth
	case status == 404:
		return CatNotFound
	case status == 429:
		return CatRateLimit
	case status >= 500:
		return CatServer
	case status >= 400:
		return CatBadRequest
	default:
		return CatUnknown
	}
}
