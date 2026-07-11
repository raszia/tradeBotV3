package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/execution"
)

// Wallex adapter — Iranian exchange, IRT-quoted spot. Ported from the sibling
// system's wallex package (REST endpoints, auth, payloads and status mapping are
// preserved faithfully). Provides BOTH a public market-data client and a private
// trading client.
//
// Known limitations / quirks (READ BEFORE USING):
//   - ORDERS ARE KEYED BY client_id, NOT an exchange order id. Wallex's
//     cancel/get/order endpoints take the CLIENT order id (the value we send as
//     client_id on PlaceOrder), and Wallex does not surface a distinct numeric
//     exchange order id in its order objects. Therefore CancelOrder(exchangeOrderID)
//     and GetOrder(exchangeOrderID) treat their argument as the CLIENT order id,
//     and OrderAck/OrderStatus set BOTH ExchangeOrderID and ClientOrderID to that
//     same client id. Capabilities.ClientOrderID is true and FetchByOrderID is
//     true (because "by order id" here means "by client id").
//   - TMN→IRT: Wallex denominates the Iranian Toman as "TMN" and uses native
//     symbols like "USDTTMN". We normalize TMN→IRT everywhere (assets, symbols,
//     markets) so it matches the rest of the system. Native symbols ending in TMN
//     map to canonical BASE/IRT.
//   - Wallex already quotes in Toman/IRT (NOT rial/IRR), so order books need NO
//     price rescale: priceMultiplier = 1.
//   - POLLING ONLY for order status: there is no private order-update WebSocket,
//     so SubscribeOrderUpdates returns ErrUnsupported and OrderStatusPoll=true.
//     The public order-book WebSocket exists on Wallex but is deferred here
//     (OrderBookWS=false; SubscribeOrderBook returns ErrUnsupported).
//   - Auth: a single "X-API-Key" header carries the API key (no HMAC signing, no
//     passphrase). The key is masked automatically by the logging transport.
//   - Rate limits: Wallex rate-limits per endpoint; a 429 is mapped to a
//     retryable rate-limit error wrapping execution.ErrRateLimited.
//   - Error format: non-2xx responses, or a 2xx body with success=false, are
//     surfaced as *NormalizedAPIError. Wallex error bodies carry either a
//     top-level "message" or a nested {"error":{"code","message"}}.
//   - PlaceOrder does NOT impose IOC/market/post-only. OrderType and TimeInForce
//     come straight from the request (owner-defined strategy). NOTE: the live
//     Wallex order endpoint as ported supports LIMIT orders with an explicit
//     price; TimeInForce is forwarded when set but Wallex has no native IOC.

const (
	wallexCode        = "wallex"
	wallexDefaultREST = "https://api.wallex.ir"
)

func init() {
	Register(Registration{
		Code: wallexCode,
		Capabilities: Capabilities{
			MarketMetadata:        true,
			OrderBookREST:         true,
			OrderBookWS:           false, // public WS exists on Wallex but is deferred here
			BalanceFetch:          true,
			PlaceOrder:            true,
			CancelByOrderID:       true, // by client_id (see file header)
			FetchByOrderID:        true, // by client_id (see file header)
			FetchOpenOrders:       true,
			RecentFills:           false,
			OrderUpdatesWS:        false, // no private order WS
			OrderStatusPoll:       true,  // status available via GetOrder polling
			ClientOrderID:         true,  // Wallex identifies orders by client_id
			LookupByClientOrderID: true,  // GetOrder(id) here IS a lookup by client_id (see file header)
		},
		NewPublic:  newWallexPublic,
		NewPrivate: newWallexPrivate,
	})
}

// ─── shared response shapes ────────────────────────────────────────────────────

type wallexAPIError struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// wallexDepthLevel is one order-book level. price/quantity are RawMessage because
// Wallex returns them sometimes as JSON strings and sometimes as numbers.
type wallexDepthLevel struct {
	Price    json.RawMessage `json:"price"`
	Quantity json.RawMessage `json:"quantity"`
}

func (l wallexDepthLevel) level() domain.Level {
	return domain.Level{Price: wallexDec(l.Price), Quantity: wallexDec(l.Quantity)}
}

// wallexNativeSymbol maps a canonical/venue-configured symbol to Wallex's native
// form, e.g. "USDT/IRT" → "USDTTMN", "BTC/USDT" → "BTCUSDT".
func wallexNativeSymbol(symbol string) string {
	n := strings.ToUpper(strings.TrimSpace(symbol))
	switch {
	case strings.HasSuffix(n, "/IRT"):
		return strings.TrimSuffix(n, "/IRT") + "TMN"
	case strings.HasSuffix(n, "_IRT"):
		return strings.TrimSuffix(n, "_IRT") + "TMN"
	case strings.HasSuffix(n, "-IRT"):
		return strings.TrimSuffix(n, "-IRT") + "TMN"
	case strings.HasSuffix(n, "IRT"):
		return strings.TrimSuffix(n, "IRT") + "TMN"
	}
	n = strings.ReplaceAll(n, "/", "")
	n = strings.ReplaceAll(n, "_", "")
	return strings.ReplaceAll(n, "-", "")
}

// wallexInternalSymbol reverses wallexNativeSymbol: "USDTTMN" → "USDT/IRT",
// "BTCUSDT" → "BTC/USDT". Wallex's TMN suffix denotes Toman, normalized to IRT.
func wallexInternalSymbol(native string) string {
	n := strings.ToUpper(strings.TrimSpace(native))
	if strings.HasSuffix(n, "TMN") {
		return strings.TrimSuffix(n, "TMN") + "/IRT"
	}
	if strings.HasSuffix(n, "USDT") {
		return strings.TrimSuffix(n, "USDT") + "/USDT"
	}
	return n
}

// wallexNormalizeAsset normalizes a Wallex asset code (TMN→IRT, upper-cased).
func wallexNormalizeAsset(asset string) string {
	a := strings.ToUpper(strings.TrimSpace(asset))
	if a == "TMN" {
		return domain.CurrencyIRT
	}
	return a
}

// wallexDec parses a Wallex numeric field (string or number) into a decimal.
func wallexDec(raw json.RawMessage) decimal.Decimal {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return decimal.Zero
	}
	s = strings.Trim(s, `"`)
	return parseDec(s)
}

func wallexIntFromRaw(raw json.RawMessage) int {
	return int(wallexDec(raw).IntPart())
}

func wallexParseTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.000000Z", time.RFC3339} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// wallexBaseURL resolves the REST base URL from cfg, defaulting to the public host.
func wallexBaseURL(cfg ClientConfig) string {
	if cfg.BaseURL != "" {
		return strings.TrimRight(cfg.BaseURL, "/")
	}
	return wallexDefaultREST
}

// ─── PUBLIC client ─────────────────────────────────────────────────────────────

type wallexPublic struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
}

func newWallexPublic(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	cfg.Code = wallexCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &wallexPublic{cfg: cfg, http: hc, restURL: wallexBaseURL(cfg)}, nil
}

func (w *wallexPublic) Name() string { return wallexCode }
func (w *wallexPublic) Capabilities() Capabilities {
	r, _ := Lookup(wallexCode)
	return r.Capabilities
}

// wallexMarketsResponse is /v1/markets. stepSize/tickSize are DECIMAL PLACE
// COUNTS (not increments): QuantityStep = 10^-stepSize, PriceTick = 10^-tickSize.
type wallexMarketsResponse struct {
	Result struct {
		Symbols map[string]wallexMarketSpec `json:"symbols"`
	} `json:"result"`
}

type wallexMarketSpec struct {
	Symbol      string          `json:"symbol"`
	BaseAsset   string          `json:"baseAsset"`
	QuoteAsset  string          `json:"quoteAsset"`
	StepSize    json.RawMessage `json:"stepSize"` // qty decimal places
	TickSize    json.RawMessage `json:"tickSize"` // price decimal places
	MinQty      json.RawMessage `json:"minQty"`
	MaxQty      json.RawMessage `json:"maxQty"`
	MinNotional json.RawMessage `json:"minNotional"`
}

func (w *wallexPublic) GetMarkets(ctx context.Context) ([]NormalizedMarket, error) {
	ctx, cancel := RequestContext(ctx, w.cfg)
	defer cancel()

	raw, err := w.get(ctx, "GetMarkets", "/v1/markets")
	if err != nil {
		return nil, err
	}
	var resp wallexMarketsResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("wallex markets decode: %w", err)
	}

	out := make([]NormalizedMarket, 0, len(resp.Result.Symbols))
	for nativeKey, m := range resp.Result.Symbols {
		base := strings.ToUpper(strings.TrimSpace(m.BaseAsset))
		quote := wallexNormalizeAsset(m.QuoteAsset)
		if base == "" || quote == "" {
			continue
		}
		canonical := base + "/" + quote
		exchangeSymbol := strings.ToUpper(strings.TrimSpace(m.Symbol))
		if exchangeSymbol == "" {
			exchangeSymbol = strings.ToUpper(strings.TrimSpace(nativeKey))
		}
		stepDigits := wallexIntFromRaw(m.StepSize)
		tickDigits := wallexIntFromRaw(m.TickSize)
		out = append(out, NormalizedMarket{
			Exchange:          wallexCode,
			ExchangeSymbol:    exchangeSymbol,
			CanonicalSymbol:   canonical,
			BaseAsset:         base,
			QuoteAsset:        quote,
			QuoteAssetType:    domain.QuoteAssetType(canonical),
			PricePrecision:    tickDigits,
			QuantityPrecision: stepDigits,
			// stepSize/tickSize are decimal place counts → 10^-n increments.
			TickSize:         decimal.New(1, int32(-tickDigits)),
			StepSize:         decimal.New(1, int32(-stepDigits)),
			MinOrderQuantity: wallexDec(m.MinQty),
			MinOrderAmount:   wallexDec(m.MinNotional),
			Tradable:         true,
		})
	}
	return out, nil
}

// wallexDepthResponse is /v1/depth?symbol=. Wallex quotes in Toman (IRT) already.
type wallexDepthResponse struct {
	Result struct {
		Ask []wallexDepthLevel `json:"ask"`
		Bid []wallexDepthLevel `json:"bid"`
	} `json:"result"`
}

func (w *wallexPublic) GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	venueSym, ok := VenueSymbol(w.cfg, symbol)
	if ok {
		venueSym = wallexNativeSymbol(venueSym)
	} else {
		venueSym = wallexNativeSymbol(symbol)
	}
	canonical, _ := domain.NormalizeSymbol(symbol)
	if canonical == "" {
		canonical = strings.ToUpper(symbol)
	}

	ctx, cancel := RequestContext(ctx, w.cfg)
	defer cancel()
	raw, err := w.get(ctx, "GetOrderBook", "/v1/depth?symbol="+url.QueryEscape(venueSym))
	if err != nil {
		return domain.OrderBook{}, err
	}
	var d wallexDepthResponse
	if err := json.Unmarshal(raw, &d); err != nil {
		return domain.OrderBook{}, fmt.Errorf("wallex depth decode: %w", err)
	}
	bids := wallexLevels(d.Result.Bid)
	asks := wallexLevels(d.Result.Ask)
	// Wallex already quotes in Toman/IRT → priceMultiplier = 1 (no rescale).
	book := BuildOrderBook(wallexCode, canonical, bids, asks, decimal.NewFromInt(1), "rest", time.Now())
	return book, nil
}

func wallexLevels(items []wallexDepthLevel) []domain.Level {
	out := make([]domain.Level, 0, len(items))
	for _, it := range items {
		lv := it.level()
		if lv.Price.IsZero() && lv.Quantity.IsZero() {
			continue
		}
		out = append(out, lv)
	}
	return out
}

// SubscribeOrderBook is unsupported here: Wallex's public WS exists but is
// deferred (OrderBookWS=false).
func (w *wallexPublic) SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error) {
	return nil, Unsupported(wallexCode, "order-book WebSocket")
}

func (w *wallexPublic) get(ctx context.Context, op, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.restURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := w.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("wallex GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, wallexHTTPError(op, resp.StatusCode, body)
	}
	return body, nil
}

// ─── PRIVATE client ────────────────────────────────────────────────────────────

type wallexPrivate struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
}

func newWallexPrivate(cfg ClientConfig, logger *IOLogger) (PrivateClient, error) {
	cfg.Code = wallexCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &wallexPrivate{cfg: cfg, http: hc, restURL: wallexBaseURL(cfg)}, nil
}

func (w *wallexPrivate) Name() string { return wallexCode }
func (w *wallexPrivate) Capabilities() Capabilities {
	r, _ := Lookup(wallexCode)
	return r.Capabilities
}

// apiKey fetches the (decrypted) API key from the credential provider.
func (w *wallexPrivate) apiKey(ctx context.Context) (string, error) {
	if w.cfg.Creds == nil {
		return "", &NormalizedAPIError{
			Exchange: wallexCode, Op: "credentials", Category: CatAuth,
			Message: "no credential provider configured", Err: execution.ErrAuthFailed,
		}
	}
	creds, err := w.cfg.Creds.Credentials(ctx, wallexCode)
	if err != nil {
		return "", fmt.Errorf("wallex credentials: %w", err)
	}
	if creds.APIKey == "" {
		return "", &NormalizedAPIError{
			Exchange: wallexCode, Op: "credentials", Category: CatAuth,
			Message: "missing api key", Err: execution.ErrAuthFailed,
		}
	}
	return creds.APIKey, nil
}

// doPrivate issues an authenticated request. Auth is a single X-API-Key header
// (faithful to the Wallex source). body is JSON-marshalled when non-nil. Returns
// the raw response body (already masked in logs by the transport).
func (w *wallexPrivate) doPrivate(ctx context.Context, op, method, path string, body any) ([]byte, error) {
	key, err := w.apiKey(ctx)
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	ctx, cancel := RequestContext(ctx, w.cfg)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, w.restURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", key) // Wallex auth header (masked in logs)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := w.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("wallex %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return raw, wallexHTTPError(op, resp.StatusCode, raw)
	}
	return raw, nil
}

// --- GetBalances ---

type wallexBalanceResponse struct {
	Success bool `json:"success"`
	Result  struct {
		Balances map[string]struct {
			Asset  string          `json:"asset"`
			Value  json.RawMessage `json:"value"`
			Locked json.RawMessage `json:"locked"`
		} `json:"balances"`
	} `json:"result"`
}

func (w *wallexPrivate) GetBalances(ctx context.Context) ([]domain.Balance, error) {
	raw, err := w.doPrivate(ctx, "GetBalances", http.MethodGet, "/v1/account/balances", nil)
	if err != nil {
		return nil, err
	}
	var payload wallexBalanceResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("wallex balances decode: %w", err)
	}
	now := time.Now().UTC()
	out := make([]domain.Balance, 0, len(payload.Result.Balances))
	for asset, b := range payload.Result.Balances {
		name := asset
		if b.Asset != "" {
			name = b.Asset
		}
		name = wallexNormalizeAsset(name) // TMN→IRT
		available := wallexDec(b.Value)
		locked := wallexDec(b.Locked)
		out = append(out, domain.Balance{
			Exchange:  wallexCode,
			Asset:     name,
			Available: available,
			Locked:    locked,
			Total:     available.Add(locked),
			UpdatedAt: now,
		})
	}
	return out, nil
}

// --- PlaceOrder ---

// wallexPlaceOrderRequest is the Wallex order body (faithful to the source):
// symbol / type / side / price / quantity / client_id.
type wallexPlaceOrderRequest struct {
	Symbol      string `json:"symbol"`
	Type        string `json:"type"`
	Side        string `json:"side"`
	Price       string `json:"price,omitempty"`
	Quantity    string `json:"quantity,omitempty"`
	ClientID    string `json:"client_id,omitempty"`
	TimeInForce string `json:"timeInForce,omitempty"`
}

type wallexOrderResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message,omitempty"`
	Error   *wallexAPIError `json:"error,omitempty"`
	Result  wallexOrder     `json:"result"`
}

func (r wallexOrderResponse) errorMessage() string {
	if r.Error != nil && r.Error.Message != "" {
		if r.Error.Code != 0 {
			return fmt.Sprintf("[%d] %s", r.Error.Code, r.Error.Message)
		}
		return r.Error.Message
	}
	if r.Message != "" {
		return r.Message
	}
	return "unknown wallex order error"
}

type wallexOrder struct {
	Symbol        string          `json:"symbol"`
	Type          string          `json:"type"`
	Side          string          `json:"side"`
	Price         json.RawMessage `json:"price"`
	OrigQty       json.RawMessage `json:"origQty"`
	OrigSum       json.RawMessage `json:"origSum"`
	ExecutedPrice json.RawMessage `json:"executedPrice"`
	ExecutedQty   json.RawMessage `json:"executedQty"`
	ExecutedSum   json.RawMessage `json:"executedSum"`
	Status        string          `json:"status"`
	Active        bool            `json:"active"`
	ClientOrderID string          `json:"clientOrderId"`
	CreatedAt     string          `json:"created_at"`
	UpdatedAt     string          `json:"updated_at"`
}

func (w *wallexPrivate) PlaceOrder(ctx context.Context, req execution.OrderRequest) (execution.OrderAck, error) {
	native := wallexNativeSymbol(w.venueSymbol(req.Symbol))

	// Order type / time-in-force come straight from the request — this layer
	// imposes NO strategy (no forced IOC/market/post-only). Default to LIMIT
	// only when the caller left OrderType empty, mirroring the source's LIMIT
	// order shape (Wallex's order endpoint as ported is price-based).
	orderType := strings.ToUpper(strings.TrimSpace(req.OrderType))
	if orderType == "" {
		orderType = "LIMIT"
	}
	body := wallexPlaceOrderRequest{
		Symbol:      native,
		Type:        orderType,
		Side:        strings.ToUpper(strings.TrimSpace(req.Side)),
		Quantity:    req.Quantity.String(),
		ClientID:    req.ClientOrderID,
		TimeInForce: strings.TrimSpace(req.TimeInForce), // forwarded as-is; empty omitted
	}
	if req.LimitPrice.IsPositive() {
		body.Price = req.LimitPrice.String()
	}

	raw, err := w.doPrivate(ctx, "PlaceOrder", http.MethodPost, "/v1/account/orders", body)
	if err != nil {
		return execution.OrderAck{}, err
	}
	var payload wallexOrderResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return execution.OrderAck{}, fmt.Errorf("wallex place order decode: %w", err)
	}
	if !payload.Success {
		return execution.OrderAck{}, &NormalizedAPIError{
			Exchange: wallexCode, Op: "PlaceOrder", StatusCode: http.StatusOK,
			Category: CatBadRequest, Message: MaskBody(payload.errorMessage()),
		}
	}
	return wallexOrderToAck(payload.Result, req.Symbol, req.Side, req.ClientOrderID), nil
}

// venueSymbol resolves the venue-native symbol for a canonical symbol via
// cfg.Symbols, falling back to the canonical symbol itself.
func (w *wallexPrivate) venueSymbol(symbol string) string {
	if v, ok := VenueSymbol(w.cfg, symbol); ok {
		return v
	}
	return symbol
}

// --- CancelOrder ---
//
// QUIRK: Wallex cancels by CLIENT order id. The exchangeOrderID argument is
// treated as the client id (Wallex exposes no separate exchange order id).

type wallexSimpleResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message,omitempty"`
	Error   *wallexAPIError `json:"error,omitempty"`
}

func (r wallexSimpleResponse) errorMessage() string {
	if r.Error != nil && r.Error.Message != "" {
		if r.Error.Code != 0 {
			return fmt.Sprintf("[%d] %s", r.Error.Code, r.Error.Message)
		}
		return r.Error.Message
	}
	if r.Message != "" {
		return r.Message
	}
	return "unknown wallex error"
}

func (w *wallexPrivate) CancelOrder(ctx context.Context, exchangeOrderID string) error {
	// exchangeOrderID is the CLIENT order id for Wallex (see file header).
	clientOrderID := exchangeOrderID
	if clientOrderID == "" {
		return &NormalizedAPIError{
			Exchange: wallexCode, Op: "CancelOrder", Category: CatBadRequest,
			Message: "wallex cancel requires a client order id", Err: execution.ErrOrderUnknown,
		}
	}
	path := "/v1/account/orders?clientOrderId=" + url.QueryEscape(clientOrderID)
	raw, err := w.doPrivate(ctx, "CancelOrder", http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	var payload wallexSimpleResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("wallex cancel decode: %w", err)
	}
	if !payload.Success {
		return &NormalizedAPIError{
			Exchange: wallexCode, Op: "CancelOrder", StatusCode: http.StatusOK,
			Category: CatBadRequest, Message: MaskBody(payload.errorMessage()),
		}
	}
	return nil
}

// --- GetOrder ---
//
// QUIRK: Wallex looks up by CLIENT order id. The exchangeOrderID argument is
// treated as the client id.

// GetOrderByClientOrderID implements exchanges.ClientOrderLookup. On Wallex the client order id
// IS the order key, so this is exactly GetOrder.
func (w *wallexPrivate) GetOrderByClientOrderID(ctx context.Context, clientOrderID string) (execution.OrderStatus, error) {
	return w.GetOrder(ctx, clientOrderID)
}

func (w *wallexPrivate) GetOrder(ctx context.Context, exchangeOrderID string) (execution.OrderStatus, error) {
	clientOrderID := exchangeOrderID
	if clientOrderID == "" {
		return execution.OrderStatus{}, &NormalizedAPIError{
			Exchange: wallexCode, Op: "GetOrder", Category: CatBadRequest,
			Message: "wallex get order requires a client order id", Err: execution.ErrOrderUnknown,
		}
	}
	raw, err := w.doPrivate(ctx, "GetOrder", http.MethodGet, "/v1/account/orders/"+url.PathEscape(clientOrderID), nil)
	if err != nil {
		return execution.OrderStatus{}, err
	}
	var payload wallexOrderResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return execution.OrderStatus{}, fmt.Errorf("wallex get order decode: %w", err)
	}
	if !payload.Success {
		return execution.OrderStatus{}, &NormalizedAPIError{
			Exchange: wallexCode, Op: "GetOrder", StatusCode: http.StatusOK,
			Category: CatBadRequest, Message: MaskBody(payload.errorMessage()), Err: execution.ErrOrderUnknown,
		}
	}
	return wallexOrderToStatus(payload.Result, "", "", clientOrderID), nil
}

// --- GetOpenOrders ---

type wallexOpenOrdersResponse struct {
	Success bool `json:"success"`
	Result  struct {
		Orders []wallexOrder `json:"orders"`
	} `json:"result"`
}

func (w *wallexPrivate) GetOpenOrders(ctx context.Context, symbol string) ([]execution.OrderStatus, error) {
	raw, err := w.doPrivate(ctx, "GetOpenOrders", http.MethodGet, "/v1/account/openOrders", nil)
	if err != nil {
		return nil, err
	}
	var payload wallexOpenOrdersResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("wallex open orders decode: %w", err)
	}
	filter := ""
	if symbol != "" {
		if c, e := domain.NormalizeSymbol(symbol); e == nil {
			filter = c
		} else {
			filter = strings.ToUpper(symbol)
		}
	}
	out := make([]execution.OrderStatus, 0, len(payload.Result.Orders))
	for _, o := range payload.Result.Orders {
		st := wallexOrderToStatus(o, "", "", o.ClientOrderID)
		if filter != "" && st.Symbol != filter {
			continue
		}
		out = append(out, st)
	}
	return out, nil
}

// SubscribeOrderUpdates is unsupported: Wallex has no private order-update WS.
// Status is obtained by polling GetOrder (OrderStatusPoll=true).
func (w *wallexPrivate) SubscribeOrderUpdates(ctx context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	return nil, Unsupported(wallexCode, "order-update WebSocket (poll via GetOrder)")
}

// ─── status mapping ────────────────────────────────────────────────────────────

// mapWallexOrderState maps a Wallex order status (+ active flag and fill
// progress) to a normalized execution.NormalizedOrderState. Ported faithfully
// from the source: Wallex relies heavily on the `active` flag — active=false
// means the order is no longer open regardless of status string.
func mapWallexOrderState(status string, active bool, filled, requested decimal.Decimal) execution.NormalizedOrderState {
	status = strings.ToUpper(strings.TrimSpace(status))

	// Full fill is full fill regardless of active/status.
	if requested.IsPositive() && filled.GreaterThanOrEqual(requested) {
		return execution.StateFilled
	}

	switch status {
	case "FILLED":
		return execution.StateFilled

	case "CANCELED", "CANCELLED":
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled

	case "NEW", "ACTIVE", "OPEN":
		if active {
			if filled.IsPositive() {
				return execution.StatePartiallyFilled
			}
			return execution.StateOpen
		}
		// active=false ⇒ no longer open.
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled

	case "PARTIALLY_FILLED", "PARTIAL":
		if active {
			return execution.StatePartiallyFilled
		}
		return execution.StatePartiallyCanceled

	case "EXPIRED":
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateExpired

	case "REJECTED":
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateRejected

	default:
		// Unknown status: lean on the active flag (matches source behavior).
		if active {
			if filled.IsPositive() {
				return execution.StatePartiallyFilled
			}
			return execution.StateOpen
		}
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled
	}
}

// wallexOrderToAck converts a Wallex order object into an OrderAck. fallback*
// values fill in fields Wallex omits in the place response.
func wallexOrderToAck(o wallexOrder, fallbackSymbol, fallbackSide, fallbackClientID string) execution.OrderAck {
	reqQty := wallexDec(o.OrigQty)
	limit := wallexDec(o.Price)
	filled := wallexDec(o.ExecutedQty)
	avg := wallexDec(o.ExecutedPrice)
	executedQuote := wallexDec(o.ExecutedSum)

	symbol := wallexResolveSymbol(o.Symbol, fallbackSymbol)
	side := wallexResolveSide(o.Side, fallbackSide)

	clientID := strings.TrimSpace(o.ClientOrderID)
	if clientID == "" {
		clientID = strings.TrimSpace(fallbackClientID)
	}

	state := mapWallexOrderState(o.Status, o.Active, filled, reqQty)

	at := wallexParseTime(o.UpdatedAt)
	if at.IsZero() {
		at = wallexParseTime(o.CreatedAt)
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	if executedQuote.IsZero() && avg.IsPositive() && filled.IsPositive() {
		executedQuote = avg.Mul(filled)
	}
	if avg.IsZero() && executedQuote.IsPositive() && filled.IsPositive() {
		avg = executedQuote.Div(filled)
	}

	return execution.OrderAck{
		// Wallex keys by client_id and exposes no separate exchange id; mirror
		// the client id into ExchangeOrderID so callers keyed on either field work.
		ExchangeOrderID: clientID,
		ClientOrderID:   clientID,
		Symbol:          symbol,
		Side:            side,
		RequestedQty:    reqQty,
		LimitPrice:      limit,
		Status:          state,
		RawStatus:       o.Status,
		FilledQty:       filled,
		AvgPrice:        avg,
		ExecutedQuote:   executedQuote,
		Active:          o.Active,
		AckedAt:         at,
		Raw:             MaskBody(string(mustJSON(o))),
	}
}

// wallexOrderToStatus converts a Wallex order object into an OrderStatus.
func wallexOrderToStatus(o wallexOrder, fallbackSymbol, fallbackSide, fallbackClientID string) execution.OrderStatus {
	ack := wallexOrderToAck(o, fallbackSymbol, fallbackSide, fallbackClientID)
	remaining := ack.RequestedQty.Sub(ack.FilledQty)
	if remaining.IsNegative() {
		remaining = decimal.Zero
	}
	return execution.OrderStatus{
		ExchangeOrderID: ack.ExchangeOrderID,
		ClientOrderID:   ack.ClientOrderID,
		Symbol:          ack.Symbol,
		Side:            ack.Side,
		Status:          ack.Status,
		IntendedQty:     ack.RequestedQty,
		FilledQty:       ack.FilledQty,
		RemainingQty:    remaining,
		AvgPrice:        ack.AvgPrice,
		ExecutedQuote:   ack.ExecutedQuote,
		CreatedAt:       ack.AckedAt,
		UpdatedAt:       ack.AckedAt,
		Raw:             ack.Raw,
	}
}

func wallexResolveSymbol(native, fallback string) string {
	if native != "" {
		return wallexInternalSymbol(native)
	}
	if fallback != "" {
		if c, err := domain.NormalizeSymbol(fallback); err == nil {
			return c
		}
		return strings.ToUpper(fallback)
	}
	return ""
}

func wallexResolveSide(side, fallback string) string {
	if side != "" {
		return strings.ToLower(strings.TrimSpace(side))
	}
	return strings.ToLower(strings.TrimSpace(fallback))
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

// ─── error mapping ─────────────────────────────────────────────────────────────

// wallexHTTPError builds a *NormalizedAPIError for a non-2xx response, wrapping
// the appropriate execution sentinel where the status is classifiable.
func wallexHTTPError(op string, status int, body []byte) error {
	cat := classifyHTTPStatus(status)
	msg := wallexExtractMessage(body)
	e := &NormalizedAPIError{
		Exchange:   wallexCode,
		Op:         op,
		StatusCode: status,
		Category:   cat,
		Retryable:  status >= 500 || status == http.StatusTooManyRequests,
		Message:    MaskBody(msg),
	}
	switch cat {
	case CatRateLimit:
		e.Err = execution.ErrRateLimited
	case CatAuth:
		e.Err = execution.ErrAuthFailed
	case CatNotFound:
		// A 404 on an order endpoint means the (client-id-keyed) order is unknown.
		e.Err = execution.ErrOrderUnknown
	case CatInsufficientBalance:
		e.Err = execution.ErrInsufficientBalance
	}
	// Wallex sometimes signals insufficient balance via a 400 message.
	if e.Err == nil && wallexLooksInsufficient(msg) {
		e.Category = CatInsufficientBalance
		e.Err = execution.ErrInsufficientBalance
	}
	return e
}

// wallexExtractMessage pulls a human message out of a Wallex error body, which is
// either {"message":...} or {"error":{"code","message"}}.
func wallexExtractMessage(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var parsed struct {
		Message string          `json:"message"`
		Error   *wallexAPIError `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil {
		if parsed.Error != nil && parsed.Error.Message != "" {
			if parsed.Error.Code != 0 {
				return fmt.Sprintf("[%d] %s", parsed.Error.Code, parsed.Error.Message)
			}
			return parsed.Error.Message
		}
		if parsed.Message != "" {
			return parsed.Message
		}
	}
	return string(body)
}

func wallexLooksInsufficient(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "insufficient") || strings.Contains(m, "not enough") || strings.Contains(m, "balance")
}
