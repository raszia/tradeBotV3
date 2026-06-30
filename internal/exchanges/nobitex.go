package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/execution"
)

// Nobitex adapter — Iranian exchange, IRT/USDT spot markets. Ported/adapted from
// the sibling iranArb system's nobitex package. It exposes BOTH a public
// market-data surface and a private trading surface.
//
// Capabilities here:
//   - Public:  GetMarkets (v2/options precisions), GetOrderBook (v3/orderbook/<sym>).
//              No order-book WebSocket in this adapter (the iranArb WS used a
//              Centrifuge client; porting it is deferred — OrderBookWS=false).
//   - Private: GetBalances, PlaceOrder, CancelOrder, GetOrder, GetOpenOrders.
//              No order-update WebSocket here (OrderUpdatesWS=false); order state
//              is obtained by POLLING GetOrder (OrderStatusPoll=true).
//
// Known limitations / quirks (from the Nobitex API and the iranArb source):
//   - RIAL pricing: Nobitex quotes IRT markets in RIAL (rial), which is 10× the
//     toman/IRT canonical. The order book multiplies raw prices by 0.1 (the
//     priceMultiplier arg to BuildOrderBook) so all book/quote prices land in
//     IRT/toman. Order placement does the inverse — internal IRT prices are
//     divided by 10 before being sent to Nobitex (nobitexPriceToVenue).
//   - Auth: Token auth. The API key is sent verbatim in the
//     "Authorization: Token <key>" header. There is NO request signing / HMAC
//     and no passphrase. Only APIKey from Credentials is used.
//   - Client order id: Nobitex DOES support a client-provided order id
//     ("clientOrderId" form field) with a 32-char cap, enforced by a partial
//     unique index per user. ClientOrderID capability = true; it is truncated to
//     32 chars and only sent when non-empty.
//   - Order type / TIF are NOT forced. The owner's OrderRequest.OrderType and
//     TimeInForce flow through: OrderType=market maps to Nobitex "execution":
//     "market", otherwise "limit"; TimeInForce maps to "mode" (ioc/fok) when set,
//     else "default". This adapter does NOT synthesize IOC by place-then-cancel
//     (that was an iranArb workaround); it submits whatever the request asks for.
//   - Order status is POLLING-ONLY here (no private WS). SubscribeOrderUpdates
//     returns Unsupported.
//   - Rate limits: Nobitex answers an over-limit request with HTTP 429 and a body
//     like {"code":"TooManyRequests","backOff":<seconds>}. The adapter maps 429 to
//     execution.ErrRateLimited; per-endpoint backoff parking (as in iranArb) is a
//     higher-layer concern and is NOT replicated here.
//   - Order placement uses a multipart/form-data POST (not JSON); reads are JSON
//     GETs. Both carry the Token auth header.
//   - "pro": "true" is sent on order placement to bypass Nobitex's 10-second
//     duplicate-order guard (same market/side/exec/amount/price within 10s would
//     otherwise be rejected as DuplicateOrder); clientOrderId preserves idempotency.

const (
	nobitexCode        = "nobitex"
	nobitexDefaultREST = "https://apiv2.nobitex.ir"
	nobitexOptionsPath = "/v2/options"
	// nobitexUserAgent matches Nobitex's TraderBot/<...> prefix which bumps the
	// request's service-type from "norm" to "ex_bot" (never worse than norm).
	nobitexUserAgent = "TraderBot/v3TradeBot-1.0"
	// nobitexClientOrderIDMax is Nobitex's clientOrderId length cap.
	nobitexClientOrderIDMax = 32
)

// nobitexRialToToman converts Nobitex's RIAL prices to the IRT/toman canonical
// (rial is 10× toman). Used as the BuildOrderBook priceMultiplier and to convert
// rial-denominated order/balance amounts to internal IRT. Built from an exact decimal
// literal — never decimal.NewFromFloat — so no money value passes through a float64.
var nobitexRialToToman = decimal.RequireFromString("0.1")

func init() {
	Register(Registration{
		Code: nobitexCode,
		Capabilities: Capabilities{
			MarketMetadata:  true,
			OrderBookREST:   true,
			OrderBookWS:     false, // WS porting deferred
			BalanceFetch:    true,
			PlaceOrder:      true,
			CancelByOrderID: true,
			FetchByOrderID:  true,
			FetchOpenOrders: true,
			RecentFills:     false,
			OrderUpdatesWS:  false, // no private WS here
			OrderStatusPoll: true,  // order state via GetOrder polling
			ClientOrderID:   true,  // Nobitex supports clientOrderId (<=32 chars)
		},
		NewPublic:  newNobitexPublic,
		NewPrivate: newNobitexPrivate,
	})
}

// ─── public client ─────────────────────────────────────────────────────────

type nobitexPublic struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
}

func newNobitexPublic(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	cfg.Code = nobitexCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = nobitexDefaultREST
	}
	return &nobitexPublic{cfg: cfg, http: hc, restURL: strings.TrimRight(restURL, "/")}, nil
}

func (p *nobitexPublic) Name() string { return nobitexCode }
func (p *nobitexPublic) Capabilities() Capabilities {
	r, _ := Lookup(nobitexCode)
	return r.Capabilities
}

// --- GetMarkets ---

// nobitexOptionsResp is GET /v2/options. The `nobitex` object carries
// amountPrecisions / pricePrecisions as DIRECT step VALUES (e.g. "0.0001", "10")
// keyed by concatenated native symbol ("AAVEIRT"). minOrders is the minimum
// order VALUE per quote currency ("rls" rial for IRT markets, "usdt").
type nobitexOptionsResp struct {
	Nobitex struct {
		AmountPrecisions map[string]decimal.Decimal `json:"amountPrecisions"`
		PricePrecisions  map[string]decimal.Decimal `json:"pricePrecisions"`
		MinOrders        map[string]decimal.Decimal `json:"minOrders"`
	} `json:"nobitex"`
}

func (p *nobitexPublic) GetMarkets(ctx context.Context) ([]NormalizedMarket, error) {
	ctx, cancel := RequestContext(ctx, p.cfg)
	defer cancel()

	raw, err := p.get(ctx, nobitexOptionsPath)
	if err != nil {
		return nil, err
	}
	var parsed nobitexOptionsResp
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("nobitex options decode: %w", err)
	}

	// Min order VALUE per quote (rial → toman via the multiplier; USDT as-is).
	minIRT := parsed.Nobitex.MinOrders["rls"].Mul(nobitexRialToToman)
	minUSDT := parsed.Nobitex.MinOrders["usdt"]

	out := make([]NormalizedMarket, 0, len(parsed.Nobitex.AmountPrecisions))
	for key, amt := range parsed.Nobitex.AmountPrecisions {
		base, quote, ok := nobitexSplitMarketKey(key)
		if !ok {
			continue
		}
		tick, ok := parsed.Nobitex.PricePrecisions[key]
		if !ok || !amt.IsPositive() || !tick.IsPositive() {
			continue
		}
		minNotional := minUSDT
		if quote == domain.CurrencyIRT {
			tick = tick.Mul(nobitexRialToToman) // rial → toman
			minNotional = minIRT
		}
		canonical := base + "/" + quote
		out = append(out, NormalizedMarket{
			Exchange:          nobitexCode,
			ExchangeSymbol:    strings.ToUpper(key),
			CanonicalSymbol:   canonical,
			BaseAsset:         base,
			QuoteAsset:        quote,
			QuoteAssetType:    domain.QuoteAssetType(canonical),
			PricePrecision:    nobitexDecimalPlaces(tick),
			QuantityPrecision: nobitexDecimalPlaces(amt),
			TickSize:          tick,
			StepSize:          amt,
			MinOrderAmount:    minNotional,
			Tradable:          true,
		})
	}
	return out, nil
}

// --- GetOrderBook (REST) ---

// nobitexV3SymbolBook is the GET /v3/orderbook/<symbol> response. Levels are
// [price_str, qty_str] pairs in native RIAL for IRT markets.
type nobitexV3SymbolBook struct {
	Status string     `json:"status"`
	Bids   [][]string `json:"bids"`
	Asks   [][]string `json:"asks"`
}

func (p *nobitexPublic) GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	venueSym, ok := VenueSymbol(p.cfg, symbol)
	if !ok {
		venueSym = nobitexNativeKey(symbol)
	}
	canonical, err := domain.NormalizeSymbol(symbol)
	if err != nil || canonical == "" {
		canonical = strings.ToUpper(symbol)
	}

	ctx, cancel := RequestContext(ctx, p.cfg)
	defer cancel()
	raw, err := p.get(ctx, "/v3/orderbook/"+url.PathEscape(venueSym))
	if err != nil {
		return domain.OrderBook{}, err
	}
	var sb nobitexV3SymbolBook
	if err := json.Unmarshal(raw, &sb); err != nil {
		return domain.OrderBook{}, fmt.Errorf("nobitex orderbook decode: %w", err)
	}

	// Rial → toman only for IRT-quoted markets; USDT books are unconverted.
	multiplier := decimal.NewFromInt(1)
	if _, quote := nobitexSplitSymbol(canonical); quote == domain.CurrencyIRT {
		multiplier = nobitexRialToToman
	}

	book := BuildOrderBook(nobitexCode, canonical,
		nobitexLevels(sb.Bids), nobitexLevels(sb.Asks),
		multiplier, "rest", time.Now())
	return book, nil
}

// SubscribeOrderBook is not implemented in this adapter (WS porting deferred).
func (p *nobitexPublic) SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error) {
	return nil, Unsupported(nobitexCode, "SubscribeOrderBook")
}

func (p *nobitexPublic) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.restURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", nobitexUserAgent)
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("nobitex GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nobitexHTTPError(path, resp.StatusCode, body)
	}
	return body, nil
}

// ─── private client ────────────────────────────────────────────────────────

type nobitexPrivate struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
}

func newNobitexPrivate(cfg ClientConfig, logger *IOLogger) (PrivateClient, error) {
	cfg.Code = nobitexCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = nobitexDefaultREST
	}
	return &nobitexPrivate{cfg: cfg, http: hc, restURL: strings.TrimRight(restURL, "/")}, nil
}

func (c *nobitexPrivate) Name() string { return nobitexCode }
func (c *nobitexPrivate) Capabilities() Capabilities {
	r, _ := Lookup(nobitexCode)
	return r.Capabilities
}

// credentials fetches and validates the API key for this exchange.
func (c *nobitexPrivate) credentials(ctx context.Context) (Credentials, error) {
	if c.cfg.Creds == nil {
		return Credentials{}, fmt.Errorf("nobitex: no credential provider configured")
	}
	creds, err := c.cfg.Creds.Credentials(ctx, nobitexCode)
	if err != nil {
		return Credentials{}, err
	}
	if creds.APIKey == "" {
		return Credentials{}, &NormalizedAPIError{
			Exchange: nobitexCode, Op: "credentials", Category: CatAuth,
			Message: "missing api key", Err: execution.ErrAuthFailed,
		}
	}
	return creds, nil
}

// --- GetBalances ---

type nobitexWalletListResponse struct {
	Status  string `json:"status"`
	Wallets []struct {
		Currency      string          `json:"currency"`
		Balance       json.RawMessage `json:"balance"`
		Blocked       json.RawMessage `json:"blockedBalance"`
		ActiveBalance json.RawMessage `json:"activeBalance"`
		RialBalance   json.RawMessage `json:"rialBalance"`
	} `json:"wallets"`
	Code string `json:"code,omitempty"`
}

func (c *nobitexPrivate) GetBalances(ctx context.Context) ([]domain.Balance, error) {
	var payload nobitexWalletListResponse
	if _, err := c.doJSON(ctx, http.MethodGet, "/users/wallets/list?type=spot", &payload); err != nil {
		return nil, err
	}
	if payload.Status != "" && payload.Status != "ok" {
		return nil, fmt.Errorf("nobitex balances failed: status=%s code=%s", payload.Status, payload.Code)
	}

	now := time.Now().UTC()
	out := make([]domain.Balance, 0, len(payload.Wallets))
	for _, wallet := range payload.Wallets {
		asset := nobitexInternalAsset(wallet.Currency)
		if asset == "" {
			continue
		}
		// activeBalance (free) preferred; fall back to balance.
		free := nobitexDecOrZero(wallet.ActiveBalance)
		if free.IsZero() {
			free = nobitexDecOrZero(wallet.Balance)
		}
		blocked := nobitexDecOrZero(wallet.Blocked)
		total := nobitexDecOrZero(wallet.Balance)
		if total.IsZero() {
			total = free.Add(blocked)
		}
		// Nobitex reports IRT balances in rial; convert to toman/IRT.
		if asset == domain.CurrencyIRT {
			free = free.Mul(nobitexRialToToman)
			blocked = blocked.Mul(nobitexRialToToman)
			total = total.Mul(nobitexRialToToman)
		}
		out = append(out, domain.Balance{
			Exchange:  nobitexCode,
			Asset:     asset,
			Available: free,
			Locked:    blocked,
			Total:     total,
			UpdatedAt: now,
		})
	}
	return out, nil
}

// --- order types ---

type nobitexOrderEnvelope struct {
	Status string       `json:"status"`
	Order  nobitexOrder `json:"order"`
	Code   string       `json:"code,omitempty"`
}

type nobitexOrdersListResponse struct {
	Status string         `json:"status"`
	Orders []nobitexOrder `json:"orders"`
	Code   string         `json:"code,omitempty"`
}

type nobitexOrder struct {
	ID              json.RawMessage `json:"id"`
	ClientOrderID   string          `json:"clientOrderId"`
	Type            string          `json:"type"`
	Execution       string          `json:"execution"`
	SrcCurrency     string          `json:"srcCurrency"`
	DstCurrency     string          `json:"dstCurrency"`
	Price           json.RawMessage `json:"price"`
	Amount          json.RawMessage `json:"amount"`
	TotalPrice      json.RawMessage `json:"totalPrice"`
	MatchedAmount   json.RawMessage `json:"matchedAmount"`
	UnmatchedAmount json.RawMessage `json:"unmatchedAmount"`
	Status          string          `json:"status"`
	Fee             json.RawMessage `json:"fee"`
	FeeCurrency     string          `json:"feeCurrency"`
	CreatedAt       string          `json:"created_at"`
	Market          string          `json:"market"`
	AveragePrice    json.RawMessage `json:"averagePrice"`
}

// --- PlaceOrder ---

func (c *nobitexPrivate) PlaceOrder(ctx context.Context, req execution.OrderRequest) (execution.OrderAck, error) {
	base, quote := nobitexSplitSymbol(req.Symbol)
	if base == "" || quote == "" {
		return execution.OrderAck{}, fmt.Errorf("nobitex: invalid symbol %q", req.Symbol)
	}

	fields := map[string]string{
		"type":        strings.ToLower(req.Side),
		"mode":        nobitexModeFromTIF(req.TimeInForce),
		"execution":   nobitexExecutionFromType(req.OrderType),
		"srcCurrency": strings.ToLower(base),
		"dstCurrency": nobitexDstCurrency(quote),
		"amount":      req.Quantity.String(),
		// pro=true bypasses Nobitex's 10-second duplicate-order guard. Idempotency
		// is preserved by clientOrderId's unique index per user.
		"pro": "true",
	}
	// Limit orders carry a price; market orders do not. The price the owner set is
	// in internal IRT — convert it to Nobitex-native rial for IRT markets.
	if !strings.EqualFold(req.OrderType, execution.OrderTypeMarket) {
		fields["price"] = nobitexPriceToVenue(quote, req.LimitPrice).String()
	}
	// clientOrderId (<=32 chars) — only sent when supported and non-empty.
	if cid := strings.TrimSpace(req.ClientOrderID); cid != "" {
		if len(cid) > nobitexClientOrderIDMax {
			cid = cid[:nobitexClientOrderIDMax]
		}
		fields["clientOrderId"] = cid
	}

	var payload nobitexOrderEnvelope
	raw, err := c.doForm(ctx, "/market/orders/add", fields, &payload)
	if err != nil {
		return execution.OrderAck{}, err
	}
	if payload.Status != "ok" {
		return execution.OrderAck{}, nobitexBusinessError("/market/orders/add", payload.Status, payload.Code, raw)
	}

	canonical := req.Symbol
	if norm, nerr := domain.NormalizeSymbol(req.Symbol); nerr == nil {
		canonical = norm
	}
	ack := c.orderToAck(payload.Order, canonical, req.Side)
	ack.RequestedQty = req.Quantity
	ack.LimitPrice = req.LimitPrice
	ack.Raw = MaskBody(string(raw))
	return ack, nil
}

// --- CancelOrder ---

func (c *nobitexPrivate) CancelOrder(ctx context.Context, exchangeOrderID string) error {
	if exchangeOrderID == "" {
		return fmt.Errorf("nobitex: order id is required")
	}
	var payload struct {
		Status string `json:"status"`
		Code   string `json:"code,omitempty"`
	}
	raw, err := c.doForm(ctx, "/market/orders/update-status", map[string]string{
		"order":  exchangeOrderID,
		"status": "canceled",
	}, &payload)
	if err != nil {
		return err
	}
	if payload.Status != "" && payload.Status != "ok" {
		return nobitexBusinessError("/market/orders/update-status", payload.Status, payload.Code, raw)
	}
	return nil
}

// --- GetOrder ---

func (c *nobitexPrivate) GetOrder(ctx context.Context, exchangeOrderID string) (execution.OrderStatus, error) {
	if exchangeOrderID == "" {
		return execution.OrderStatus{}, fmt.Errorf("nobitex: order id is required")
	}
	var payload nobitexOrderEnvelope
	path := "/market/orders/status?id=" + url.QueryEscape(exchangeOrderID)
	raw, err := c.doJSON(ctx, http.MethodGet, path, &payload)
	if err != nil {
		return execution.OrderStatus{}, err
	}
	if payload.Status != "ok" {
		return execution.OrderStatus{}, nobitexBusinessError(path, payload.Status, payload.Code, raw)
	}
	return c.orderToStatus(payload.Order, "", ""), nil
}

// --- GetOpenOrders ---

func (c *nobitexPrivate) GetOpenOrders(ctx context.Context, symbol string) ([]execution.OrderStatus, error) {
	path := "/market/orders/list?status=open&details=2"
	if symbol != "" {
		base, quote := nobitexSplitSymbol(symbol)
		if base != "" && quote != "" {
			path += "&srcCurrency=" + strings.ToLower(base) + "&dstCurrency=" + nobitexDstCurrency(quote)
		}
	}
	var payload nobitexOrdersListResponse
	raw, err := c.doJSON(ctx, http.MethodGet, path, &payload)
	if err != nil {
		return nil, err
	}
	if payload.Status != "" && payload.Status != "ok" {
		return nil, nobitexBusinessError(path, payload.Status, payload.Code, raw)
	}
	out := make([]execution.OrderStatus, 0, len(payload.Orders))
	for _, o := range payload.Orders {
		out = append(out, c.orderToStatus(o, "", ""))
	}
	return out, nil
}

// SubscribeOrderUpdates is polling-only here (no private WS in this adapter).
func (c *nobitexPrivate) SubscribeOrderUpdates(ctx context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	return nil, Unsupported(nobitexCode, "SubscribeOrderUpdates")
}

// ─── order mapping ─────────────────────────────────────────────────────────

// orderToAck maps a Nobitex order object to a normalized OrderAck. fallbackSymbol
// and fallbackSide are canonical hints used when the order object omits them.
func (c *nobitexPrivate) orderToAck(o nobitexOrder, fallbackSymbol, fallbackSide string) execution.OrderAck {
	symbol := fallbackSymbol
	if symbol == "" {
		symbol = nobitexSymbolFromOrder(o)
	}
	side := strings.ToLower(fallbackSide)
	if o.Type != "" {
		side = strings.ToLower(o.Type)
	}
	_, quote := nobitexSplitSymbol(symbol)

	reqQty := nobitexDecOrZero(o.Amount)
	filled := nobitexDecOrZero(o.MatchedAmount)
	remaining := nobitexDecOrZero(o.UnmatchedAmount)
	if reqQty.IsZero() && filled.IsPositive() {
		reqQty = filled.Add(remaining)
	}
	limit := nobitexPriceToInternal(quote, nobitexDecOrZero(o.Price))
	avg := nobitexPriceToInternal(quote, nobitexDecOrZero(o.AveragePrice))
	executedQuote := nobitexPriceToInternal(quote, nobitexDecOrZero(o.TotalPrice))
	if executedQuote.IsZero() && avg.IsPositive() && filled.IsPositive() {
		executedQuote = avg.Mul(filled)
	}
	at := nobitexParseTime(o.CreatedAt)
	if at.IsZero() {
		at = time.Now().UTC()
	}
	state := nobitexMapOrderState(o.Status, filled, reqQty)
	return execution.OrderAck{
		ExchangeOrderID: nobitexStringFromRaw(o.ID),
		ClientOrderID:   o.ClientOrderID,
		Symbol:          symbol,
		Side:            side,
		RequestedQty:    reqQty,
		LimitPrice:      limit,
		Status:          state,
		RawStatus:       o.Status,
		FilledQty:       filled,
		AvgPrice:        avg,
		ExecutedQuote:   executedQuote,
		Fee:             nobitexDecOrZero(o.Fee),
		FeeAsset:        nobitexInternalAsset(o.FeeCurrency),
		Active:          state == execution.StateOpen || state == execution.StatePartiallyFilled || state == execution.StateNew,
		AckedAt:         at,
	}
}

func (c *nobitexPrivate) orderToStatus(o nobitexOrder, fallbackSymbol, fallbackSide string) execution.OrderStatus {
	ack := c.orderToAck(o, fallbackSymbol, fallbackSide)
	remaining := nobitexDecOrZero(o.UnmatchedAmount)
	if remaining.IsZero() {
		remaining = ack.RequestedQty.Sub(ack.FilledQty)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}
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
		Fee:             ack.Fee,
		FeeAsset:        ack.FeeAsset,
		CreatedAt:       ack.AckedAt,
		UpdatedAt:       ack.AckedAt,
		Raw:             ack.Raw,
	}
}

// nobitexMapOrderState maps a raw Nobitex order status to the normalized state.
// Ported from iranArb's mapNobitexOrderState: a fully-matched order is Filled
// regardless of the textual status; "done" = filled; "canceled" with a partial
// fill = partially-canceled; "active" with a partial fill = partially-filled.
func nobitexMapOrderState(status string, filled, requested decimal.Decimal) execution.NormalizedOrderState {
	s := strings.ToLower(strings.TrimSpace(status))
	if requested.IsPositive() && filled.Equal(requested) {
		return execution.StateFilled
	}
	switch s {
	case "done":
		return execution.StateFilled
	case "canceled", "cancelled":
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled
	case "active":
		if filled.IsPositive() {
			return execution.StatePartiallyFilled
		}
		return execution.StateOpen
	case "rejected":
		return execution.StateRejected
	case "":
		return execution.StateUnknown
	default:
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled
	}
}

// ─── HTTP helpers ──────────────────────────────────────────────────────────

// doJSON issues an authenticated JSON GET/DELETE-style request (no body) and
// decodes the response into out.
func (c *nobitexPrivate) doJSON(ctx context.Context, method, path string, out any) ([]byte, error) {
	creds, err := c.credentials(ctx)
	if err != nil {
		return nil, err
	}
	ctx, cancel := RequestContext(ctx, c.cfg)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, c.restURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Token "+creds.APIKey)
	req.Header.Set("User-Agent", nobitexUserAgent)
	return c.do(req, path, out)
}

// doForm issues an authenticated multipart/form-data POST (Nobitex's order API
// shape) and decodes the response into out.
func (c *nobitexPrivate) doForm(ctx context.Context, path string, fields map[string]string, out any) ([]byte, error) {
	creds, err := c.credentials(ctx)
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for k, v := range fields {
		if err := writer.WriteField(k, v); err != nil {
			_ = writer.Close()
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	ctx, cancel := RequestContext(ctx, c.cfg)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.restURL+path, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Token "+creds.APIKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("User-Agent", nobitexUserAgent)
	return c.do(req, path, out)
}

func (c *nobitexPrivate) do(req *http.Request, path string, out any) ([]byte, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("nobitex %s %s: %w", req.Method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return raw, nobitexHTTPError(path, resp.StatusCode, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return raw, fmt.Errorf("nobitex %s decode: %w", path, err)
		}
	}
	return raw, nil
}

// nobitexHTTPError builds a *NormalizedAPIError from a non-2xx response, wrapping
// the appropriate execution sentinel where classifiable.
func nobitexHTTPError(op string, status int, body []byte) error {
	category := classifyHTTPStatus(status)
	e := &NormalizedAPIError{
		Exchange:   nobitexCode,
		Op:         op,
		StatusCode: status,
		Category:   category,
		Retryable:  status >= 500 || status == http.StatusTooManyRequests,
		Message:    MaskBody(string(body)),
	}
	switch {
	case status == http.StatusTooManyRequests:
		e.Err = execution.ErrRateLimited
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		e.Err = execution.ErrAuthFailed
	}
	// Inspect the venue body for codes that classify the error more precisely.
	code, msg := nobitexErrorCode(body)
	if code != "" {
		e.Code = code
	}
	switch {
	case nobitexIndicatesOrderUnknown(code, msg):
		e.Category = CatNotFound
		e.Err = execution.ErrOrderUnknown
	case nobitexIndicatesInsufficientBalance(code, msg):
		e.Category = CatInsufficientBalance
		e.Err = execution.ErrInsufficientBalance
	}
	return e
}

// nobitexBusinessError builds a *NormalizedAPIError for a 2xx response whose
// envelope reports a non-"ok" status (Nobitex returns 200 with {"status":"failed",
// "code":"..."} for many business errors).
func nobitexBusinessError(op, status, code string, body []byte) error {
	e := &NormalizedAPIError{
		Exchange: nobitexCode,
		Op:       op,
		Code:     code,
		Category: CatBadRequest,
		Message:  MaskBody(string(body)),
	}
	switch {
	case nobitexIndicatesOrderUnknown(code, ""):
		e.Category = CatNotFound
		e.Err = execution.ErrOrderUnknown
	case nobitexIndicatesInsufficientBalance(code, ""):
		e.Category = CatInsufficientBalance
		e.Err = execution.ErrInsufficientBalance
	}
	if e.Message == "" {
		e.Message = fmt.Sprintf("status=%s code=%s", status, code)
	}
	return e
}

func nobitexErrorCode(body []byte) (code, message string) {
	var r struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(body, &r)
	return r.Code, r.Message
}

func nobitexIndicatesOrderUnknown(code, msg string) bool {
	hay := strings.ToLower(code + " " + msg)
	return strings.Contains(hay, "ordernotfound") ||
		strings.Contains(hay, "order not found") ||
		strings.Contains(hay, "invalidorderid") ||
		strings.Contains(hay, "notfound")
}

func nobitexIndicatesInsufficientBalance(code, msg string) bool {
	hay := strings.ToLower(code + " " + msg)
	return strings.Contains(hay, "insufficientbalance") ||
		strings.Contains(hay, "insufficient balance") ||
		strings.Contains(hay, "notenoughbalance") ||
		strings.Contains(hay, "balance")
}

// ─── pure helpers ──────────────────────────────────────────────────────────

// nobitexLevels converts [[price_str, qty_str], ...] into []domain.Level. Pairs
// with fewer than two elements or unparseable numbers are skipped.
func nobitexLevels(raw [][]string) []domain.Level {
	out := make([]domain.Level, 0, len(raw))
	for _, pair := range raw {
		if len(pair) < 2 {
			continue
		}
		price := parseDec(pair[0])
		qty := parseDec(pair[1])
		if price.IsZero() && qty.IsZero() {
			continue
		}
		out = append(out, domain.Level{Price: price, Quantity: qty})
	}
	return out
}

// nobitexNativeKey normalizes a symbol to Nobitex's concatenated native form
// ("USDT/IRT" → "USDTIRT"); rial aliases collapse to IRT.
func nobitexNativeKey(s string) string {
	n := strings.ToUpper(strings.TrimSpace(s))
	for _, sep := range []string{"-", "/", "_", " "} {
		n = strings.ReplaceAll(n, sep, "")
	}
	n = strings.ReplaceAll(n, "RLS", "IRT")
	n = strings.ReplaceAll(n, "IRR", "IRT")
	n = strings.ReplaceAll(n, "RIAL", "IRT")
	return n
}

// nobitexSplitSymbol splits a canonical "BASE/QUOTE" symbol into upper-cased
// parts. Returns empty strings for malformed input.
func nobitexSplitSymbol(symbol string) (base, quote string) {
	parts := strings.SplitN(symbol, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return strings.ToUpper(parts[0]), strings.ToUpper(parts[1])
}

// nobitexSplitMarketKey parses a native market key ("AAVEIRT") → ("AAVE","IRT").
// Quote suffixes are checked longest-first; RLS normalizes to IRT.
func nobitexSplitMarketKey(key string) (base, quote string, ok bool) {
	k := strings.ToUpper(strings.TrimSpace(key))
	for _, q := range []string{"USDT", "IRT", "RLS"} {
		if strings.HasSuffix(k, q) && len(k) > len(q) {
			base = k[:len(k)-len(q)]
			quote = q
			if quote == "RLS" {
				quote = domain.CurrencyIRT
			}
			return base, quote, true
		}
	}
	return "", "", false
}

// nobitexInternalAsset normalizes a Nobitex currency code to the canonical asset
// (rial aliases → IRT). Empty input returns "".
func nobitexInternalAsset(asset string) string {
	a := strings.ToUpper(strings.TrimSpace(asset))
	switch a {
	case "":
		return ""
	case "RLS", "IRR", "RIAL":
		return domain.CurrencyIRT
	default:
		return a
	}
}

// nobitexDstCurrency maps a canonical quote asset to Nobitex's dstCurrency value
// (IRT → "rls"; otherwise the lower-cased asset).
func nobitexDstCurrency(quote string) string {
	switch strings.ToUpper(quote) {
	case domain.CurrencyIRT, domain.CurrencyIRR:
		return "rls"
	default:
		return strings.ToLower(quote)
	}
}

// nobitexExecutionFromType maps OrderRequest.OrderType to Nobitex's "execution"
// field. Order type is NOT forced: market → "market", anything else → "limit".
func nobitexExecutionFromType(orderType string) string {
	if strings.EqualFold(orderType, execution.OrderTypeMarket) {
		return "market"
	}
	return "limit"
}

// nobitexModeFromTIF maps OrderRequest.TimeInForce to Nobitex's "mode" field. TIF
// is NOT forced: IOC/FOK pass through; empty/GTC → "default".
func nobitexModeFromTIF(tif string) string {
	switch strings.ToUpper(strings.TrimSpace(tif)) {
	case execution.TIFIOC:
		return "ioc"
	case execution.TIFFOK:
		return "fok"
	default:
		return "default"
	}
}

// nobitexPriceToVenue converts an internal IRT price to Nobitex-native rial for
// IRT markets (×10); other quotes pass through unchanged.
func nobitexPriceToVenue(quote string, price decimal.Decimal) decimal.Decimal {
	if strings.EqualFold(quote, domain.CurrencyIRT) || strings.EqualFold(quote, domain.CurrencyIRR) {
		return price.Div(nobitexRialToToman) // ÷0.1 == ×10
	}
	return price
}

// nobitexPriceToInternal converts a Nobitex-native price/amount (rial for IRT
// markets) to internal IRT (×0.1); other quotes pass through unchanged.
func nobitexPriceToInternal(quote string, price decimal.Decimal) decimal.Decimal {
	if strings.EqualFold(quote, domain.CurrencyIRT) || strings.EqualFold(quote, domain.CurrencyIRR) {
		return price.Mul(nobitexRialToToman)
	}
	return price
}

// nobitexSymbolFromOrder derives a canonical symbol from a Nobitex order object,
// preferring the "market" field and falling back to src/dst currencies.
func nobitexSymbolFromOrder(o nobitexOrder) string {
	if o.Market != "" {
		if base, quote, ok := nobitexSplitMarketKey(nobitexNativeKey(o.Market)); ok {
			return base + "/" + quote
		}
	}
	base := nobitexInternalAsset(o.SrcCurrency)
	quote := nobitexInternalAsset(o.DstCurrency)
	if base != "" && quote != "" {
		return base + "/" + quote
	}
	return ""
}

// nobitexDecimalPlaces returns the number of fractional digits in a step value
// (e.g. "0.0001" → 4, "10" → 0). Used to fill PricePrecision/QuantityPrecision.
func nobitexDecimalPlaces(step decimal.Decimal) int {
	if exp := step.Exponent(); exp < 0 {
		return int(-exp)
	}
	return 0
}

func nobitexDecOrZero(raw json.RawMessage) decimal.Decimal {
	if len(raw) == 0 {
		return decimal.Zero
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if d, err := decimal.NewFromString(text); err == nil {
			return d
		}
		return decimal.Zero
	}
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		if d, err := decimal.NewFromString(num.String()); err == nil {
			return d
		}
	}
	return decimal.Zero
}

func nobitexStringFromRaw(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String()
	}
	return strings.Trim(string(raw), `"`)
}

func nobitexParseTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.000000Z"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
