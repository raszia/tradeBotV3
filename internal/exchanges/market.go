package exchanges

import "github.com/shopspring/decimal"

// NormalizedMarket is the exchange-agnostic market metadata an adapter returns
// from GetMarkets. It maps directly onto the exchange_markets table (PR2): the
// market-discovery process (later PR) upserts these rows. ExchangeSymbol is the
// venue-native symbol; CanonicalSymbol is BASE/QUOTE.
type NormalizedMarket struct {
	Exchange          string
	ExchangeSymbol    string
	CanonicalSymbol   string
	BaseAsset         string
	QuoteAsset        string
	QuoteAssetType    string // USDT | IRT | IRR | OTHER (see domain.QuoteAssetType)
	PricePrecision    int
	QuantityPrecision int
	MinOrderAmount    decimal.Decimal // min notional (quote)
	MinOrderQuantity  decimal.Decimal // min base qty
	TickSize          decimal.Decimal
	StepSize          decimal.Decimal
	MakerFee          decimal.Decimal
	TakerFee          decimal.Decimal
	Tradable          bool
	Raw               string // raw venue metadata (already secret-masked); stored in exchange_markets.raw_metadata
}
