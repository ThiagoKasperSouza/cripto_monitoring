// Package normalizer converts raw Binance WebSocket JSON trade frames into
// the typed Protobuf messages shared across the ecosystem
// (marketdata.v1.TickerResponse).
package normalizer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	marketdatav1 "github.com/financial/marketdata/v1"
)

// combinedStreamEnvelope is the wrapper Binance uses for combined streams
// (wss://.../stream?streams=btcusdt@trade/ethusdt@trade).
type combinedStreamEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// binanceTradeEvent mirrors the payload of the `<symbol>@trade` stream.
// Note: the spot stream does NOT carry buyer/seller order IDs (b/a) — those
// are only present on futures. They are parsed opportunistically, tolerating
// both JSON number and string encodings, and default to 0 when absent.
type binanceTradeEvent struct {
	EventType          string          `json:"e"`
	EventTime          int64           `json:"E"` // event timestamp, ms UTC
	Symbol             string          `json:"s"`
	TradeID            int64           `json:"t"`
	Price              string          `json:"p"`
	Quantity           string          `json:"q"`
	BuyerOrderID       json.RawMessage `json:"b"`
	SellerOrderID      json.RawMessage `json:"a"`
	TradeTime          int64           `json:"T"` // trade timestamp, ms UTC
	IsBuyerMarketMaker bool            `json:"m"`
}

// parseIDFromRaw decodes an ID that may be a JSON number, a JSON string or
// absent (nil raw message -> 0).
func parseIDFromRaw(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	s := strings.TrimSpace(string(raw))
	s = strings.Trim(s, `"`)
	if s == "" {
		return 0, nil
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid order id %q: %w", s, err)
	}
	return id, nil
}

// ExtractTradePayload unwraps the combined-stream envelope and returns the
// raw trade object inside it. If the payload is not wrapped (single-stream
// connections) it is returned as-is.
func ExtractTradePayload(frame []byte) (json.RawMessage, error) {
	var env combinedStreamEnvelope
	if err := json.Unmarshal(frame, &env); err == nil && len(env.Data) > 0 {
		return env.Data, nil
	}
	// Not an envelope: assume it is a bare trade frame.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(frame, &probe); err != nil {
		return nil, fmt.Errorf("frame is neither a combined-stream envelope nor JSON: %w", err)
	}
	if _, ok := probe["e"]; !ok {
		return nil, errors.New("frame missing both envelope 'data' and trade 'e' field")
	}
	return frame, nil
}

// NormalizeTrade parses a raw trade object and returns the canonical
// TickerResponse. Prices and quantities are kept as strings exactly as
// received to avoid float64 precision loss, per the contract.
func NormalizeTrade(data []byte) (*marketdatav1.TickerResponse, error) {
	var ev binanceTradeEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, fmt.Errorf("invalid trade JSON: %w", err)
	}
	if ev.EventType != "trade" {
		return nil, fmt.Errorf("unexpected event type %q (want %q)", ev.EventType, "trade")
	}
	if ev.Symbol == "" {
		return nil, errors.New("empty symbol")
	}
	if ev.TradeID <= 0 {
		return nil, fmt.Errorf("invalid trade_id %d", ev.TradeID)
	}
	if err := validateDecimal("price", ev.Price); err != nil {
		return nil, err
	}
	if err := validateDecimal("quantity", ev.Quantity); err != nil {
		return nil, err
	}
	buyerOrderID, err := parseIDFromRaw(ev.BuyerOrderID)
	if err != nil {
		return nil, fmt.Errorf("buyer_order_id: %w", err)
	}
	sellerOrderID, err := parseIDFromRaw(ev.SellerOrderID)
	if err != nil {
		return nil, fmt.Errorf("seller_order_id: %w", err)
	}

	return &marketdatav1.TickerResponse{
		Symbol:             ev.Symbol,
		Price:              ev.Price,
		Quantity:           ev.Quantity,
		EventTime:          ev.EventTime, // "E" — timestamp do evento gerado pela exchange
		TradeId:            ev.TradeID,
		BuyerOrderId:       buyerOrderID,
		SellerOrderId:      sellerOrderID,
		IsBuyerMarketMaker: ev.IsBuyerMarketMaker,
	}, nil
}

func validateDecimal(field, s string) error {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f <= 0 {
		return fmt.Errorf("invalid %s %q: not a positive number", field, s)
	}
	return nil
}
