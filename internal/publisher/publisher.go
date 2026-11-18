// Package publisher abstracts the destination of normalized trade events.
// Phase 1 ships a stdout/log implementation only; Phase 2 will add the
// Kafka publisher (topic per financial asset, key = symbol, message =
// serialized marketdata.v1.TickerResponse) without touching the ingestion
// pipeline.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	marketdatav1 "github.com/financial/marketdata/v1"
)

// Publisher is the sink for normalized trades.
type Publisher interface {
	Publish(ctx context.Context, trade *marketdatav1.TickerResponse) error
	Close() error
}

// logPublisher writes one JSON line per trade to an io.Writer (stdout by
// default). It is safe for concurrent use.
type logPublisher struct {
	mu sync.Mutex
	w  io.Writer
}

// NewLogPublisher returns a Publisher that emits structured JSON logs.
func NewLogPublisher(w io.Writer) Publisher {
	if w == nil {
		w = os.Stdout
	}
	return &logPublisher{w: w}
}

func (p *logPublisher) Publish(_ context.Context, trade *marketdatav1.TickerResponse) error {
	line, err := json.Marshal(map[string]any{
		"kind":                "trade",
		"symbol":              trade.GetSymbol(),
		"price":               trade.GetPrice(),
		"quantity":            trade.GetQuantity(),
		"event_time":          trade.GetEventTime(),
		"trade_id":            trade.GetTradeId(),
		"buyer_order_id":      trade.GetBuyerOrderId(),
		"seller_order_id":     trade.GetSellerOrderId(),
		"is_buyer_market_maker": trade.GetIsBuyerMarketMaker(),
	})
	if err != nil {
		return fmt.Errorf("marshal trade: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err = fmt.Fprintf(p.w, "%s\n", line)
	return err
}

func (p *logPublisher) Close() error { return nil }
