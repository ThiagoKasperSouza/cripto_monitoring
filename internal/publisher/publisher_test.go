package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	marketdatav1 "github.com/financial/marketdata/v1"
)

func TestLogPublisherEmitsValidJSONLine(t *testing.T) {
	var buf bytes.Buffer
	p := NewLogPublisher(&buf)

	trade := &marketdatav1.TickerResponse{
		Symbol:             "BTCUSDT",
		Price:              "97234.56",
		Quantity:           "0.00125",
		EventTime:          1730000000000,
		TradeId:            424242,
		BuyerOrderId:       10,
		SellerOrderId:      20,
		IsBuyerMarketMaker: true,
	}
	if err := p.Publish(context.Background(), trade); err != nil {
		t.Fatalf("publish: %v", err)
	}

	line := strings.TrimSpace(buf.String())
	if strings.Contains(line, "\n") {
		t.Fatal("expected a single line")
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	checks := map[string]any{
		"kind":      "trade",
		"symbol":    "BTCUSDT",
		"price":     "97234.56",
		"trade_id":  float64(424242), // encoding/json números viram float64
		"event_time": float64(1730000000000),
	}
	for k, want := range checks {
		if got := decoded[k]; got != want {
			t.Errorf("field %q: got %v, want %v", k, got, want)
		}
	}
}

func TestLogPublisherNilWriterDefaultsToStdout(t *testing.T) {
	// Apenas garante que o construtor não entra em pânico com nil.
	p := NewLogPublisher(nil)
	if p == nil {
		t.Fatal("expected non-nil publisher")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
