package publisher

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	marketdatav1 "github.com/financial/marketdata/v1"
)

func TestNewKafkaPublisherValidatesArgs(t *testing.T) {
	if _, err := NewKafkaPublisher(nil, "market.trades"); err == nil {
		t.Fatal("expected error for empty brokers")
	}
	if _, err := NewKafkaPublisher([]string{" "}, "market.trades"); err == nil {
		t.Fatal("expected error for blank broker entry")
	}
	if _, err := NewKafkaPublisher([]string{"localhost:9092"}, ""); err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestBuildRecordKeyHeadersAndRoundTrip(t *testing.T) {
	trade := &marketdatav1.TickerResponse{
		Symbol:    "BTCUSDT",
		Price:     "97234.56",
		Quantity:  "0.5",
		EventTime: 1730000000000,
		TradeId:   6877325976,
	}
	rec, err := buildRecord("market.trades", trade)
	if err != nil {
		t.Fatalf("buildRecord: %v", err)
	}
	if string(rec.Key) != "BTCUSDT" {
		t.Fatalf("key must be the symbol (per-partition ordering), got %q", rec.Key)
	}
	if rec.Topic != "market.trades" {
		t.Fatalf("topic: got %q", rec.Topic)
	}

	header := map[string]string{}
	for _, h := range rec.Headers {
		header[h.Key] = string(h.Value)
	}
	if header["trade_id"] != "6877325976" {
		t.Fatalf("trade_id header: got %q", header["trade_id"])
	}
	if header["event_time"] != "1730000000000" {
		t.Fatalf("event_time header: got %q", header["event_time"])
	}

	decoded := &marketdatav1.TickerResponse{}
	if err := proto.Unmarshal(rec.Value, decoded); err != nil {
		t.Fatalf("round-trip unmarshal: %v", err)
	}
	if decoded.GetTradeId() != trade.GetTradeId() || decoded.GetPrice() != trade.GetPrice() ||
		decoded.GetSymbol() != trade.GetSymbol() {
		t.Fatalf("round-trip mismatch: %+v", decoded)
	}
}

func TestKafkaPublisherCloseDoesNotPanic(t *testing.T) {
	// Não conecta no broker no construtor (kgo é lazy), então podemos
	// construir e fechar sem infraestrutura.
	p, err := NewKafkaPublisher([]string{"localhost:9092"}, "market.trades")
	if err != nil {
		t.Fatalf("NewKafkaPublisher: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = context.Background()
}
