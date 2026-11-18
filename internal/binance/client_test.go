package binance

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/financial/marketdata/internal/metrics"
	marketdatav1 "github.com/financial/marketdata/v1"
)

// chanPublisher is a test Publisher that pushes trades into a channel.
type chanPublisher struct {
	ch chan *marketdatav1.TickerResponse
}

func (p *chanPublisher) Publish(_ context.Context, t *marketdatav1.TickerResponse) error {
	p.ch <- t
	return nil
}
func (p *chanPublisher) Close() error { return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func fastBackoff() *Backoff {
	return NewBackoffWithRand(time.Millisecond, 5*time.Millisecond, 2.0, func() float64 { return 0 })
}

func tradeEnvelope(symbol string, price string, tradeID int64) string {
	return fmt.Sprintf(`{"stream":"%[1]s@trade","data":{"e":"trade","E":1730000000000,"s":"BTCUSDT",`+
		`"t":%d,"p":"%s","q":"0.001","T":1729999999990,"m":true,"M":true}}`,
		strings.ToLower(symbol), tradeID, price)
}

var upgrader = websocket.Upgrader{}

// TestClientStreamsAndPublishesTrade spins up a fake Binance WS server and
// verifies the full ingestion path: frame → normalizer → publisher.
func TestClientStreamsAndPublishesTrade(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(tradeEnvelope("btcusdt", "97234.56", 111)))
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	pub := &chanPublisher{ch: make(chan *marketdatav1.TickerResponse, 10)}
	m := metrics.NewRegistry()
	client := NewClient([]string{"btcusdt@trade"}, pub, m, testLogger()).
		WithEndpoint("ws" + strings.TrimPrefix(server.URL, "http")).
		WithBackoff(fastBackoff())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()

	select {
	case trade := <-pub.ch:
		if trade.GetSymbol() != "BTCUSDT" || trade.GetTradeId() != 111 || trade.GetPrice() != "97234.56" {
			t.Fatalf("unexpected trade: %+v", trade)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for published trade")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("client did not stop after cancel")
	}

	if got := m.Handler(); got == nil {
		t.Fatal("metrics handler must not be nil")
	}
}

// TestClientReconnectsAfterServerDrop verifies that when the server drops the
// connection the client dials again and keeps publishing.
func TestClientReconnectsAfterServerDrop(t *testing.T) {
	var conns int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&conns, 1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Send one trade then immediately close: simulates abrupt disconnect.
		_ = conn.WriteMessage(websocket.TextMessage, []byte(tradeEnvelope("btcusdt", "97234.56", 222)))
		_ = conn.Close()
	}))
	defer server.Close()

	pub := &chanPublisher{ch: make(chan *marketdatav1.TickerResponse, 10)}
	m := metrics.NewRegistry()
	client := NewClient([]string{"btcusdt@trade"}, pub, m, testLogger()).
		WithEndpoint("ws" + strings.TrimPrefix(server.URL, "http")).
		WithBackoff(fastBackoff())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	deadline := time.After(3 * time.Second)
	received := 0
	for received < 2 {
		select {
		case <-pub.ch:
			received++
		case <-deadline:
			t.Fatalf("expected trades from 2 connections, got %d (conns=%d)", received, atomic.LoadInt32(&conns))
		}
	}
	if atomic.LoadInt32(&conns) < 2 {
		t.Fatalf("expected at least 2 connections, got %d", conns)
	}
}

// TestClientSkipsMalformedFrames ensures a bad frame is counted and dropped
// while the following valid frame is still delivered.
func TestClientSkipsMalformedFrames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"definitely":"not a trade"`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(tradeEnvelope("btcusdt", "97234.56", 333)))
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	pub := &chanPublisher{ch: make(chan *marketdatav1.TickerResponse, 10)}
	m := metrics.NewRegistry()
	client := NewClient([]string{"btcusdt@trade"}, pub, m, testLogger()).
		WithEndpoint("ws" + strings.TrimPrefix(server.URL, "http")).
		WithBackoff(fastBackoff())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	select {
	case trade := <-pub.ch:
		if trade.GetTradeId() != 333 {
			t.Fatalf("expected trade 333 to survive the malformed frame, got %d", trade.GetTradeId())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for valid trade")
	}
}
