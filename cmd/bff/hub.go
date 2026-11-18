package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	marketdatav1 "github.com/financial/marketdata/v1"
)

// hub multiplexa: 1 stream StreamTicker (gRPC → core) por símbolo,
// distribuído para N clientes WebSocket — o browser nunca fala com o core
// diretamente (Camada 4).
//
// Política de consumidor lento no BFF: cliente que não drena a fila
// dedicada (256 mensagens) é desconectado — mesmo espírito do core.
const wsClientBuffer = 256

var tracer = otel.Tracer("bff")

type hub struct {
	coreAddr string
	metrics  *bffMetrics

	mu      sync.Mutex
	feeds   map[string]*symbolFeed
	allowed map[string]struct{}
}

type symbolFeed struct {
	symbol  string
	mu      sync.Mutex
	clients map[*wsClient]struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
}

type wsClient struct {
	conn          *websocket.Conn
	send          chan []byte
	metricCounted atomic.Bool
}

func newHub(coreAddr string, symbols []string, m *bffMetrics) *hub {
	allowed := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		if s = strings.TrimSpace(strings.ToUpper(s)); s != "" {
			allowed[s] = struct{}{}
		}
	}
	return &hub{
		coreAddr: coreAddr,
		metrics:  m,
		feeds:    map[string]*symbolFeed{},
		allowed:  allowed,
	}
}

func (h *hub) allowSymbol(symbol string) bool {
	_, ok := h.allowed[strings.ToUpper(strings.TrimSpace(symbol))]
	return ok
}

func (h *hub) subscribe(symbol string, conn *websocket.Conn) (*wsClient, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if !h.allowSymbol(symbol) {
		return nil, errors.New("symbol not allowed: " + symbol)
	}

	c := &wsClient{conn: conn, send: make(chan []byte, wsClientBuffer)}

	h.mu.Lock()
	feed, ok := h.feeds[symbol]
	if !ok {
		ctx, cancel := context.WithCancel(context.Background())
		feed = &symbolFeed{symbol: symbol, clients: map[*wsClient]struct{}{}, ctx: ctx, cancel: cancel}
		h.feeds[symbol] = feed
	}
	feed.mu.Lock()
	feed.clients[c] = struct{}{}
	first := !feed.started
	feed.started = true
	feed.mu.Unlock()
	h.mu.Unlock()

	if first {
		go h.runFeed(feed)
	}

	h.metrics.wsConnections.Inc()
	c.metricCounted.Store(true)
	h.metrics.wsActive.Add(1)
	h.metrics.wsConnected.Set(1)
	return c, nil
}

func (h *hub) unsubscribe(symbol string, c *wsClient) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	h.mu.Lock()
	feed, ok := h.feeds[symbol]
	if !ok {
		h.mu.Unlock()
		return
	}
	feed.mu.Lock()
	delete(feed.clients, c)
	empty := len(feed.clients) == 0
	feed.mu.Unlock()
	if empty {
		delete(h.feeds, symbol)
		feed.cancel()
	}
	h.mu.Unlock()

	h.deregisterClientMetrics(c)
}

func (h *hub) broadcast(feed *symbolFeed, msg []byte) {
	feed.mu.Lock()
	kicked := []*wsClient{}
	for c := range feed.clients {
		select {
		case c.send <- msg:
			h.metrics.messagesSent.Inc()
		default:
			slog.Warn("bff: terminating slow websocket client", "symbol", feed.symbol)
			kicked = append(kicked, c)
		}
	}
	feed.mu.Unlock()
	for _, c := range kicked {
		h.kickClient(feed, c)
	}
}

// kickClient remove um cliente lento com a ordem de locks h.mu → feed.mu
// (mesma de subscribe/unsubscribe) — evita deadlock ABBA.
func (h *hub) kickClient(feed *symbolFeed, c *wsClient) {
	h.mu.Lock()
	feed.mu.Lock()
	if _, ok := feed.clients[c]; !ok {
		feed.mu.Unlock()
		h.mu.Unlock()
		return
	}
	delete(feed.clients, c)
	close(c.send)
	_ = c.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "slow consumer"),
		time.Now().Add(time.Second),
	)
	_ = c.conn.Close()
	empty := len(feed.clients) == 0
	feed.mu.Unlock()
	if empty {
		delete(h.feeds, feed.symbol)
		feed.cancel()
	}
	h.mu.Unlock()
	h.deregisterClientMetrics(c)
}

// deregisterClientMetrics decrementa contadores exatamente uma vez por cliente,
// independente do caminho de saída (client close vs slow-consumer kick).
func (h *hub) deregisterClientMetrics(c *wsClient) {
	if !c.metricCounted.CompareAndSwap(true, false) {
		return
	}
	h.metrics.wsConnections.Dec()
	if h.metrics.wsActive.Add(-1) == 0 {
		h.metrics.wsConnected.Set(0)
	}
}

// runFeed possui o StreamTicker upstream de um símbolo e faz o fan-out.
// Reconecta indefinidamente até o feed ser cancelado (zero clientes).
func (h *hub) runFeed(feed *symbolFeed) {
	for {
		err := h.streamOnce(feed.ctx, feed)
		if feed.ctx.Err() != nil {
			return
		}
		h.metrics.upstreamErrors.Inc()
		slog.Warn("bff: upstream stream ended; reconnecting in 2s",
			"error", err, "symbol", feed.symbol)
		select {
		case <-feed.ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (h *hub) streamOnce(ctx context.Context, feed *symbolFeed) error {
	conn, err := grpc.NewClient(h.coreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := marketdatav1.NewMarketDataServiceClient(conn)
	stream, err := client.StreamTicker(ctx, &marketdatav1.TickerRequest{Symbol: feed.symbol})
	if err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return errors.New("server closed the stream")
		}
		if err != nil {
			return err
		}

		_, span := tracer.Start(ctx, "bff.upstream.recv")
		span.SetAttributes(
			attribute.String("symbol", msg.GetSymbol()),
			attribute.Int64("trade_id", msg.GetTradeId()),
			attribute.Float64("event_age_ms", float64(time.Now().UnixMilli()-msg.GetEventTime())),
		)

		out, err := json.Marshal(map[string]any{
			"type":                  "trade",
			"symbol":                msg.GetSymbol(),
			"price":                 msg.GetPrice(),
			"quantity":              msg.GetQuantity(),
			"event_time":            msg.GetEventTime(),
			"trade_id":              msg.GetTradeId(),
			"buyer_order_id":        msg.GetBuyerOrderId(),
			"seller_order_id":       msg.GetSellerOrderId(),
			"is_buyer_market_maker": msg.GetIsBuyerMarketMaker(),
		})
		if err != nil {
			span.End()
			continue
		}
		h.broadcast(feed, out)
		span.End()
	}
}
