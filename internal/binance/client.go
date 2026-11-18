package binance

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/financial/marketdata/internal/metrics"
	"github.com/financial/marketdata/internal/normalizer"
	"github.com/financial/marketdata/internal/publisher"
)

const (
	defaultEndpoint = "wss://stream.binance.com:9443/stream"
	readLimitBytes  = 1 << 20 // 1 MiB
	readTimeout     = 75 * time.Second
	writeWait       = 10 * time.Second
)

// Client maintains a resilient connection to the Binance public WebSocket
// API and forwards normalized trades to a Publisher.
//
// The connection is a single combined stream (/stream?streams=btcusdt@trade/...)
// so N symbols multiplex over one TCP/TLS session. On any failure the client
// reconnects with exponential backoff + jitter (see Backoff).
type Client struct {
	endpoint  string
	streams   []string
	publisher publisher.Publisher
	metrics   *metrics.Registry
	logger    *slog.Logger
	backoff   *Backoff
	header    http.Header
}

// NewClient builds a Client for the given streams (e.g. "btcusdt@trade").
func NewClient(streams []string, pub publisher.Publisher, m *metrics.Registry, logger *slog.Logger) *Client {
	return &Client{
		endpoint:  defaultEndpoint,
		streams:   streams,
		publisher: pub,
		metrics:   m,
		logger:    logger,
		backoff:   NewBackoff(),
		header:    http.Header{"User-Agent": []string{"marketdata-gateway/1.0"}},
	}
}

// WithEndpoint overrides the Binance endpoint (useful for tests and for
// Binance testnet).
func (c *Client) WithEndpoint(endpoint string) *Client {
	c.endpoint = endpoint
	return c
}

// StreamURL returns the full combined-stream URL (exposed for tests/logging).
func (c *Client) StreamURL() string {
	return fmt.Sprintf("%s?streams=%s", c.endpoint, strings.Join(c.streams, "/"))
}

// Run blocks until ctx is cancelled. It automatically reconnects forever
// with exponential backoff + jitter on any connection or read failure.
func (c *Client) Run(ctx context.Context) error {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		url := c.StreamURL()
		c.logger.Info("connecting to binance", "url", url)
		conn, _, err := dialer.DialContext(ctx, url, c.header)
		if err != nil {
			wait := c.backoff.Next()
			c.metrics.Reconnects()
			c.logger.Warn("dial failed; will retry", "error", err, "attempt", c.backoff.Attempt(), "retry_in", wait)
			if !sleepOrDone(ctx, wait) {
				return ctx.Err()
			}
			continue
		}

		// Successful connect: reset the backoff budget.
		c.backoff.Reset()
		c.metrics.SetConnected(true)
		c.logger.Info("connected to binance", "streams", c.streams)

		readErr := c.readLoop(ctx, conn)
		_ = conn.Close()
		c.metrics.SetConnected(false)

		if ctx.Err() != nil {
			c.logger.Info("shutdown requested; gateway stopped")
			return ctx.Err()
		}

		wait := c.backoff.Next()
		c.metrics.Reconnects()
		c.logger.Warn("connection lost; reconnecting", "error", readErr, "retry_in", wait)
		if !sleepOrDone(ctx, wait) {
			return ctx.Err()
		}
	}
}

func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
	conn.SetReadLimit(readLimitBytes)

	extendReadDeadline := func() {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	}
	extendReadDeadline()

	// Binance sends a ping every ~20s. gorilla answers pongs automatically;
	// we also use any frame as a liveness signal to extend the read deadline.
	conn.SetPingHandler(func(appData string) error {
		extendReadDeadline()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeWait))
	})

	// If the context is cancelled during a quiet period, close the connection
	// so ReadMessage unblocks immediately instead of waiting for readTimeout.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stop:
		}
	}()

	for {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		extendReadDeadline()
		c.metrics.MessagesReceived()

		payload, err := normalizer.ExtractTradePayload(frame)
		if err != nil {
			c.metrics.ParseErrors()
			c.logger.Warn("dropping malformed frame", "error", err)
			continue
		}
		trade, err := normalizer.NormalizeTrade(payload)
		if err != nil {
			c.metrics.ParseErrors()
			c.logger.Warn("dropping invalid trade", "error", err)
			continue
		}
		c.metrics.SetLastEventTimeMs(trade.GetEventTime())

		if err := c.publisher.Publish(ctx, trade); err != nil {
			c.logger.Error("publish failed", "error", err, "symbol", trade.GetSymbol(), "trade_id", trade.GetTradeId())
			continue
		}
		c.metrics.MessagesPublished()
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
