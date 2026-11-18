// BFF — Backend For Frontend (Camada 4).
//
// Consome o StreamTicker gRPC interno do serviço core de mercado e entrega
// para o browser via WebSocket (/ws?symbol=BTCUSDT). Também faz proxy com
// cache das APIs públicas REST da Binance usadas pela UI
// (/api/ticker24h, /api/klines) — a UI nunca fala com a Binance diretamente.
//
// Uso:
//
//	go run ./cmd/bff --listen :8080 --core-addr localhost:50051
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/financial/marketdata/internal/tracing"
)

//go:embed static
var staticFS embed.FS

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// Dev/demo: aceite qualquer origem. Em produção, restrinja aos domínios do frontend.
	CheckOrigin: func(*http.Request) bool { return true },
}

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "bff: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":8080", "endereço do BFF (UI + /ws + proxies + /metrics)")
	coreAddr := flag.String("core-addr", "localhost:50051", "endereço do serviço core gRPC")
	symbolsFlag := flag.String("symbols",
		"BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT",
		"pares aceitos no seletor da UI e no WebSocket (comma-separated)")
	binanceAPI := flag.String("binance-api", "https://api.binance.com", "base URL da API pública REST da Binance")
	cacheTTL := flag.Duration("cache-ttl", 5*time.Second, "TTL do cache dos proxies da Binance")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	shutdown, err := tracing.Setup("bff")
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	metrics := newBffMetrics()
	symbols := strings.Split(*symbolsFlag, ",")
	hub := newHub(*coreAddr, symbols, metrics)

	proxy := &proxyConfig{baseURL: strings.TrimRight(*binanceAPI, "/"), cache: newTTLCache(), ttl: *cacheTTL}

	mux := http.NewServeMux()

	// Estáticos da UI (embutidos no binário)
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return err
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// Métricas + saúde
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// API da própria UI
	mux.HandleFunc("/api/symbols", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		syms := strings.Split(*symbolsFlag, ",")
		out := make([]string, 0, len(syms))
		for _, s := range syms {
			s = strings.TrimSpace(strings.ToUpper(s))
			if s != "" {
				out = append(out, s)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"symbols": out})
	})
	mux.HandleFunc("/api/ticker24h", proxy.handleTicker24h)
	mux.HandleFunc("/api/klines", proxy.handleKlines)

	// WebSocket de trades em tempo real
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return // upgrader já respondeu o erro HTTP
		}
		c, err := hub.subscribe(symbol, conn)
		if err != nil {
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "symbol not allowed"),
				time.Now().Add(time.Second))
			_ = conn.Close()
			return
		}
		slog.Info("bff: ws client subscribed", "symbol", symbol, "remote", r.RemoteAddr)

		done := make(chan struct{})
		go wsWriteLoop(conn, c, hub, symbol, done)
		wsReadLoop(conn) // bloqueia até o cliente fechar
		slog.Info("bff: ws client gone", "symbol", symbol, "remote", r.RemoteAddr)
		hub.unsubscribe(symbol, c)
		<-done
	})

	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	slog.Info("bff listening", "addr", *listen, "core", *coreAddr, "symbols", *symbolsFlag)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// wsWriteLoop drena a fila do cliente para o socket e responde pings.
// Quando a fila encheu e o cliente lento foi desconectado pelo hub, o canal
// `send` é fechado e o loop termina.
func wsWriteLoop(conn *websocket.Conn, c *wsClient, h *hub, symbol string, done chan<- struct{}) {
	defer close(done)
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return // cliente lento removido pelo hub
			}
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ping.C:
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second)); err != nil {
				return
			}
		}
	}
}

// wsReadLoop apenas drena mensagens do cliente (keepalive/close) — o cliente
// do browser é read-only do ponto de vista da aplicação.
func wsReadLoop(conn *websocket.Conn) {
	conn.SetReadLimit(4 << 10)
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}
