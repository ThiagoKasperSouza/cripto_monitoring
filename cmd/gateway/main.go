// Market Data Gateway (Camada 1 — Aquisição Externa).
//
// Responsibilities (Fase 1 do roadmap):
//   - Manter conexão WebSocket persistente e resiliente com a Binance
//     (reconexão automática com Exponential Backoff + Jitter).
//   - Converter payloads JSON brutos em mensagens Protobuf do contrato
//     marketdata.v1 (TickerResponse).
//   - Emitir métricas de latência/estado em /metrics (default :9090).
//
// A entrega para o barramento (Kafka/Redpanda) é configurada por
// --kafka-brokers; sem brokers, os trades vão como JSON lines no stdout.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/financial/marketdata/internal/binance"
	"github.com/financial/marketdata/internal/metrics"
	"github.com/financial/marketdata/internal/publisher"
)

func main() {
	if err := run(); err != nil {
		// context.Canceled is the expected result of SIGINT/SIGTERM.
		if !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
			os.Exit(1)
		}
	}
}

func run() error {
	symbolsFlag := flag.String("symbols", "BTCUSDT,ETHUSDT", "comma-separated list of symbols to stream (e.g. BTCUSDT,ETHUSDT)")
	metricsAddr := flag.String("metrics-addr", ":9090", "address for the /metrics and /healthz HTTP endpoints (empty to disable)")
	kafkaBrokers := flag.String("kafka-brokers", "", "comma-separated Kafka/Redpanda brokers (e.g. localhost:9092). Empty = stdout log publisher")
	kafkaTopic := flag.String("kafka-topic", "market.trades", "topic to publish normalized trades to")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	streams := make([]string, 0)
	for _, s := range strings.Split(*symbolsFlag, ",") {
		s = strings.TrimSpace(strings.ToLower(s))
		if s != "" {
			streams = append(streams, s+"@trade")
		}
	}
	if len(streams) == 0 {
		return errors.New("no symbols provided via --symbols")
	}

	logger.Info("gateway starting", "symbols", *symbolsFlag, "metrics_addr", *metricsAddr)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	registry := metrics.NewRegistry()
	if *metricsAddr != "" {
		server := &http.Server{
			Addr:              *metricsAddr,
			Handler:           registry.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			<-ctx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer shutdownCancel()
			_ = server.Shutdown(shutdownCtx)
		}()
		go func() {
			logger.Info("metrics endpoint listening", "addr", *metricsAddr)
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server failed", "error", err)
			}
		}()
	}

	var pub publisher.Publisher
	var mode string
	if *kafkaBrokers != "" {
		brokers := strings.Split(*kafkaBrokers, ",")
		kafkaPub, err := publisher.NewKafkaPublisher(brokers, *kafkaTopic)
		if err != nil {
			return fmt.Errorf("kafka publisher: %w", err)
		}
		pub, mode = kafkaPub, "kafka topic="+*kafkaTopic
	} else {
		pub = publisher.NewLogPublisher(os.Stdout)
		mode = "stdout"
	}
	defer func() { _ = pub.Close() }()
	logger.Info("publisher configured", "mode", mode)

	client := binance.NewClient(streams, pub, registry, logger)
	return client.Run(ctx)
}
