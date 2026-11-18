// core-probe é um cliente gRPC mínimo (Go) para o serviço core de mercado
// (Java). Serve para smoke-test manual do contrato marketdata.v1.MarketDataService:
// valida a interoperabilidade Go↔Java do StreamTicker (Server-Side Streaming).
//
// Uso:
//
//	go run ./cmd/coreprobe --addr localhost:50051 --symbol BTCUSDT --count 10
//	go run ./cmd/coreprobe --symbol ETHUSDT --aggregation 1000 --count 5
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	marketdatav1 "github.com/financial/marketdata/v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "endereço do serviço core gRPC")
	symbol := flag.String("symbol", "BTCUSDT", "símbolo do ativo (ex: BTCUSDT)")
	aggregation := flag.Int("aggregation", 0, "aggregation_interval_ms (0 = sem agregação)")
	count := flag.Int("count", 10, "número de mensagens a ler antes de encerrar")
	timeout := flag.Duration("timeout", 15*time.Second, "timeout máximo de espera")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := marketdatav1.NewMarketDataServiceClient(conn)
	stream, err := client.StreamTicker(ctx, &marketdatav1.TickerRequest{
		Symbol:               *symbol,
		AggregationIntervalMs: int32(*aggregation),
	})
	if err != nil {
		log.Fatalf("streamTicker: %v", err)
	}

	fmt.Printf("streaming %s from %s (aggregation=%dms)...\n", *symbol, *addr, *aggregation)
	for i := 0; i < *count; i++ {
		msg, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("server closed the stream")
			return
		}
		if err != nil {
			log.Fatalf("recv: %v", err)
		}
		fmt.Printf("event_time=%d trade_id=%d %s price=%s qty=%s mm=%v\n",
			msg.GetEventTime(), msg.GetTradeId(), msg.GetSymbol(),
			msg.GetPrice(), msg.GetQuantity(), msg.GetIsBuyerMarketMaker())
	}
}
