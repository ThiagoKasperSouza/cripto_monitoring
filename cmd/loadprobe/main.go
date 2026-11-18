// loadprobe gera carga sintética de trades no tópico market.trades para
// validar o caminho Kafka→core→gRPC sob volume (teste de carga/soak da
// Fase 2), sem depender do ritmo da Binance real.
//
// Uso:
//
//	go run ./cmd/loadprobe --symbol LOADTEST --rate 500 --duration 10
//	# termina imprimindo sent/errors e a vazão efetiva (msg/s)
//
// trade_id é derivado de UnixNano()+sequencial — único entre runs, então a
// dedup ({symbol}:{trade_id}) nunca descarta mensagens novas por acidente.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	marketdatav1 "github.com/financial/marketdata/v1"
)

func main() {
	brokersFlag := flag.String("brokers", "localhost:9092", "comma-separated Kafka/Redpanda brokers")
	topic := flag.String("topic", "market.trades", "tópico de destino")
	symbol := flag.String("symbol", "LOADTEST", "símbolo sintético dos trades")
	rate := flag.Int("rate", 500, "trades por segundo")
	duration := flag.Duration("duration", 10*time.Second, "duração do envio (ex: 10s, 1m)")
	flag.Parse()

	if *rate <= 0 || *duration <= 0 {
		log.Fatal("--rate and --duration must be > 0")
	}
	brokers := strings.Split(*brokersFlag, ",")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		log.Fatalf("kgo client: %v", err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	base := time.Now().UnixNano()
	interval := time.Second / time.Duration(*rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	deadline := time.After(*duration)

	var sent, failed int64
	start := time.Now()

	for i := int64(0); ; i++ {
		select {
		case <-ctx.Done():
			fmt.Printf("interrupted: sent=%d failed=%d\n", sent, failed)
			return
		case <-deadline:
			elapsed := time.Since(start).Seconds()
			fmt.Printf("done: sent=%d failed=%d effective_rate=%.1f msg/s in %.1fs\n", sent, failed, float64(sent)/elapsed, elapsed)
			return
		case <-ticker.C:
			trade := &marketdatav1.TickerResponse{
				Symbol:    *symbol,
				Price:     "100.00000000",
				Quantity:  "1.00000000",
				EventTime: time.Now().UnixMilli(),
				TradeId:   base + i,
			}
			value, err := proto.Marshal(trade)
			if err != nil {
				failed++
				continue
			}
			rec := &kgo.Record{
				Topic: *topic,
				Key:   []byte(*symbol),
				Value: value,
				Headers: []kgo.RecordHeader{
					{Key: "trade_id", Value: []byte(fmt.Sprintf("%d", trade.GetTradeId()))},
					{Key: "source", Value: []byte("loadprobe")},
				},
			}
			if err := client.ProduceSync(ctx, rec).FirstErr(); err != nil {
				failed++
			} else {
				sent++
			}
		}
	}
}
