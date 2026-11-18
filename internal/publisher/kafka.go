package publisher

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	marketdatav1 "github.com/financial/marketdata/v1"
)

// KafkaPublisher sends normalized trades to the event backbone (Camada 2).
// Messages carry the TickerResponse protobuf as value and the *symbol* as key.
//
// Nota sobre a chave (divergência documentada em relação ao doc original):
// o documento sugere trade_id como chave, mas chaves únicas a cada mensagem
// distribuem trades do mesmo ativo em partições arbitrárias e quebram a
// ordem cronológica por ativo. A chave correta para o objetivo declarado
// ("todas as atualizações de um mesmo ativo roteadas para a mesma partição,
// preservando a ordem cronológica") é o symbol. O trade_id segue disponível
// como header para rastreabilidade e como dedup key dos consumidores.
type KafkaPublisher struct {
	topic  string
	client *kgo.Client
}

// NewKafkaPublisher builds a producer for brokers (host:port list) writing to
// topic. acks=all and an idempotent producer (franz-go default with acks=all)
// are used for durability and exactly-once-ish production.
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka: at least one broker is required")
	}
	for i, b := range brokers {
		brokers[i] = strings.TrimSpace(b)
		if brokers[i] == "" {
			return nil, errors.New("kafka: broker list contains an empty entry")
		}
	}
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("kafka: topic is required")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: create client: %w", err)
	}
	return &KafkaPublisher{topic: topic, client: client}, nil
}

// buildRecord assembles the Kafka record — factored out for tests.
func buildRecord(topic string, trade *marketdatav1.TickerResponse) (*kgo.Record, error) {
	value, err := proto.Marshal(trade)
	if err != nil {
		return nil, fmt.Errorf("marshal trade: %w", err)
	}
	return &kgo.Record{
		Topic: topic,
		Key:   []byte(trade.GetSymbol()),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "trade_id", Value: []byte(strconv.FormatInt(trade.GetTradeId(), 10))},
			{Key: "event_time", Value: []byte(strconv.FormatInt(trade.GetEventTime(), 10))},
		},
	}, nil
}

// Publish marshals the trade and produces it synchronously (waiting for the
// broker ack), so backpressure surfaces in the ingestion loop instead of
// unbounded internal buffering.
func (p *KafkaPublisher) Publish(ctx context.Context, trade *marketdatav1.TickerResponse) error {
	rec, err := buildRecord(p.topic, trade)
	if err != nil {
		return err
	}
	res := p.client.ProduceSync(ctx, rec)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("kafka produce to %s: %w", p.topic, err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
	p.client.Close()
	return nil
}
