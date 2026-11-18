# Real-Time Dash — Market Data em Tempo Real (Binance)

Implementação da **Fase 1 do roadmap** descrito em
`Arquitetura RPC e Streaming de Dados em Tempo Real (Integração Binance).md`:

- ✅ Contrato `proto/market_data.proto` homologado (`marketdata.v1`) — base da
  geração de código de **Go** e **Java** a partir de uma única fonte de verdade.
- ✅ **Camada 1 (Aquisição Externa)** — Market Data Gateway em **Go**:
  WebSocket resiliente com a Binance, reconexão automática com
  *Exponential Backoff + Jitter*, parsing/validação de JSON → Protobuf e
  métricas em `/metrics`.
- ✅ **Camada 3 (Serviço Core de Mercado)** — iniciada em **Java** (gRPC sobre
  HTTP/2): interface `StreamTicker` (Server-Side Streaming) implementada,
  executando contra um `TradeSource` sintético. Kafka (Fase 2) substituirá a
  fonte sem mudar o contrato.
- ✅ Cliente de smoke test Go↔Java (`cmd/coreprobe`) validando a
  interoperabilidade do contrato entre as duas linguagens.

## Layout

```
proto/market_data.proto     # contrato único (fonte de verdade)
v1/                         # bindings Go gerados (protoc-gen-go)
cmd/gateway/                # gateway de aquisição (Go)
cmd/coreprobe/              # cliente gRPC de smoke test (Go → Java)
internal/binance/           # cliente WS + backoff/jitter
internal/normalizer/        # JSON Binance → TickerResponse (validação)
internal/publisher/         # sink dos trades (stdout; Kafka chega na Fase 2)
internal/metrics/           # contadores/gauges + endpoint /metrics
core/                       # serviço core gRPC (Java 21, Maven)
```

## Pré-requisitos

- Go ≥ 1.23, Java 21, Maven ≥ 3.8, `protoc` ≥ 29 (apenas para regenerar os bindings Go)
- Rede com acesso a `stream.binance.com`

## Regenerar bindings

```bash
# Go (protoc + protoc-gen-go + protoc-gen-go-grpc)
protoc -I=proto --go_out=. --go_opt=module=github.com/financial/marketdata \
                --go-grpc_out=. --go-grpc_opt=module=github.com/financial/marketdata \
                proto/market_data.proto

# Java (feito automaticamente pelo Maven via protobuf-maven-plugin; proto vai de ../proto)
cd core && mvn compile
```

## Executar o Gateway (Go)

```bash
go run ./cmd/gateway --symbols BTCUSDT,ETHUSDT --metrics-addr :9090
# trades normalizados → linhas JSON no stdout
# métricas → curl localhost:9090/metrics   (/healthz também disponível)
```

## Executar o Core (Java)

```bash
cd core && mvn -q package
java -jar target/core-1.0.0-SNAPSHOT.jar --port 50051   # default: 50051
```

## Smoke test ponta a ponta (Go → Java via gRPC)

```bash
go run ./cmd/coreprobe --symbol BTCUSDT --count 10
go run ./cmd/coreprobe --symbol ETHUSDT --aggregation 1000 --count 5   # janelas VWAP de 1s
```

## Testes

```bash
go test ./...                 # unit tests (backoff/jitter, normalizer)
cd core && mvn test           # in-process gRPC: streaming, validação, VWAP
```

## Decisões e notas

- **`event_time`** mapeia para o campo `E` (event time) da exchange.
- O stream `@trade` de **spot** não expõe `b`/`a` (buyer/seller order id);
  eles vêm apenas em futures e são lidos tolerando número **ou** string,
  com default `0`.
- `aggregation_interval_ms > 0` agrega trades em janelas fixas de VWAP
  (ver `TradeAggregator`); sem trades na janela, nada é emitido.
- Backoff do gateway: base 500ms, fator 2.0, teto 30s, *equal jitter*;
  resetado após conexão bem-sucedida.
- A Fase 2 (Kafka + idempotency/`trade_id` via Redis) e a Fase 3
  (BFF/WebSocket, observabilidade Prometheus/Grafana/OTel) ainda não
  foram iniciadas.
