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
make test        # tudo
make test-go     # go test -race ./...
make test-java   # (cd core && mvn test)
```

Cobertura automatizada atual (Fase 1):

| Componente | Testes | O que cobre |
| :--- | :--- | :--- |
| `internal/binance` | `backoff_test.go`, `client_test.go` | Crescimento/teto/jitter/reset do backoff; WS fake via `httptest`: ingestão ponta a ponta, **reconexão após queda abrupta**, frames malformados ignorados, cancelamento via context (`-race` limpo) |
| `internal/normalizer` | `normalizer_test.go` | Envelope `stream+data`, frame cru, validação de preço/qty/trade_id, variantes de `b`/`a` (spot ausente, futures string/número) |
| | `regression_test.go` | Regressão: `b:null`/`a:null` não podem derrubar o frame; `"data":null` no envelope é rejeitado; frame `null` nunca vira trade |
| `internal/publisher` | `publisher_test.go` | Linha JSON válida com todos os campos do contrato |
| `internal/metrics` | `metrics_test.go` | Formato Prometheus, `/healthz`, contadores |
| | `regression_test.go` | Regressão: métricas `*_total` expostas com `# TYPE ... counter` (eram `gauge`) |
| `core` (Java) | `StreamTickerTest` | Streaming via gRPC in-process, símbolo vazio/intervalo inválido → `INVALID_ARGUMENT`, janelas VWAP |
| | `SimulatedTradeSourceTest` | Emissão periódica, `trade_id` monotônico, isolamento por símbolo, unsubscribe/close param a emissão |
| | `TradeAggregatorTest` | VWAP exato da janela, janela vazia não emite, janelas separadas geram buckets separados |
| | `RegressionTest` | Regressão: mensagens top-level em `com.financial.marketdata.v1` (falta de `java_multiple_files` quebrou todo o compile); classe externa `MarketData` sem mensagens aninhadas |

Não cobertos (dependem de ambiente real): entrega efetiva na Binance (smoke
manual, ver README) e carga/rate limit. Fase 2 adicionará testes de dedup
`trade_id`/Redis e de falha do Kafka.

## CI/CD

Três camadas, todas rodando os **mesmos testes localmente e no GitHub**:

| Camada | Onde roda | O que faz |
| :--- | :--- | :--- |
| `make ci` | local | `go vet` + `go build` + `go test -race` + `mvn -B clean verify` — idêntico ao pipeline |
| git hook `pre-push` | local (instalado) | roda `make ci` antes de cada `git push`; instale/remova com `make install-hooks` / `make uninstall-hooks`; pule com `git push --no-verify` |
| `.github/workflows/ci.yml` | GitHub (push/PR em `main`) | 3 jobs: `go` (vet/build/test -race), `java` (test + package shade), `contract` (regenera bindings Go do `.proto` e falha se `v1/` divergir) |

Cada job da CI espelha um alvo do Makefile — se a CI falhar, rode `make ci`
localmente, corrija e faça push de novo (o hook já teria barrado antes).

Para rodar o workflow do GitHub Actions localmente via [`act`](https://github.com/nektos/act)
(requer Docker):

```bash
act -j go       # job Go
act -j contract # drift check do contrato
```

Observação: o job `contract` usa `protoc` 29.1 e plugins pinados
(`protoc-gen-go` v1.36.5, `protoc-gen-go-grpc` v1.5.1); se você regenerar
`v1/` com versões diferentes, alinhe os pins em `.github/workflows/ci.yml`.

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
