.PHONY: build test test-go test-java gateway core probe proto clean

build: ## compila Go e empacota o core Java
	go build ./...
	cd core && mvn -q -B -DskipTests package

test: test-go test-java ## roda toda a suíte automatizada

ci: ## pipeline completo localmente (= jobs da CI): vet + build + testes + package
	go vet ./...
	go build ./...
	go test -race -count=1 ./...
	cd core && mvn -B clean verify

install-hooks: ## instala o git hook pre-push que roda `make ci` antes de cada push
	@mkdir -p .git/hooks
	@cp scripts/pre-push .git/hooks/pre-push
	@chmod +x .git/hooks/pre-push
	@echo "pre-push hook instalado (.git/hooks/pre-push) — pule com: git push --no-verify"

uninstall-hooks: ## remove o git hook pre-push
	@rm -f .git/hooks/pre-push
	@echo "pre-push hook removido"

test-go: ## unit + integração leve (WS fake) do gateway, com race detector
	go test -race ./...

test-java: ## testes do core gRPC (in-process) e agregador VWAP
	cd core && mvn -B test

proto: ## regenera os bindings Go a partir de proto/market_data.proto
	protoc -I=proto --go_out=. --go_opt=module=github.com/financial/marketdata \
		--go-grpc_out=. --go-grpc_opt=module=github.com/financial/marketdata \
		proto/market_data.proto

gateway: ## roda o gateway (BTCUSDT,ETHUSDT, métricas em :9090)
	go run ./cmd/gateway --symbols BTCUSDT,ETHUSDT --metrics-addr :9090

gateway-kafka: ## sobe o gateway publicando no tópico market.trades
	go run ./cmd/gateway --symbols BTCUSDT,ETHUSDT --metrics-addr :9090 --kafka-brokers localhost:9092 --kafka-topic market.trades

loadprobe: ## soak test: gera carga sintética no market.trades (500 msg/s por 10s)
	go run ./cmd/loadprobe --symbol LOADTEST --rate 500 --duration 10s

core: ## empacota e roda o serviço core gRPC (Fase 1, fonte simulada)
	cd core && mvn -q -B -DskipTests package && java -jar target/core-1.0.0-SNAPSHOT.jar --port 50051

core-kafka: ## empacota e roda o core consumindo market.trades com dedup Redis
	cd core && mvn -q -B -DskipTests package && java -jar target/core-1.0.0-SNAPSHOT.jar \
		--port 50051 --source kafka --brokers localhost:9092 --topic market.trades \
		--group market-data-core --dedup redis --redis-addr localhost:6379

infra-up: ## sobe Redpanda (Kafka API) + Redis e cria o tópico market.trades
	cd deploy && docker compose up -d

infra-down: ## derruba a infra e remove volumes
	cd deploy && docker compose down -v

probe: ## smoke test Go→Java: BTCUSDT, 10 mensagens
	go run ./cmd/coreprobe --symbol BTCUSDT --count 10

clean:
	rm -rf core/target gateway coreprobe
