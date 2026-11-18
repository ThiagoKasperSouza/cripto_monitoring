# **Arquitetura RPC e Streaming de Dados em Tempo Real (Integração Binance)**

# **Visão Geral e Objetivo da Arquitetura**

O presente documento estabelece o design arquitetural para um sistema distribuído de processamento e distribuição de dados financeiros de alta frequência em tempo real. O objetivo primário é ingerir múltiplos fluxos de mercado provenientes das APIs de alta performance da Binance, processá-los internamente com latência sub-milissegundo predizível e entregá-los de forma consistente para dashboards analíticos e sistemas de execução.

```
[Binance WebSocket] ──> [Adaptador / Gateway] ──> [Apache Kafka] ──> [gRPC Core Services] ──> [BFF] ──> [Web Dashboards]
```

Para sustentar volumes elevados de mensagens sem comprometer a estabilidade do ecossistema, a camada interna de comunicação adota gRPC (HTTP/2) em detrimento de abordagens tradicionais baseadas em REST/JSON. As justificativas técnicas para essa decisão englobam:

* **Contratos Fortemente Tipados:** O uso de Protocol Buffers (proto3) garante um esquema estrito, evolutivo e com checagem de tipos em tempo de compilação, eliminando erros de serialização comuns em JSON.  
* **Eficiência de Serialização Binária:** Protocol Buffers gera cargas úteis (payloads) significativamente menores e requer menor uso de CPU para parse em comparação com texto puro (JSON/XML).  
* **Multiplexação e Conexões Persistentes sobre HTTP/2:** Suporte a múltiplos fluxos bidirecionais simultâneos sobre um único socket TCP, reduzindo severamente o overhead de handshake TLS/TCP e eliminando o problema de *Head-of-Line Blocking* no nível da aplicação.  
* **Comunicação Poliglota Nativa:** Permite a coexistência de microsserviços desenvolvidos em linguagens distintas (como Go para ingestão, C++ ou Rust para execução e Node.js/Python para serviços de suporte), utilizando código cliente/servidor gerado automaticamente.  
* **Streaming Nativo e Controle de Fluxo:** Suporte de primeira classe para *Server-Side Streaming* e *Bidirectional Streaming*, integrando mecanismos nativos de *backpressure* no nível do transporte.

---

# **Topologia e Fluxo de Dados de Ponta a Ponta**

A arquitetura é dividida em quatro camadas funcionais isoladas para garantir desacoplamento, escalabilidade horizontal e tolerância a falhas.

## **Camada 1: Aquisição Externa (Binance Market Data Gateway)**

Esta camada é composta por adaptadores especializados responsáveis por manter conexões WebSocket persistentes e resilientes com os endpoints da Binance (ex: `wss://stream.binance.com:9443/ws/btcusdt@trade`). Mais informações em `https://developers.binance.com/en/docs/products/spot/web-socket-streams`

* **Ingestão e Normalização:** O adaptador recebe os quadros JSON brutos, valida a integridade dos payloads e realiza a tradução imediata para os objetos binários gerados pelo Protobuf.  
* **Isolamento de Conectividade:** Impede que instabilidades de rede externa afetem os barramentos internos.

## **Camada 2: Barramento Central de Eventos (Apache Kafka)**

Atua como o espinha dorsal de eventos (Event Backbone) do sistema, fornecendo:

* **Desacoplamento Temporal:** A velocidade de consumo dos serviços internos não afeta a taxa de ingestão do adaptador da Binance.  
* **Bufferização contra Picos de Volatilidade:** Durante eventos de alta volatilidade no mercado, o Kafka absorve aumentos repentinos na taxa de mensagens por segundo (througput burst) sem perda de dados.  
* **Retenção e Fan-Out:** Permite que múltiplos consumidores independentes (módulos de auditoria, motores de risco, dashboards e agregadores de candlestick) processem o mesmo stream de dados simultaneamente sem interferência mútua.

## **Camada 3: Comunicação Interna (gRPC Streaming)**

Os serviços core de mercado consomem os eventos do Kafka e expõem interfaces gRPC baseadas em *Server-Side Streaming* para redistribuição interna de baixíssima latência.

* **Controle de Backpressure Nativo:** Se um consumidor interno entra em estado de degradação, as janelas de fluxo do HTTP/2 sinalizam ao produtor gRPC para reduzir a taxa de envio, evitando estouro de memória (*OutOfMemory errors*) nos nós distribuídos.  
* **Eficiência de Recursos:** Redução substancial de footprint de rede e CPU no cluster interno.

## **Camada 4: Camada de Borda / BFF (Backend For Frontend) e Entrega**

O frontend (navegadores web e clientes mobile) não deve consumir diretamente o Apache Kafka nem gRPC puro, uma vez que:

1. **Limitações do Navegador:** Browsers não possuem suporte nativo completo ao protocolo HTTP/2 frames brutos exigidos por clientes gRPC padrão sem uma camada de proxy.  
2. **Segurança e Autorização:** O Kafka não deve ser exposto publicamente para evitar riscos de segurança e acesso não autorizado a tópicos internos.  
3. **Filtragem de Payload:** O frontend necessita frequentemente de agregações específicas e payloads reduzidos adaptados à interface do usuário.  
* **Solução:** O BFF consome os streams gRPC internos e realiza o downgrade gracioso do protocolo para **WebSockets** ou **Server-Sent Events (SSE)** para exibição nos dashboards. Alternativamente, pode ser utilizado o **Envoy Proxy** atuando como tradutor **gRPC-Web** para o navegador.

---

# **Especificação do Contrato gRPC (Protocol Buffers)**

Abaixo está a definição do contrato `market_data.proto`, projetado para ser estrito, extensível e eficiente.

```
syntax = "proto3";

package marketdata.v1;

option go_package = "github.com/financial/marketdata/v1;marketdatav1";
option java_package = "com.financial.marketdata.v1";

// Serviço responsável pela distribuição de dados de mercado em tempo real.
service MarketDataService {
  // Estabelece um stream unilateral do servidor para o cliente enviando atualizações de ticker.
  rpc StreamTicker (TickerRequest) returns (stream TickerResponse);
}

// Solicitante do stream de dados de mercado.
message TickerRequest {
  // Símbolo do ativo financeiro (ex: "BTCUSDT", "ETHUSDT").
  string symbol = 1;

  // Parâmetro opcional para solicitar agregação de dados no lado do servidor (em ms).
  int32 aggregation_interval_ms = 2;
}

// Resposta contendo os dados de cada negócio (Trade) processado.
message TickerResponse {
  // Símbolo do ativo correspondente.
  string symbol = 1;

  // Preço da transação codificado como string para evitar perdas de precisão de ponto flutuante.
  string price = 2;

  // Quantidade negociada na transação.
  string quantity = 3;

  // Timestamp do evento gerado pela exchange (em milissegundos UTC).
  int64 event_time = 4;

  // Identificador único da transação provido pela Binance (Idempotência/Rastreabilidade).
  int64 trade_id = 5;

  // Identificador do comprador (Buyer Order ID) para fins de auditoria.
  int64 buyer_order_id = 6;

  // Identificador do vendedor (Seller Order ID) para fins de auditoria.
  int64 seller_order_id = 7;

  // Indica se o comprador foi o criador da ordem (Market Maker).
  bool is_buyer_market_maker = 8;
}
```

---

# **Requisitos Não Funcionais e Estratégias de Resiliência**

## **Estabilidade de Latência e Controle de Jitter**

1. **Processamento Assíncrono e Não-Bloqueante:** A ingestão de sockets externos roda em threads/goroutines isoladas do pipeline de processamento principal (*Asynchronous Background Fetch*), garantindo que operações de I/O em redes externas nunca bloqueiem a thread de execução crítica.  
2. **Estratégia de Cache para Enriquecimento:** Para evitar consultas síncronas a APIs REST da Binance durante a passagem do stream (ex: metadados do par de moedas), utilize um cache local em memória (LRU local) espelhado por uma instância **Redis** de ultrabaixa latência.  
3. **Resiliência em Chamadas Auxiliares Síncronas:**  
   * **Circuit Breakers:** Implementados via padrões como Resilience4j ou Hystrix/Go-breaker. Se a API REST de suporte falhar sucessivamente, o circuito abre e o sistema responde com dados degradados/fallback.  
   * **Timeouts Agressivos (Fail-Fast):** Timeouts de conexão definidos em patamares baixos (ex: 200ms) para evitar acúmulo de requisições pendentes em cenários de degradação da API externa.  
   * **Connection Pooling e TCP Keep-Alive:** Manutenção de pools de conexões HTTP/2 e gRPC previamente aquecidas para mitigar latências de handshake.  
4. **Reconexão Resiliente WebSocket:** Implementação de algoritmo de **Exponential Backoff com Jitter** para lidar com desconexões na Binance. Isso impede que múltiplos nós reconectem simultaneamente gerando ataques inadvertidos de negação de serviço (*Thundering Herd Problem*).

## **Garantia de Idempotência e Consistência**

O gRPC garante a entrega e a ordem das mensagens dentro de uma mesma conexão HTTP/2 ativa. No entanto, o protocolo gRPC não previne a duplicidade de mensagens de negócio em cenários de reconexão de rede, falhas parciais de nós ou *retries* automáticos acionados pelos clientes gRPC.

Para assegurar o processamento *exactly-once* do ponto de vista do negócio:

* **Uso do `trade_id` da Binance como Chave Natural:** Cada evento de transação emitido pela Binance possui um `trade_id` sequencial e único por símbolo.  
* **Deduplicação no Barramento de Eventos e Consumidores:**  
  * No pipeline de ingestão, o `trade_id` é utilizado como chave da mensagem no Kafka (`Kafka Message Key`), garantindo que todas as atualizações de um mesmo ativo sejam roteadas para a mesma partição, preservando a ordem cronológica.  
  * Os microsserviços consumidores utilizam uma janela deslizante em memória ou uma tabela no **Redis** (com tempo de expiração curto / TTL) armazenando as chaves `{symbol}:{trade_id}` já processadas. Caso um evento duplicado chegue via reconexão de stream, ele é descartado silenciosamente antes de impactar o estado do sistema.

---

# **Tabela Resumo de Tecnologias e Responsabilidades**

| Camada | Tecnologia Recomendada | Protocolo de Comunicação | Responsabilidade Principal |
| :---- | :---- | :---- | :---- |
| **1\. Aquisição Externa** | Go / Rust / Node.js Engine | WebSocket (`wss://`) | Manter conexões persistentes com a Binance, converter payloads JSON brutos em Protobuf e emitir métricas de latência externa. |
| **2\. Barramento de Eventos** | Apache Kafka / Redpanda | TCP (Kafka Protocol) | Prover buffer de alta velocidade, desacoplamento temporal, retenção de mensagens e distribuição fan-out resiliente. |
| **3\. Serviço Core de Mercado** | Go / C++ / Java | gRPC sobre HTTP/2 | Consumir eventos do barramento, aplicar regras de negócio, agregação de dados e redistribuir via Server-Side Streaming. |
| **4\. Camada de Borda (BFF)** | Node.js (NestJS) / Go | gRPC (Inbound) / WebSocket ou SSE (Outbound) | Realizar a tradução de protocolos internos (gRPC) para navegadores, autenticar clientes finais e gerenciar sessões do frontend. |
| **5\. Cache & Deduplicação** | Redis Cluster | RESP (Redis Serialization) | Manter tabelas de deduplicação (`trade_id`), gerenciar estado de Circuit Breakers e armazenar metadados de ativos. |

---

# **Roadmap Sugerido de Implementação (Fases)**

## **Fase 1: Especificação de Contratos e Engine de Aquisição**

* Definir e homologar os arquivos `.proto` do ecossistema.  
* Desenvolver o microsserviço de **Aquisição Externa** (Market Data Gateway).  
* Implementar a conexão WebSocket resiliente com a Binance contendo reconexão automática (Exponential Backoff \+ Jitter).  
* Construir a camada de parsing de JSON para objetos Protobuf binários.

## **Fase 2: Integração com Event Backbone e Políticas de Idempotência**

* Configurar o cluster **Apache Kafka** e provisionar tópicos particionados por ativo financeiro.  
* Conectar o Gateway de Aquisição ao Kafka emitindo mensagens tipadas em Protobuf.  
* Implementar os mecanismos de deduplicação e garantia de idempotência nos consumidores utilizando o `trade_id` e instâncias de cache Redis.  
* Validação de testes de carga e resiliência simulando quedas bruscas de conexão externa.

## **Fase 3: Camada gRPC Interna, BFF e Observabilidade**

* Desenvolver o serviço core gRPC implementando as interfaces de *Server-Side Streaming* (`StreamTicker`).  
* Construir a camada de **BFF (Backend For Frontend)** consumindo o gRPC interno e expondo endpoints **WebSocket/SSE** para as aplicações de front-end.  
* Configurar a infraestrutura de observabilidade:  
  * Coleta de métricas de latência **p95, p99 e p99.9** via Prometheus.  
  * Exibição de painéis no Grafana monitorando *throughput* de mensagens, taxa de descarte por duplicidade e estado dos Circuit Breakers.  
  * Rastreamento distribuído (*Distributed Tracing*) através do OpenTelemetry nos pontos de salto de protocolo.

