package com.financial.marketdata.core.source;

import com.financial.marketdata.v1.TickerResponse;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Fonte sintética de trades para desenvolvimento local e testes do contrato
 * gRPC enquanto o Kafka (Fase 2) não está disponível.
 *
 * <p>Cada símbolo subscrito recebe um trade a cada {@code emitIntervalMs}
 * milissegundos, com random walk em torno de um preço-base determinístico
 * (derivado do hash do símbolo). IDs de trade são sequenciais por símbolo.
 *
 * <p><b>NÃO usar em produção</b> — é um stand-in para o {@code KafkaTradeSource}.
 */
public final class SimulatedTradeSource implements TradeSource {

    private final long emitIntervalMs;
    private final Random random;
    private final ScheduledExecutorService executor;
    private final Map<String, Double> lastPrice = new ConcurrentHashMap<>();
    private final Map<String, Long> tradeId = new ConcurrentHashMap<>();
    private final Map<String, List<Consumer<TickerResponse>>> listeners = new ConcurrentHashMap<>();
    private volatile boolean started = false;

    public SimulatedTradeSource() {
        this(250, new Random());
    }

    public SimulatedTradeSource(long emitIntervalMs, Random random) {
        this.emitIntervalMs = emitIntervalMs;
        this.random = random;
        // Single thread: all listener callbacks are globally serialized,
        // which keeps gRPC StreamObserver.onNext calls serialized per contract.
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "simulated-trade-source");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        synchronized (this) {
            if (started) return;
            started = true;
            executor.scheduleAtFixedRate(this::emitTick, emitIntervalMs, emitIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private void emitTick() {
        try {
            for (Map.Entry<String, List<Consumer<TickerResponse>>> entry : listeners.entrySet()) {
                String symbol = entry.getKey();
                List<Consumer<TickerResponse>> registered = entry.getValue();
                if (registered.isEmpty()) continue;
                TickerResponse trade = nextTrade(symbol);
                for (Consumer<TickerResponse> listener : new ArrayList<>(registered)) {
                    try {
                        listener.accept(trade);
                    } catch (Throwable t) {
                        // Listener failure must not kill the source loop.
                        t.printStackTrace(System.err);
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }
    }

    private TickerResponse nextTrade(String symbol) {
        double base = basePrice(symbol);
        double prev = lastPrice.getOrDefault(symbol, base);
        double drift = (random.nextDouble() - 0.5) * 0.002; // ±0.1% per trade
        double price = Math.max(0.00000001, prev * (1 + drift));
        lastPrice.put(symbol, price);

        long id = tradeId.merge(symbol, 1L, Long::sum);
        double quantity = 0.001 + random.nextDouble() * 0.5;

        return TickerResponse.newBuilder()
                .setSymbol(symbol)
                .setPrice(String.format(java.util.Locale.US, "%.8f", price))
                .setQuantity(String.format(java.util.Locale.US, "%.8f", quantity))
                .setEventTime(Instant.now().toEpochMilli())
                .setTradeId(id)
                .setBuyerOrderId(0)   // spot não expõe IDs de ordem; ver doc
                .setSellerOrderId(0)
                .setIsBuyerMarketMaker(random.nextBoolean())
                .build();
    }

    private double basePrice(String symbol) {
        // Deterministic per symbol: 20_000 + (hash % 80_000).
        int h = Math.abs(symbol.hashCode());
        return 20_000d + (h % 80_000);
    }

    @Override
    public Subscription subscribe(String symbol, Consumer<TickerResponse> listener) {
        listeners.computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>()).add(listener);
        return () -> {
            List<Consumer<TickerResponse>> registered = listeners.get(symbol);
            if (registered != null) {
                registered.remove(listener);
            }
        };
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
