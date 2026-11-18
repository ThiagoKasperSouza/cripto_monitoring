package com.financial.marketdata.core;

import com.financial.marketdata.v1.TickerResponse;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Agregador de trades por janela temporal ({@code aggregation_interval_ms}).
 *
 * <p>Ao fechar cada janela, emite um {@link TickerResponse} sintético:
 * <ul>
 *   <li>{@code price} = VWAP da janela (Σpreço·qtd / Σqtd), 8 casas decimais;</li>
 *   <li>{@code quantity} = volume total negociado na janela;</li>
 *   <li>{@code event_time} = fim da janela (ms UTC);</li>
 *   <li>{@code trade_id} = id do último trade da janela (rastreabilidade);</li>
 *   <li>{@code buyer_order_id}/{@code seller_order_id} = 0 (sem significado agregado);</li>
 *   <li>{@code is_buyer_market_maker} = false.</li>
 * </ul>
 *
 * <p>Uma janela sem trades não emite nada.
 */
final class TradeAggregator implements AutoCloseable {

    private final String symbol;
    private final int intervalMs;
    private final Consumer<TickerResponse> downstream;
    private final ScheduledExecutorService scheduler;

    private BigDecimal notional = BigDecimal.ZERO;
    private BigDecimal volume = BigDecimal.ZERO;
    private long lastTradeId = 0;
    private boolean hasTrades = false;

    TradeAggregator(String symbol, int intervalMs, Consumer<TickerResponse> downstream) {
        this.symbol = symbol;
        this.intervalMs = intervalMs;
        this.downstream = downstream;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "trade-aggregator-" + symbol);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::flushSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** Thread-safe: pode ser chamado a partir de múltiplas threads. */
    void accept(TickerResponse trade) {
        synchronized (this) {
            BigDecimal price = new BigDecimal(trade.getPrice());
            BigDecimal qty = new BigDecimal(trade.getQuantity());
            notional = notional.add(price.multiply(qty));
            volume = volume.add(qty);
            lastTradeId = trade.getTradeId();
            hasTrades = true;
        }
    }

    private void flushSafely() {
        try {
            flush();
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }
    }

    private void flush() {
        TickerResponse snapshot;
        synchronized (this) {
            if (!hasTrades || volume.signum() == 0) return;
            BigDecimal vwap = notional.divide(volume, 8, RoundingMode.HALF_UP);
            snapshot = TickerResponse.newBuilder()
                    .setSymbol(symbol)
                    .setPrice(vwap.toPlainString())
                    .setQuantity(volume.setScale(8, RoundingMode.HALF_UP).toPlainString())
                    .setEventTime(Instant.now().toEpochMilli())
                    .setTradeId(lastTradeId)
                    .setBuyerOrderId(0)
                    .setSellerOrderId(0)
                    .setIsBuyerMarketMaker(false)
                    .build();
            notional = BigDecimal.ZERO;
            volume = BigDecimal.ZERO;
            hasTrades = false;
        }
        downstream.accept(snapshot);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
