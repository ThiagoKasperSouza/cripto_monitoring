package com.financial.marketdata.core;

import com.financial.marketdata.v1.TickerResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TradeAggregatorTest {

    private TradeAggregator aggregator;
    private final List<TickerResponse> buckets = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        if (aggregator != null) {
            aggregator.close();
        }
    }

    private static TickerResponse trade(String price, String qty, long tradeId) {
        return TickerResponse.newBuilder()
                .setSymbol("BTCUSDT")
                .setPrice(price)
                .setQuantity(qty)
                .setEventTime(System.currentTimeMillis())
                .setTradeId(tradeId)
                .build();
    }

    @Test
    void emitsVwapBucket() throws Exception {
        aggregator = new TradeAggregator("BTCUSDT", 80, buckets::add);
        // VWAP esperado = (100*1 + 200*3) / 4 = 175
        aggregator.accept(trade("100", "1", 1));
        aggregator.accept(trade("200", "3", 2));

        TimeUnit.MILLISECONDS.sleep(600); // espera o flush da janela de 80ms

        assertEquals(1, buckets.size(), "expected exactly one VWAP bucket");
        TickerResponse bucket = buckets.get(0);
        assertEquals("BTCUSDT", bucket.getSymbol());
        assertEquals(0, new BigDecimal(bucket.getPrice()).compareTo(new BigDecimal("175.00000000")),
                "VWAP must be 175, got " + bucket.getPrice());
        assertEquals(0, new BigDecimal(bucket.getQuantity()).compareTo(new BigDecimal("4.00")),
                "volume must be 4, got " + bucket.getQuantity());
        assertEquals(2, bucket.getTradeId(), "bucket trade_id must be the last trade's id");
        assertEquals(0, bucket.getBuyerOrderId());
        assertEquals(0, bucket.getSellerOrderId());
    }

    @Test
    void emptyWindowEmitsNothing() throws Exception {
        aggregator = new TradeAggregator("BTCUSDT", 80, buckets::add);
        TimeUnit.MILLISECONDS.sleep(350); // ~4 janelas vazias
        assertTrue(buckets.isEmpty(), "windows without trades must not emit");
    }

    @Test
    void distinctWindowsEmitSeparateBuckets() throws Exception {
        aggregator = new TradeAggregator("BTCUSDT", 100, buckets::add);
        aggregator.accept(trade("100", "1", 1));
        TimeUnit.MILLISECONDS.sleep(250); // janela 1 fecha
        aggregator.accept(trade("300", "1", 2));
        TimeUnit.MILLISECONDS.sleep(300); // janela 2 fecha

        assertTrue(buckets.size() >= 2, "expected at least 2 buckets, got " + buckets.size());
        assertEquals(0, new BigDecimal(buckets.get(0).getPrice()).compareTo(new BigDecimal("100.00000000")));
        assertEquals(0, new BigDecimal(buckets.get(1).getPrice()).compareTo(new BigDecimal("300.00000000")));
    }
}
