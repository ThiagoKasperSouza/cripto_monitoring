package com.financial.marketdata.core.source;

import com.financial.marketdata.core.dedup.InMemoryDedupStore;
import com.financial.marketdata.v1.TickerResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testa handleRecord (a lógica de desserialização + dedup) sem precisar de
 * broker — o consumer só é criado em start(), que os testes não chamam.
 */
class KafkaTradeSourceTest {

    private InMemoryDedupStore dedup;
    private KafkaTradeSource source;
    private final List<TickerResponse> received = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        dedup = new InMemoryDedupStore(60_000, 1_000);
        source = new KafkaTradeSource("localhost:9092", "market.trades", "test-group", dedup);
        source.subscribe("BTCUSDT", received::add);
    }

    @AfterEach
    void tearDown() {
        source.close();
    }

    private static TickerResponse trade(long tradeId) {
        return TickerResponse.newBuilder()
                .setSymbol("BTCUSDT")
                .setPrice("97234.56")
                .setQuantity("0.5")
                .setEventTime(1730000000000L)
                .setTradeId(tradeId)
                .build();
    }

    @Test
    void validRecordIsDispatchedToListener() {
        assertTrue(source.handleRecord("BTCUSDT", trade(1).toByteArray()));
        assertEquals(1, received.size());
        assertEquals(1, received.get(0).getTradeId());
    }

    @Test
    void duplicateRecordIsSilentlyDropped() {
        assertTrue(source.handleRecord("BTCUSDT", trade(7).toByteArray()));
        assertFalse(source.handleRecord("BTCUSDT", trade(7).toByteArray()));
        assertEquals(1, received.size(), "listener must not see the duplicate");
        assertEquals(1, source.duplicatesDropped());
    }

    @Test
    void sameTradeIdDifferentSymbolIsNotADuplicate() {
        List<TickerResponse> ethReceived = new CopyOnWriteArrayList<>();
        source.subscribe("ETHUSDT", ethReceived::add);

        assertTrue(source.handleRecord("BTCUSDT", trade(9).toByteArray()));

        TickerResponse ethTrade = TickerResponse.newBuilder()
                .setSymbol("ETHUSDT").setPrice("3200.1").setQuantity("1")
                .setEventTime(1730000000001L).setTradeId(9).build();
        assertTrue(source.handleRecord("ETHUSDT", ethTrade.toByteArray()));
        assertEquals(1, ethReceived.size());
    }

    @Test
    void unparseableValueIsDroppedWithoutThrowing() {
        // 0x01 é um tag com field=0 (inválido) — parseFrom lança InvalidProtocolBufferException
        assertFalse(source.handleRecord("BTCUSDT", new byte[]{0x01}));
        assertEquals(0, received.size());
        assertEquals(1, source.parseErrors());
    }

    @Test
    void nullKeyFallsBackToPayloadSymbol() {
        assertTrue(source.handleRecord(null, trade(3).toByteArray()));
        assertEquals(1, received.size(), "dedup and dispatch must work off the payload symbol");
    }

    @Test
    void listenerForOtherSymbolDoesNotReceiveRecord() {
        List<TickerResponse> ethReceived = new CopyOnWriteArrayList<>();
        source.subscribe("ETHUSDT", ethReceived::add);
        assertTrue(source.handleRecord("BTCUSDT", trade(11).toByteArray()));
        assertTrue(ethReceived.isEmpty());
        assertEquals(1, received.size());
    }
}
