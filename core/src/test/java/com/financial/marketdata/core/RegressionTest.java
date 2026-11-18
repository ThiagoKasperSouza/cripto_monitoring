package com.financial.marketdata.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for bugs found during Fase 1 implementation (2026-11-18).
 *
 * <p>Bug: the proto originally lacked {@code java_multiple_files = true}, so
 * protoc generated a single outer class {@code MarketData} with all messages
 * nested inside ({@code MarketData.TickerResponse}). Every import of
 * {@code com.financial.marketdata.v1.TickerResponse} then failed with
 * "cannot find symbol" and the whole core did not compile. The option was
 * added to {@code proto/market_data.proto} and the Go side was regenerated
 * from the same file (no Go impact, same package).
 *
 * <p>These tests fail loudly if someone removes the option or regenerates
 * with different settings: they assert the messages and the service stub are
 * top-level classes, and that the outer descriptor class `MarketData` (always
 * emitted by protoc) carries no nested message classes.
 */
class RegressionTest {

    @Test
    void messagesAndServiceAreTopLevelClasses() throws ClassNotFoundException {
        for (String className : new String[]{
                "com.financial.marketdata.v1.TickerRequest",
                "com.financial.marketdata.v1.TickerResponse",
                "com.financial.marketdata.v1.MarketDataServiceGrpc",
        }) {
            Class<?> clazz = Class.forName(className);
            assertEquals("com.financial.marketdata.v1", clazz.getPackageName(),
                    className + " must live in com.financial.marketdata.v1");
            assertFalse(clazz.isMemberClass(),
                    className + " must be a top-level class, not nested (java_multiple_files regression)");
        }
    }

    /**
     * The outer descriptor class `MarketData` always exists (protobuf needs
     * it for descriptor registration) — with java_multiple_files=true it must
     * carry NO nested message classes. Before the fix, ALL messages were
     * nested here (MarketData.TickerResponse), which broke every direct
     * import of com.financial.marketdata.v1.TickerResponse.
     */
    @Test
    void outerClassCarriesNoNestedMessages() throws ClassNotFoundException {
        Class<?> outer = Class.forName("com.financial.marketdata.v1.MarketData");
        for (Class<?> nested : outer.getDeclaredClasses()) {
            fail("MarketData must carry no nested classes with java_multiple_files=true, found: " + nested.getName());
        }
    }

    @Test
    void builderIsNestedStaticClassOfMessage() throws ClassNotFoundException {
        Class<?> tickerResponse = Class.forName("com.financial.marketdata.v1.TickerResponse");
        Class<?> builder = Class.forName("com.financial.marketdata.v1.TickerResponse$Builder");
        assertEquals(tickerResponse, builder.getDeclaringClass(),
                "Builder must be nested inside TickerResponse");
        assertTrue(java.lang.reflect.Modifier.isStatic(builder.getModifiers()),
                "Builder must be static");
    }
}
