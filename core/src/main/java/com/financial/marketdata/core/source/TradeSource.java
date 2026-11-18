package com.financial.marketdata.core.source;

import com.financial.marketdata.v1.TickerResponse;

import java.util.function.Consumer;

/**
 * Fontes de trades normalizados para o serviço core.
 *
 * <p>Na Fase 1 o core roda contra {@link SimulatedTradeSource} (dados
 * sintéticos para desenvolvimento e teste do contrato gRPC). Na Fase 2 uma
 * implementação {@code KafkaTradeSource} consome os tópicos por ativo,
 * deduplicando via {@code trade_id} + Redis.
 */
public interface TradeSource extends AutoCloseable {

    /** Inicia a fonte (idempotente). */
    void start();

    /**
     * Inscreve um listener para trades do símbolo informado.
     *
     * @return handle para cancelar a inscrição
     */
    Subscription subscribe(String symbol, Consumer<TickerResponse> listener);

    @Override
    void close();

    interface Subscription extends AutoCloseable {
        @Override
        void close();
    }
}
