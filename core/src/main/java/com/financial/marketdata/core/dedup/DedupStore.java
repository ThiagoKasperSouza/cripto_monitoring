package com.financial.marketdata.core.dedup;

/**
 * Armazena as chaves {@code {symbol}:{trade_id}} já processadas para garantir
 * idempotência (exactly-once do ponto de vista do negócio) nos consumidores.
 *
 * Implementações:
 *   - {@link InMemoryDedupStore}: janela deslizante em memória (desenvolvimento).
 *   - {@link RedisDedupStore}: Redis com TTL (produção, compartilhado entre nós).
 */
public interface DedupStore extends AutoCloseable {

    /**
     * Registra a chave como processada.
     *
     * @return {@code true} se a chave foi inserida agora;
     *         {@code false} se ela já existia (evento duplicado).
     */
    boolean markProcessed(String key);

    @Override
    void close();
}
