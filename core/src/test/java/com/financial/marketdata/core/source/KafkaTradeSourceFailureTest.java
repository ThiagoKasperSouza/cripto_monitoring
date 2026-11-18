package com.financial.marketdata.core.source;

import com.financial.marketdata.core.dedup.InMemoryDedupStore;
import com.financial.marketdata.v1.TickerResponse;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes de falha do consumidor Kafka sem broker: o poll loop é exercitado
 * contra um FakeConsumer com resultados pré-programados (batches/exceções),
 * cobrindo recuperação de falhas transitórias e rebalanceamentos.
 */
class KafkaTradeSourceFailureTest {

    private static final String TOPIC = "market.trades";

    /** FakeConsumer determinístico: executa exatamente o que foi programado. */
    static final class FakeConsumer implements Consumer<String, byte[]> {
        final Queue<Exception> failures = new ArrayDeque<>();
        final Queue<List<ConsumerRecord<String, byte[]>>> batches = new ArrayDeque<>();
        final AtomicBoolean wakeupRequested = new AtomicBoolean();
        volatile ConsumerRebalanceListener rebalanceListener;
        volatile Set<TopicPartition> assigned = new HashSet<>();
        volatile boolean closed = false;
        final Set<TopicPartition> paused = ConcurrentHashMap.newKeySet();
        final AtomicLong pollCount = new AtomicLong();

        void addFailure(Exception e) { failures.add(e); }
        void addBatch(ConsumerRecord<String, byte[]>... records) { batches.add(Arrays.asList(records)); }

        void fakeRebalance(Set<TopicPartition> newAssignment) {
            ConsumerRebalanceListener listener = rebalanceListener;
            if (listener == null) return;
            listener.onPartitionsRevoked(new ArrayList<>(assigned));
            assigned = new HashSet<>(newAssignment);
            listener.onPartitionsAssigned(newAssignment);
        }

        @Override public Set<TopicPartition> assignment() { return new HashSet<>(assigned); }
        @Override public Set<String> subscription() { return Set.of(TOPIC); }
        @Override public void subscribe(Collection<String> topics) {}
        @Override public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            this.rebalanceListener = listener;
        }
        @Override public void assign(Collection<TopicPartition> partitions) { assigned = new HashSet<>(partitions); }
        @Override public void subscribe(java.util.regex.Pattern pattern, ConsumerRebalanceListener listener) {
            this.rebalanceListener = listener;
        }
        @Override public void subscribe(java.util.regex.Pattern pattern) {}
        @Override public void unsubscribe() { if (rebalanceListener != null) rebalanceListener.onPartitionsRevoked(assigned); }

        @Override public ConsumerRecords<String, byte[]> poll(long timeoutMs) { return poll(Duration.ofMillis(timeoutMs)); }

        @Override public ConsumerRecords<String, byte[]> poll(Duration timeout) {
            pollCount.incrementAndGet();
            if (closed) {
                throw new WakeupException();
            }
            if (wakeupRequested.getAndSet(false)) {
                throw new WakeupException();
            }
            Exception failure = failures.poll();
            if (failure != null) {
                throw new KafkaException(failure);
            }
            List<ConsumerRecord<String, byte[]>> batch = batches.poll();
            if (batch == null || batch.isEmpty()) {
                return ConsumerRecords.empty();
            }
            Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> byPartition = new HashMap<>();
            for (ConsumerRecord<String, byte[]> record : batch) {
                byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), k -> new ArrayList<>())
                        .add(record);
            }
            return new ConsumerRecords<>(byPartition);
        }

        @Override public void wakeup() { wakeupRequested.set(true); }
        @Override public void close() { closed = true; }
        @Override public void close(Duration timeout) { closed = true; }

        @Override public void pause(Collection<TopicPartition> partitions) { paused.addAll(partitions); }
        @Override public void resume(Collection<TopicPartition> partitions) { partitions.forEach(paused::remove); }
        @Override public Set<TopicPartition> paused() { return new HashSet<>(paused); }

        // Defaults úteis para o restante da interface
        @Override public void commitSync() {}
        @Override public void commitSync(Duration timeout) {}
        @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {}
        @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {}
        @Override public void commitAsync() {}
        @Override public void commitAsync(OffsetCommitCallback callback) {}
        @Override public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {}
        @Override public void seek(TopicPartition partition, long offset) {}
        @Override public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {}
        @Override public void seekToBeginning(Collection<TopicPartition> partitions) {}
        @Override public void seekToEnd(Collection<TopicPartition> partitions) {}
        @Override public long position(TopicPartition partition) { return 0L; }
        @Override public long position(TopicPartition partition, Duration duration) { return 0L; }
        @Override public OffsetAndMetadata committed(TopicPartition partition) { return null; }
        @Override public OffsetAndMetadata committed(TopicPartition partition, Duration duration) { return null; }
        @Override public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) { return Map.of(); }
        @Override public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration duration) { return Map.of(); }
        @Override public Uuid clientInstanceId(Duration duration) { return Uuid.randomUuid(); }
        @Override public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }
        @Override public List<PartitionInfo> partitionsFor(String topic) { return List.of(); }
        @Override public List<PartitionInfo> partitionsFor(String topic, Duration duration) { return List.of(); }
        @Override public Map<String, List<PartitionInfo>> listTopics() { return Map.of(); }
        @Override public Map<String, List<PartitionInfo>> listTopics(Duration duration) { return Map.of(); }
        @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) { return Map.of(); }
        @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration duration) { return Map.of(); }
        @Override public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) { return Map.of(); }
        @Override public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration duration) { return Map.of(); }
        @Override public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) { return Map.of(); }
        @Override public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration duration) { return Map.of(); }
        @Override public OptionalLong currentLag(TopicPartition partition) { return OptionalLong.empty(); }
        @Override public ConsumerGroupMetadata groupMetadata() { return new ConsumerGroupMetadata("test-group"); }
        @Override public void enforceRebalance() {}
        @Override public void enforceRebalance(String reason) {}
    }

    private InMemoryDedupStore dedup;
    private FakeConsumer fakeConsumer;
    private KafkaTradeSource source;
    private final List<TickerResponse> received = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        dedup = new InMemoryDedupStore(600_000, 1_000);
        fakeConsumer = new FakeConsumer();
        source = new KafkaTradeSource("localhost:9092", TOPIC, "test-group", dedup, () -> fakeConsumer, 1);
        source.subscribe("BTCUSDT", received::add);
    }

    @AfterEach
    void tearDown() {
        source.close();
    }

    private static ConsumerRecord<String, byte[]> record(long tradeId) {
        TickerResponse trade = TickerResponse.newBuilder()
                .setSymbol("BTCUSDT").setPrice("100.0").setQuantity("1")
                .setEventTime(1730000000000L + tradeId).setTradeId(tradeId).build();
        return new ConsumerRecord<>(TOPIC, 0, tradeId, "BTCUSDT", trade.toByteArray());
    }

    private static void await(BooleanSupplier condition, long timeoutMs, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(10);
        }
        fail("timed out waiting for: " + what);
    }

    @Test
    void recoversFromTransientPollFailures() throws Exception {
        fakeConsumer.addFailure(new TimeoutException("broker not responding"));
        fakeConsumer.addFailure(new KafkaException("NotLeaderOrFollower")); // classe de falha transitória típica
        fakeConsumer.addBatch(record(1), record(2));

        source.start();
        await(() -> received.size() == 2, 3_000, "2 records after transient failures");

        assertEquals(List.of(1L, 2L), received.stream().map(TickerResponse::getTradeId).toList());
        assertTrue(fakeConsumer.pollCount.get() >= 3, "poll must be retried after failure");
    }

    @Test
    void rebalanceKeepsConsumerProcessing() throws Exception {
        source.start();

        fakeConsumer.fakeRebalance(Set.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1)));
        await(() -> source.rebalanceCount() == 2, 2_000, "revoke+assign");
        assertEquals(2, source.assignedPartitions().size());

        fakeConsumer.addBatch(record(10));
        await(() -> received.size() == 1, 2_000, "record after first rebalance");

        // segundo rebalance (partição 1 perdida)
        fakeConsumer.fakeRebalance(Set.of(new TopicPartition(TOPIC, 1)));
        await(() -> source.rebalanceCount() == 4, 2_000, "second rebalance");

        fakeConsumer.addBatch(record(11));
        await(() -> received.size() == 2, 2_000, "record after second rebalance");

        assertEquals(List.of(10L, 11L), received.stream().map(TickerResponse::getTradeId).toList());
        assertEquals(1, source.assignedPartitions().size());
    }

    @Test
    void closeUnblocksPollLoopQuickly() throws Exception {
        source.start();
        long start = System.currentTimeMillis();
        source.close();
        assertTrue(System.currentTimeMillis() - start < 2_500, "close must not hang the poll loop");
    }

    @Test
    void deduplicationIsSharedAcrossConsumerInstancesSharingTheStore() throws Exception {
        // Simula dois consumers simultâneos (ex: rebalance com overlapping membership)
        // compartilhando a mesma loja de dedup: o trade entregue duas vezes é uma vez só.
        FakeConsumer consumer2 = new FakeConsumer();
        KafkaTradeSource source2 = new KafkaTradeSource("localhost:9092", TOPIC, "test-group",
                dedup, () -> consumer2, 1);
        source2.subscribe("BTCUSDT", received::add);

        source.start();
        source2.start();

        fakeConsumer.addBatch(record(42));
        consumer2.addBatch(record(42)); // mesma mensagem entregue aos dois

        // Corrida: quem chegar primeiro no store despacha; o outro descarta como dup.
        await(() -> (source.dispatched() + source2.dispatched()) == 1
                        && (source.duplicatesDropped() + source2.duplicatesDropped()) == 1,
                2_000, "one dispatch and one duplicate across both consumers");

        assertEquals(1, received.size(), "identical trade must be dispatched exactly once across consumers");
        source2.close();
    }
}
