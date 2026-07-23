package io.github.pruthvidhani.idempotencyoutbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pruthvidhani.idempotencyoutbox.outbox.publisher.kafka.KafkaEventPublisher;
import io.github.pruthvidhani.idempotencyoutbox.outbox.store.jdbc.JdbcOutboxStore;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.AbstractPostgresKafkaIT;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.MutableClock;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** Full outbox pipeline: record in TX → poll → real Kafka → consumed, plus retry/dead paths. */
class OutboxPollerIT extends AbstractPostgresKafkaIT {

  private static JdbcTemplate jdbc;
  private static JdbcOutboxStore store;
  private static TransactionTemplate tx;
  private static MutableClock clock;
  private static OutboxPublisher outboxPublisher;
  private static KafkaEventPublisher kafkaPublisher;

  /** Wraps the real Kafka publisher to inject failures for retry/dead-letter tests. */
  static class FlakyPublisher implements EventPublisher {
    final AtomicInteger failuresRemaining = new AtomicInteger();

    @Override
    public void publish(String topic, String key, String payload, Map<String, String> headers)
        throws Exception {
      if (failuresRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        throw new RuntimeException("injected failure");
      }
      kafkaPublisher.publish(topic, key, payload, headers);
    }
  }

  private static final FlakyPublisher flaky = new FlakyPublisher();

  private OutboxPoller newPoller(int maxAttempts) {
    return new OutboxPoller(
        store,
        flaky,
        TopicResolver.byAggregateType(),
        tx,
        clock,
        new Backoff(Duration.ofMillis(200), Duration.ofSeconds(30)),
        100,
        maxAttempts);
  }

  @BeforeAll
  static void setUp() {
    var dataSource = new DriverManagerDataSource(jdbcUrl(), username(), password());
    Flyway.configure().dataSource(dataSource).load().migrate();
    jdbc = new JdbcTemplate(dataSource);
    store = new JdbcOutboxStore(jdbc);
    tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    clock = new MutableClock(Instant.parse("2026-01-01T10:00:00Z"));
    outboxPublisher = new OutboxPublisher(store, clock);
    var producerFactory =
        new DefaultKafkaProducerFactory<String, String>(
            Map.of(
                "bootstrap.servers", kafkaBootstrapServers(),
                "key.serializer", org.apache.kafka.common.serialization.StringSerializer.class,
                "value.serializer", org.apache.kafka.common.serialization.StringSerializer.class));
    kafkaPublisher =
        new KafkaEventPublisher(new KafkaTemplate<>(producerFactory), Duration.ofSeconds(15));
  }

  @BeforeEach
  void resetState() {
    jdbc.update("truncate table outbox_events");
    clock.setTo(Instant.parse("2026-01-01T10:00:00Z"));
    flaky.failuresRemaining.set(0);
  }

  record OrderPlaced(String orderId, int seq) {}

  private List<ConsumerRecord<String, String>> consume(String topic, int expected) {
    try (var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "poller-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName()))) {
      consumer.subscribe(List.of(topic));
      List<ConsumerRecord<String, String>> records = new ArrayList<>();
      long deadline = System.currentTimeMillis() + 20_000;
      while (records.size() < expected && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(250)).forEach(records::add);
      }
      return records;
    }
  }

  @Test
  void recordedEventsReachKafkaInPerAggregateOrderWithEventIdHeaders() {
    // distinct aggregate type per test: topics live in the shared Kafka container across tests
    UUID e1 =
        tx.execute(s -> outboxPublisher.record("e2eorder", "order-1", "OrderPlaced", new OrderPlaced("order-1", 0)));
    tx.execute(s -> outboxPublisher.record("e2eorder", "order-1", "OrderPlaced", new OrderPlaced("order-1", 1)));
    tx.execute(s -> outboxPublisher.record("e2eorder", "order-2", "OrderPlaced", new OrderPlaced("order-2", 0)));

    assertThat(store.countPending()).as("nothing published before the poller runs").isEqualTo(3);
    int published = newPoller(8).pollOnce();

    assertThat(published).isEqualTo(3);
    assertThat(store.countPending()).isZero();

    List<ConsumerRecord<String, String>> records = consume("e2eorder.events", 3);
    assertThat(records).hasSize(3);
    // jsonb round-trip normalizes whitespace — compare with spaces stripped
    List<String> order1Payloads =
        records.stream()
            .filter(r -> r.key().equals("order-1"))
            .map(r -> r.value().replace(" ", ""))
            .toList();
    assertThat(order1Payloads.get(0)).contains("\"seq\":0");
    assertThat(order1Payloads.get(1)).contains("\"seq\":1");
    ConsumerRecord<String, String> first =
        records.stream().filter(r -> r.key().equals("order-1")).findFirst().orElseThrow();
    assertThat(
            new String(
                first.headers().lastHeader(OutboxPublisher.EVENT_ID_HEADER).value(),
                StandardCharsets.UTF_8))
        .isEqualTo(e1.toString());
  }

  @Test
  void transientFailureRetriesWithBackoffThenPublishes() {
    tx.execute(s -> outboxPublisher.record("retryorder", "order-1", "OrderPlaced", new OrderPlaced("order-1", 0)));
    flaky.failuresRemaining.set(1);
    OutboxPoller poller = newPoller(8);

    assertThat(poller.pollOnce()).as("first pass fails").isZero();
    assertThat(store.countPending()).isEqualTo(1);

    assertThat(poller.pollOnce()).as("not due yet — backoff").isZero();

    clock.advanceBy(Duration.ofMillis(250)); // past the 200ms base delay
    assertThat(poller.pollOnce()).as("retry succeeds").isEqualTo(1);
    assertThat(store.countPending()).isZero();
    assertThat(consume("retryorder.events", 1)).hasSize(1);
  }

  @Test
  void permanentFailureEndsDeadAfterMaxAttempts() {
    tx.execute(s -> outboxPublisher.record("deadorder", "order-1", "OrderPlaced", new OrderPlaced("order-1", 0)));
    flaky.failuresRemaining.set(Integer.MAX_VALUE);
    OutboxPoller poller = newPoller(2);

    poller.pollOnce(); // attempt 1 → rescheduled
    clock.advanceBy(Duration.ofSeconds(1));
    poller.pollOnce(); // attempt 2 = max → DEAD

    assertThat(store.countPending()).isZero();
    Integer dead =
        jdbc.queryForObject("select count(*) from outbox_events where status = 'DEAD'", Integer.class);
    assertThat(dead).isEqualTo(1);
  }
}
