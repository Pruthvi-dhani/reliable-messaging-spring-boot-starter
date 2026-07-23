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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

/**
 * The headline no-event-loss test. Records a known set of events, injects failures mid-flight, and
 * proves the two guarantees the outbox exists to give:
 *
 * <ul>
 *   <li><b>No loss</b> — a broker outage before the send just retries; every recorded event
 *       eventually lands on Kafka (at-least-once).
 *   <li><b>Exactly-once effect</b> — a crash <i>after</i> the broker ack but before the DB records
 *       success replays the event (a genuine Kafka duplicate), but the stable {@code
 *       outbox-event-id} header lets a consumer collapse duplicates back to exactly one.
 * </ul>
 */
class OutboxChaosIT extends AbstractPostgresKafkaIT {

  private static final int EVENT_COUNT = 8;
  private static final Instant START = Instant.parse("2026-01-01T10:00:00Z");

  private static JdbcTemplate jdbc;
  private static JdbcOutboxStore store;
  private static TransactionTemplate tx;
  private static MutableClock clock;
  private static OutboxPublisher outboxPublisher;
  private static KafkaEventPublisher kafkaPublisher;

  /** Records the payload and drops the broker for the first {@code failFirst} publish attempts. */
  static class BrokerOutagePublisher implements EventPublisher {
    private int failuresRemaining;

    void dropBrokerFor(int attempts) {
      this.failuresRemaining = attempts;
    }

    @Override
    public void publish(String topic, String key, String payload, Map<String, String> headers)
        throws Exception {
      if (failuresRemaining > 0) {
        failuresRemaining--;
        throw new RuntimeException("broker unavailable");
      }
      kafkaPublisher.publish(topic, key, payload, headers);
    }
  }

  /**
   * Sends to Kafka for real, then — for events whose id is marked to crash, once each — throws
   * <i>after</i> the broker already has the message. Models a poller crash in the window between
   * the broker ack and the DB commit: the event is re-polled and re-sent, producing a duplicate.
   */
  static class CrashAfterAckPublisher implements EventPublisher {
    final Set<String> crashOnce = ConcurrentHashMap.newKeySet();
    private final Set<String> alreadyCrashed = ConcurrentHashMap.newKeySet();

    @Override
    public void publish(String topic, String key, String payload, Map<String, String> headers)
        throws Exception {
      kafkaPublisher.publish(topic, key, payload, headers); // broker durably has it now
      String eventId = headers.get(OutboxPublisher.EVENT_ID_HEADER);
      if (crashOnce.contains(eventId) && alreadyCrashed.add(eventId)) {
        throw new RuntimeException("crash after ack, before DB commit for " + eventId);
      }
    }
  }

  private OutboxPoller newPoller(EventPublisher publisher) {
    return new OutboxPoller(
        store,
        publisher,
        TopicResolver.byAggregateType(),
        tx,
        clock,
        new Backoff(Duration.ofMillis(200), Duration.ofSeconds(30)),
        100,
        8,
        OutboxMetrics.NOOP);
  }

  @BeforeAll
  static void setUp() {
    var dataSource = new DriverManagerDataSource(jdbcUrl(), username(), password());
    Flyway.configure().dataSource(dataSource).load().migrate();
    jdbc = new JdbcTemplate(dataSource);
    store = new JdbcOutboxStore(jdbc);
    tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    clock = new MutableClock(START);
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
    clock.setTo(START);
  }

  record OrderPlaced(String orderId) {}

  /** Records EVENT_COUNT events under one aggregate type (one topic), distinct aggregate ids. */
  private List<UUID> recordEvents(String aggregateType) {
    return IntStream.range(0, EVENT_COUNT)
        .mapToObj(
            i ->
                tx.execute(
                    s ->
                        outboxPublisher.record(
                            aggregateType,
                            "order-" + i,
                            "OrderPlaced",
                            new OrderPlaced("order-" + i))))
        .toList();
  }

  /** Drains the outbox until nothing is pending, advancing the clock to clear any backoff. */
  private void drainUntilEmpty(OutboxPoller poller) {
    for (int pass = 0; pass < 50 && store.countPending() > 0; pass++) {
      poller.pollOnce();
      clock.advanceBy(Duration.ofSeconds(1)); // step past exponential backoff between passes
    }
    assertThat(store.countPending()).as("everything drained").isZero();
  }

  private List<ConsumerRecord<String, String>> consumeAtLeast(String topic, int expected) {
    try (var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "chaos-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName()))) {
      consumer.subscribe(List.of(topic));
      List<ConsumerRecord<String, String>> records = new ArrayList<>();
      long deadline = System.currentTimeMillis() + 20_000;
      // keep polling a beat past `expected` so any surplus duplicates are observed too
      while (System.currentTimeMillis() < deadline) {
        int before = records.size();
        consumer.poll(Duration.ofMillis(250)).forEach(records::add);
        if (records.size() >= expected && records.size() == before) {
          break; // reached the target and a subsequent poll returned nothing new
        }
      }
      return records;
    }
  }

  private static String eventId(ConsumerRecord<String, String> record) {
    return new String(
        record.headers().lastHeader(OutboxPublisher.EVENT_ID_HEADER).value(),
        StandardCharsets.UTF_8);
  }

  @Test
  void brokerOutageBeforeSendLosesNothingAndProducesNoDuplicates() {
    String type = "chaosoutage";
    List<UUID> recorded = recordEvents(type);
    var publisher = new BrokerOutagePublisher();
    publisher.dropBrokerFor(EVENT_COUNT); // first attempt of each event fails at the broker

    drainUntilEmpty(newPoller(publisher));

    List<ConsumerRecord<String, String>> records = consumeAtLeast(type + ".events", EVENT_COUNT);
    // failure was BEFORE the send, so nothing was ever put on the broker twice: exactly N, no dups
    assertThat(records).hasSize(EVENT_COUNT);
    Set<String> deliveredIds = records.stream().map(OutboxChaosIT::eventId).collect(Collectors.toSet());
    assertThat(deliveredIds)
        .as("every recorded event landed exactly once")
        .containsExactlyInAnyOrderElementsOf(recorded.stream().map(UUID::toString).toList());
    assertThat(deadCount()).isZero();
  }

  @Test
  void crashAfterAckReplaysButEventIdHeaderCollapsesToExactlyOnce() {
    String type = "chaoscrash";
    List<UUID> recorded = recordEvents(type);
    var publisher = new CrashAfterAckPublisher();
    // two events crash after the broker acks — each will be re-sent once → two genuine duplicates
    List<UUID> crashing = List.of(recorded.get(2), recorded.get(5));
    crashing.forEach(id -> publisher.crashOnce.add(id.toString()));

    drainUntilEmpty(newPoller(publisher));

    int expectedRaw = EVENT_COUNT + crashing.size();
    List<ConsumerRecord<String, String>> records = consumeAtLeast(type + ".events", expectedRaw);

    // at-least-once: the broker saw the two crashed events twice (no loss, but real duplicates)
    assertThat(records).as("crashed events were physically re-sent").hasSize(expectedRaw);
    Map<String, Long> byEventId =
        records.stream()
            .collect(Collectors.groupingBy(OutboxChaosIT::eventId, Collectors.counting()));
    crashing.forEach(
        id -> assertThat(byEventId.get(id.toString())).as("duplicate on Kafka").isEqualTo(2L));

    // exactly-once effect: dedup on the stable event-id header yields the original N, no loss
    assertThat(byEventId.keySet())
        .containsExactlyInAnyOrderElementsOf(recorded.stream().map(UUID::toString).toList());
    assertThat(deadCount()).isZero();
  }

  private int deadCount() {
    Integer dead =
        jdbc.queryForObject("select count(*) from outbox_events where status = 'DEAD'", Integer.class);
    return dead == null ? 0 : dead;
  }
}
