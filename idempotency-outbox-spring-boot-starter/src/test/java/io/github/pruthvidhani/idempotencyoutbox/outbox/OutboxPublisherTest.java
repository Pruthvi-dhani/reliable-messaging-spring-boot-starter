package io.github.pruthvidhani.idempotencyoutbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pruthvidhani.idempotencyoutbox.testsupport.MutableClock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OutboxPublisherTest {

  record OrderPlaced(String orderId, int amountPence) {}

  /** Captures recorded events; the JDBC implementation is exercised in its own IT. */
  static class CapturingStore implements OutboxStore {
    final List<OutboxEvent> recorded = new ArrayList<>();

    @Override
    public void record(OutboxEvent event) {
      recorded.add(event);
    }

    @Override
    public List<OutboxEvent> lockNextBatch(int limit, Instant now) {
      return List.of();
    }

    @Override
    public void markPublished(UUID id, Instant publishedAt) {}

    @Override
    public void reschedule(UUID id, int attempts, Instant nextAttemptAt) {}

    @Override
    public void markDead(UUID id) {}

    @Override
    public int countPending() {
      return recorded.size();
    }
  }

  private final CapturingStore store = new CapturingStore();
  private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T10:00:00Z"));
  private final OutboxPublisher publisher = new OutboxPublisher(store, clock);

  @Test
  void recordsAPendingEventWithSerializedPayloadAndStableId() {
    UUID eventId = publisher.record("order", "order-1", "OrderPlaced", new OrderPlaced("order-1", 4999));

    assertThat(store.recorded).hasSize(1);
    OutboxEvent event = store.recorded.get(0);
    assertThat(event.id()).isEqualTo(eventId);
    assertThat(event.aggregateType()).isEqualTo("order");
    assertThat(event.aggregateId()).isEqualTo("order-1");
    assertThat(event.eventType()).isEqualTo("OrderPlaced");
    assertThat(event.payload()).contains("\"orderId\":\"order-1\"").contains("\"amountPence\":4999");
    assertThat(event.status()).isEqualTo(OutboxEvent.Status.PENDING);
    assertThat(event.attempts()).isZero();
    assertThat(event.createdAt()).isEqualTo(clock.instant());
    assertThat(event.nextAttemptAt()).as("first attempt due immediately").isEqualTo(clock.instant());
    assertThat(event.publishedAt()).isNull();
  }

  @Test
  void headersCarryTheEventIdForConsumerDedupe() {
    UUID eventId = publisher.record("order", "order-1", "OrderPlaced", new OrderPlaced("order-1", 1));

    OutboxEvent event = store.recorded.get(0);
    assertThat(event.headers())
        .containsEntry(OutboxPublisher.EVENT_ID_HEADER, eventId.toString())
        .containsEntry(OutboxPublisher.EVENT_TYPE_HEADER, "OrderPlaced");
  }

  @Test
  void everyEventGetsADistinctId() {
    UUID first = publisher.record("order", "order-1", "OrderPlaced", new OrderPlaced("order-1", 1));
    UUID second = publisher.record("order", "order-1", "OrderPlaced", new OrderPlaced("order-1", 1));

    assertThat(first).isNotEqualTo(second);
  }

  @Test
  void unserializablePayloadIsRejectedUpFront() {
    Object unserializable = new Object(); // no properties — Jackson fails

    assertThatThrownBy(() -> publisher.record("order", "o-1", "Broken", unserializable))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not serializable");
    assertThat(store.recorded).isEmpty();
  }

  @Test
  void defaultTopicResolverUsesAggregateTypeConvention() {
    publisher.record("order", "order-1", "OrderPlaced", new OrderPlaced("order-1", 1));

    assertThat(TopicResolver.byAggregateType().topicFor(store.recorded.get(0)))
        .isEqualTo("order.events");
  }
}
