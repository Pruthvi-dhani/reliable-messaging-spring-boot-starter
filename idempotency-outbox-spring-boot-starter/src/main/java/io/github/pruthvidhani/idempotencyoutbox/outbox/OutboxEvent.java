package io.github.pruthvidhani.idempotencyoutbox.outbox;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * One event awaiting (or done with) publication. Mirrors a row of {@code outbox_events}.
 *
 * <p>The {@code id} is assigned once, when the event is recorded in the business transaction, and
 * travels with every publish attempt — so downstream consumers can deduplicate re-deliveries on it
 * (the producer side of the at-least-once + idempotent-consumer contract).
 *
 * @param id stable event id, assigned at record time
 * @param aggregateType kind of aggregate that emitted the event (e.g. {@code "order"}); drives
 *     topic resolution
 * @param aggregateId id of the emitting aggregate; used as the Kafka message key so all events of
 *     one aggregate stay in one partition, preserving per-aggregate ordering
 * @param eventType what happened (e.g. {@code "OrderPlaced"})
 * @param payload event body as JSON
 * @param headers extra metadata propagated as broker message headers
 * @param status lifecycle state
 * @param attempts publish attempts made so far
 * @param nextAttemptAt when the next attempt is due (backoff schedule); null once terminal
 * @param createdAt when the event was recorded (insertion order = publish order)
 * @param publishedAt when publication succeeded; null until then
 */
public record OutboxEvent(
    UUID id,
    String aggregateType,
    String aggregateId,
    String eventType,
    String payload,
    Map<String, String> headers,
    Status status,
    int attempts,
    Instant nextAttemptAt,
    Instant createdAt,
    Instant publishedAt) {

  /** Lifecycle of an outbox event. */
  public enum Status {
    /** Awaiting publication (or awaiting its next retry). */
    PENDING,
    /** Successfully handed to the broker; terminal. */
    PUBLISHED,
    /** Retries exhausted; parked for operator attention (dead-letter); terminal. */
    DEAD
  }
}
