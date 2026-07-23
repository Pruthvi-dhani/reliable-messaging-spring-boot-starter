package io.github.pruthvidhani.idempotencyoutbox.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Drains the outbox: locks a batch of due PENDING events, publishes each, and marks the outcome.
 *
 * <p>One {@link #pollOnce() pass} runs inside a single transaction — required because the row
 * locks taken by {@link OutboxStore#lockNextBatch} (SKIP LOCKED) live only as long as the
 * transaction, which is also what makes concurrent poller instances safe.
 *
 * <p>Outcomes per event:
 *
 * <ul>
 *   <li><b>published</b> — broker acked → {@code markPublished}.
 *   <li><b>failed</b> — attempt failed → rescheduled with exponential backoff, or {@code DEAD}
 *       once {@code maxAttempts} is reached (operator intervention required).
 *   <li><b>stalled</b> — a preceding event of the <i>same aggregate</i> failed in this pass; the
 *       event is rescheduled untried (attempts unchanged) to the same due time as the failed one.
 *       Publishing it would overtake the failed event and break per-aggregate ordering.
 * </ul>
 */
public class OutboxPoller {

  private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

  private final OutboxStore store;
  private final EventPublisher publisher;
  private final TopicResolver topicResolver;
  private final TransactionOperations transaction;
  private final Clock clock;
  private final Backoff backoff;
  private final int batchSize;
  private final int maxAttempts;
  private final OutboxMetrics metrics;

  public OutboxPoller(
      OutboxStore store,
      EventPublisher publisher,
      TopicResolver topicResolver,
      TransactionOperations transaction,
      Clock clock,
      Backoff backoff,
      int batchSize,
      int maxAttempts,
      OutboxMetrics metrics) {
    this.store = store;
    this.publisher = publisher;
    this.topicResolver = topicResolver;
    this.transaction = transaction;
    this.clock = clock;
    this.backoff = backoff;
    this.batchSize = batchSize;
    this.maxAttempts = maxAttempts;
    this.metrics = metrics;
  }

  /**
   * Locks and processes one batch.
   *
   * @return number of events successfully published in this pass
   */
  public int pollOnce() {
    Integer published =
        transaction.execute(
            status -> {
              List<OutboxEvent> batch = store.lockNextBatch(batchSize, clock.instant());
              Set<String> stalledAggregates = new HashSet<>();
              int successes = 0;
              for (OutboxEvent event : batch) {
                if (stalledAggregates.contains(event.aggregateId())) {
                  stall(event);
                  continue;
                }
                if (publish(event)) {
                  successes++;
                } else {
                  stalledAggregates.add(event.aggregateId());
                }
              }
              return successes;
            });
    return published == null ? 0 : published;
  }

  /** Publishes one event; on failure applies backoff or dead-letters. Returns success. */
  private boolean publish(OutboxEvent event) {
    long startNanos = System.nanoTime();
    try {
      publisher.publish(
          topicResolver.topicFor(event), event.aggregateId(), event.payload(), event.headers());
      store.markPublished(event.id(), clock.instant());
      metrics.recordPublished(Duration.ofNanos(System.nanoTime() - startNanos));
      return true;
    } catch (Exception failure) {
      int attemptsMade = event.attempts() + 1;
      if (attemptsMade >= maxAttempts) {
        store.markDead(event.id());
        metrics.recordDead();
        log.error(
            "Outbox event {} ({} for aggregate {}) DEAD after {} attempts",
            event.id(),
            event.eventType(),
            event.aggregateId(),
            attemptsMade,
            failure);
      } else {
        Instant nextAttemptAt = clock.instant().plus(backoff.delayAfter(event.attempts()));
        store.reschedule(event.id(), attemptsMade, nextAttemptAt);
        metrics.recordRetried();
        log.warn(
            "Outbox event {} failed attempt {}/{}; next attempt at {}",
            event.id(),
            attemptsMade,
            maxAttempts,
            nextAttemptAt,
            failure);
      }
      return false;
    }
  }

  /** Reschedules an untried event to keep it behind its aggregate's failed event. */
  private void stall(OutboxEvent event) {
    store.reschedule(
        event.id(), event.attempts(), clock.instant().plus(backoff.delayAfter(event.attempts())));
  }
}
