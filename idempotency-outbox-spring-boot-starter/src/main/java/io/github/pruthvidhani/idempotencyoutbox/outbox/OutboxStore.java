package io.github.pruthvidhani.idempotencyoutbox.outbox;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * SPI for outbox persistence. Default implementation is Postgres ({@code store.jdbc}).
 *
 * <p>Contract notes for implementors:
 *
 * <ul>
 *   <li>{@link #record} MUST participate in the caller's transaction — that is the whole point of
 *       the outbox pattern (business write + event write commit or roll back together).
 *   <li>{@link #lockNextBatch} MUST lock the returned rows against concurrent pollers (e.g.
 *       {@code FOR UPDATE SKIP LOCKED}) and therefore MUST be called inside an active transaction
 *       that stays open until the batch is marked. Rows are returned in {@code created_at} order.
 * </ul>
 */
public interface OutboxStore {

  /** Inserts a new {@link OutboxEvent.Status#PENDING} event within the caller's transaction. */
  void record(OutboxEvent event);

  /**
   * Locks and returns up to {@code limit} PENDING events due for an attempt ({@code
   * next_attempt_at <= now}), oldest first, skipping rows locked by other pollers.
   */
  List<OutboxEvent> lockNextBatch(int limit, Instant now);

  /** Marks the event PUBLISHED at the given instant; terminal. */
  void markPublished(UUID id, Instant publishedAt);

  /** Schedules the next retry: increments to {@code attempts}, due at {@code nextAttemptAt}. */
  void reschedule(UUID id, int attempts, Instant nextAttemptAt);

  /** Parks the event as DEAD (retries exhausted); terminal. */
  void markDead(UUID id);

  /** Number of PENDING events (outbox lag in events, for metrics/tests). */
  int countPending();

  /**
   * Timestamp of the oldest PENDING event ({@code min(created_at)}), empty if none. The age of
   * this instant is the outbox lag in seconds.
   */
  Optional<Instant> oldestPendingTimestamp();
}
