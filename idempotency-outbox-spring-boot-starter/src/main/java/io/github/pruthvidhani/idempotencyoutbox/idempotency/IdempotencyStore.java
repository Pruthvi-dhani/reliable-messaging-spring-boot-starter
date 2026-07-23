package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import java.time.Instant;
import java.util.Optional;

/**
 * SPI for the dedupe storage backing {@link Idempotent}. The default implementation is
 * Postgres-backed ({@code store.jdbc}); alternatives (Redis, DynamoDB) can be plugged in by
 * providing another implementation of this interface.
 *
 * <p>Contract notes for implementors:
 *
 * <ul>
 *   <li>{@link #putInProgress} is the concurrency gate: exactly one caller may win for a given
 *       key. It must be atomic (e.g. {@code INSERT ... ON CONFLICT DO NOTHING}).
 *   <li>Expired entries (past {@code expiresAt}) must be treated as absent by {@link #find} or be
 *       replaceable by {@link #putInProgress}, so an expired key behaves like a fresh request.
 * </ul>
 */
public interface IdempotencyStore {

  /** Returns the entry for the key, empty if none exists (or only an expired one exists). */
  Optional<IdempotencyRecord> find(String key);

  /**
   * Atomically claims the key by inserting an {@link IdempotencyRecord.Status#IN_PROGRESS} entry.
   *
   * @return true if this caller won the claim (proceed to execute the method); false if an entry
   *     already exists (someone else is executing, or has completed — re-read with {@link #find})
   */
  boolean putInProgress(String key, String requestHash, Instant createdAt, Instant expiresAt);

  /**
   * Marks the key {@link IdempotencyRecord.Status#COMPLETED}, storing the response for replay.
   *
   * @param responseStatus HTTP status to replay, null for non-HTTP results
   */
  void complete(String key, byte[] responsePayload, Integer responseStatus);

  /**
   * Removes the claim for the key (e.g. the method threw — the client must be able to retry).
   * Removing a key that does not exist is a no-op.
   */
  void remove(String key);

  /**
   * Deletes entries whose {@code expiresAt} is at or before {@code now}.
   *
   * @return number of entries deleted (for sweeper metrics)
   */
  int deleteExpired(Instant now);
}
