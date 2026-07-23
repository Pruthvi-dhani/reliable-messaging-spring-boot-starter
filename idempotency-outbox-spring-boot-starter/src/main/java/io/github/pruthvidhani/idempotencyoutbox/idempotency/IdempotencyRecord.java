package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import java.time.Instant;

/**
 * A dedupe entry, one per idempotency key. Mirrors one row of the {@code idempotency_keys} table.
 *
 * @param key the client-supplied idempotency key (primary key)
 * @param requestHash canonical hash of the original request, used to detect same-key/different-body
 *     conflicts; never null (empty string when hashing is disabled)
 * @param responsePayload serialized response of the completed execution; null while
 *     {@link Status#IN_PROGRESS}
 * @param responseStatus HTTP status of the completed execution; null while in progress or for
 *     non-HTTP results
 * @param status lifecycle state of the entry
 * @param createdAt when the first request with this key arrived
 * @param expiresAt when this entry stops being honored (a retry after this is a fresh request)
 */
public record IdempotencyRecord(
    String key,
    String requestHash,
    byte[] responsePayload,
    Integer responseStatus,
    Status status,
    Instant createdAt,
    Instant expiresAt) {

  /** Lifecycle of a dedupe entry. */
  public enum Status {
    /** The first request is still executing; concurrent duplicates must wait or be rejected. */
    IN_PROGRESS,
    /** Execution finished; {@code responsePayload} holds the replayable result. */
    COMPLETED
  }

  /** True if this entry is past its TTL at the given instant. */
  public boolean isExpiredAt(Instant now) {
    return !now.isBefore(expiresAt);
  }
}
