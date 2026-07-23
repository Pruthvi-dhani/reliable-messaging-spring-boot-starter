package io.github.pruthvidhani.idempotencyoutbox.idempotency;

/**
 * Thrown when a request reuses an idempotency key with a different request body than the original
 * (replay protection). Mapped to HTTP 409 Conflict for web consumers.
 */
public class IdempotencyConflictException extends RuntimeException {

  private final String key;

  public IdempotencyConflictException(String key) {
    super("Idempotency key '" + key + "' was already used with a different request body");
    this.key = key;
  }

  public String key() {
    return key;
  }
}
