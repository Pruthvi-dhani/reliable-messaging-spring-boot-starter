package io.github.pruthvidhani.idempotencyoutbox.idempotency;

/**
 * Thrown when a duplicate request arrives while the first request with the same key is still
 * executing, and the completed response did not become available within the interceptor's wait
 * budget. The client should retry after a short delay. Mapped to HTTP 409 Conflict for web
 * consumers (mirroring Stripe's behavior for concurrent idempotent requests).
 */
public class IdempotencyInProgressException extends RuntimeException {

  private final String key;

  public IdempotencyInProgressException(String key) {
    super("A request with idempotency key '" + key + "' is already in progress; retry shortly");
    this.key = key;
  }

  public String key() {
    return key;
  }
}
