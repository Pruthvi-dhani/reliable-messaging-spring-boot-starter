package io.github.pruthvidhani.idempotencyoutbox.idempotency;

/**
 * Thrown when the {@link Idempotent#key()} expression resolves to null or blank — the client did
 * not supply an idempotency key. Fail-closed policy: mapped to HTTP 400 Bad Request for web
 * consumers rather than silently running without dedupe.
 */
public class IdempotencyKeyMissingException extends RuntimeException {

  public IdempotencyKeyMissingException(String expression) {
    super(
        "Idempotency key is required but expression '"
            + expression
            + "' resolved to null or blank (is the Idempotency-Key header missing?)");
  }
}
