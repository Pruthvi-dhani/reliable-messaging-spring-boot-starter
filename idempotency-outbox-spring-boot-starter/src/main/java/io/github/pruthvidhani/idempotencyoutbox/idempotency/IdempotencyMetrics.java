package io.github.pruthvidhani.idempotencyoutbox.idempotency;

/**
 * Records idempotency outcomes for observability. Implemented by a Micrometer adapter; a
 * {@link #NOOP} is used when metrics are not wired, so the aspect never has to null-check.
 */
public interface IdempotencyMetrics {

  /** A duplicate request was served from the cache (or no-op replayed). */
  void recordHit();

  /** A first-seen request executed the method and cached its result. */
  void recordMiss();

  /** A key was reused with a different request body (409). */
  void recordConflict();

  /** No-op sink. */
  IdempotencyMetrics NOOP =
      new IdempotencyMetrics() {
        @Override
        public void recordHit() {}

        @Override
        public void recordMiss() {}

        @Override
        public void recordConflict() {}
      };
}
