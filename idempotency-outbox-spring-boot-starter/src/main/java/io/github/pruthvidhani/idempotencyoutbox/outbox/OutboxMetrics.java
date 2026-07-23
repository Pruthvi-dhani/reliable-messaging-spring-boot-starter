package io.github.pruthvidhani.idempotencyoutbox.outbox;

import java.time.Duration;

/**
 * Records outbox publish outcomes for observability. Implemented by a Micrometer adapter; a
 * {@link #NOOP} is used when metrics are not wired, so the poller never has to null-check. (Lag
 * gauges are registered by the Micrometer adapter itself, not driven from here.)
 */
public interface OutboxMetrics {

  /** An event was published successfully; {@code latency} is the publish call duration. */
  void recordPublished(Duration latency);

  /** A publish attempt failed and the event was rescheduled for retry. */
  void recordRetried();

  /** An event exhausted its retries and was dead-lettered. */
  void recordDead();

  /** No-op sink. */
  OutboxMetrics NOOP =
      new OutboxMetrics() {
        @Override
        public void recordPublished(Duration latency) {}

        @Override
        public void recordRetried() {}

        @Override
        public void recordDead() {}
      };
}
