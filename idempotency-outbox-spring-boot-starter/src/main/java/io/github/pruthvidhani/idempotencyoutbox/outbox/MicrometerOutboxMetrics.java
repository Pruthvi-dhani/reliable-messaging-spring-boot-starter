package io.github.pruthvidhani.idempotencyoutbox.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;

/**
 * Micrometer-backed {@link OutboxMetrics}.
 *
 * <ul>
 *   <li>{@code outbox.publish.latency} — timer of successful publish calls
 *   <li>{@code outbox.events{result=published|retried|dead}} — counters
 *   <li>{@code outbox.lag.events} — gauge, number of PENDING events
 *   <li>{@code outbox.lag.seconds} — gauge, age of the oldest PENDING event
 * </ul>
 */
public class MicrometerOutboxMetrics implements OutboxMetrics {

  private final Timer publishLatency;
  private final Counter published;
  private final Counter retried;
  private final Counter dead;

  public MicrometerOutboxMetrics(MeterRegistry registry, OutboxStore store, Clock clock) {
    this.publishLatency =
        Timer.builder("outbox.publish.latency")
            .description("Duration of successful outbox publish calls")
            .register(registry);
    this.published =
        Counter.builder("outbox.events").tag("result", "published").register(registry);
    this.retried = Counter.builder("outbox.events").tag("result", "retried").register(registry);
    this.dead = Counter.builder("outbox.events").tag("result", "dead").register(registry);

    Gauge.builder("outbox.lag.events", store, OutboxStore::countPending)
        .description("Number of pending (unpublished) outbox events")
        .register(registry);
    Gauge.builder("outbox.lag.seconds", store, s -> lagSeconds(s, clock))
        .description("Age of the oldest pending outbox event, in seconds")
        .register(registry);
  }

  private static double lagSeconds(OutboxStore store, Clock clock) {
    return store
        .oldestPendingTimestamp()
        .map(oldest -> (double) Duration.between(oldest, clock.instant()).toMillis() / 1000.0)
        .orElse(0.0);
  }

  @Override
  public void recordPublished(Duration latency) {
    publishLatency.record(latency);
    published.increment();
  }

  @Override
  public void recordRetried() {
    retried.increment();
  }

  @Override
  public void recordDead() {
    dead.increment();
  }
}
