package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Micrometer-backed {@link IdempotencyMetrics}.
 *
 * <ul>
 *   <li>{@code idempotency.requests{result=hit|miss|conflict}} — counters
 *   <li>{@code idempotency.hit.rate} — gauge, hits / (hits + misses); conflicts are excluded as
 *       they are client errors, not dedupe outcomes
 * </ul>
 */
public class MicrometerIdempotencyMetrics implements IdempotencyMetrics {

  private final Counter hits;
  private final Counter misses;
  private final Counter conflicts;

  public MicrometerIdempotencyMetrics(MeterRegistry registry) {
    this.hits = Counter.builder("idempotency.requests").tag("result", "hit").register(registry);
    this.misses = Counter.builder("idempotency.requests").tag("result", "miss").register(registry);
    this.conflicts =
        Counter.builder("idempotency.requests").tag("result", "conflict").register(registry);
    Gauge.builder("idempotency.hit.rate", this, MicrometerIdempotencyMetrics::hitRate)
        .description("Fraction of resolved requests served from the dedupe cache")
        .register(registry);
  }

  @Override
  public void recordHit() {
    hits.increment();
  }

  @Override
  public void recordMiss() {
    misses.increment();
  }

  @Override
  public void recordConflict() {
    conflicts.increment();
  }

  private double hitRate() {
    double resolved = hits.count() + misses.count();
    return resolved == 0 ? 0.0 : hits.count() / resolved;
  }
}
