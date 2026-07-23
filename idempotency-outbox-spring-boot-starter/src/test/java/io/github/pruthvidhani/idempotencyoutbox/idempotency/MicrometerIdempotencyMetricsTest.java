package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class MicrometerIdempotencyMetricsTest {

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final MicrometerIdempotencyMetrics metrics = new MicrometerIdempotencyMetrics(registry);

  private double counter(String result) {
    return registry.get("idempotency.requests").tag("result", result).counter().count();
  }

  @Test
  void countsOutcomesByResultTag() {
    metrics.recordMiss();
    metrics.recordHit();
    metrics.recordHit();
    metrics.recordConflict();

    assertThat(counter("miss")).isEqualTo(1);
    assertThat(counter("hit")).isEqualTo(2);
    assertThat(counter("conflict")).isEqualTo(1);
  }

  @Test
  void hitRateIsHitsOverHitsPlusMisses() {
    metrics.recordMiss();
    metrics.recordMiss();
    metrics.recordMiss();
    metrics.recordHit(); // 1 hit / 4 resolved = 0.25

    assertThat(registry.get("idempotency.hit.rate").gauge().value()).isEqualTo(0.25);
  }

  @Test
  void hitRateIsZeroBeforeAnyTraffic() {
    assertThat(registry.get("idempotency.hit.rate").gauge().value()).isEqualTo(0.0);
  }

  @Test
  void conflictsDoNotAffectHitRate() {
    metrics.recordHit();
    metrics.recordConflict();

    assertThat(registry.get("idempotency.hit.rate").gauge().value()).isEqualTo(1.0);
  }
}
