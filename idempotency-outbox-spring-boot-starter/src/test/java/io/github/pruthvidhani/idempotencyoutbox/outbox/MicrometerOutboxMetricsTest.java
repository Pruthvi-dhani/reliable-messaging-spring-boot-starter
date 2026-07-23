package io.github.pruthvidhani.idempotencyoutbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.pruthvidhani.idempotencyoutbox.testsupport.MutableClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MicrometerOutboxMetricsTest {

  private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final OutboxStore store = mock(OutboxStore.class);
  private final MutableClock clock = new MutableClock(NOW);
  private final MicrometerOutboxMetrics metrics =
      new MicrometerOutboxMetrics(registry, store, clock);

  @Test
  void publishTimerCountsAndRecordsLatency() {
    metrics.recordPublished(Duration.ofMillis(40));
    metrics.recordPublished(Duration.ofMillis(60));

    var timer = registry.get("outbox.publish.latency").timer();
    assertThat(timer.count()).isEqualTo(2);
    assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(100.0);
  }

  @Test
  void resultCountersIncrement() {
    metrics.recordPublished(Duration.ofMillis(1));
    metrics.recordRetried();
    metrics.recordRetried();
    metrics.recordDead();

    assertThat(counter("published")).isEqualTo(1);
    assertThat(counter("retried")).isEqualTo(2);
    assertThat(counter("dead")).isEqualTo(1);
  }

  @Test
  void lagEventsGaugeReflectsPendingCount() {
    when(store.countPending()).thenReturn(7);

    assertThat(registry.get("outbox.lag.events").gauge().value()).isEqualTo(7.0);
  }

  @Test
  void lagSecondsGaugeIsAgeOfOldestPending() {
    when(store.oldestPendingTimestamp()).thenReturn(Optional.of(NOW));
    clock.advanceBy(Duration.ofSeconds(30));

    assertThat(registry.get("outbox.lag.seconds").gauge().value()).isEqualTo(30.0);
  }

  @Test
  void lagSecondsIsZeroWhenNothingPending() {
    when(store.oldestPendingTimestamp()).thenReturn(Optional.empty());

    assertThat(registry.get("outbox.lag.seconds").gauge().value()).isEqualTo(0.0);
  }

  private double counter(String result) {
    return registry.get("outbox.events").tag("result", result).counter().count();
  }
}
