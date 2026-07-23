package io.github.pruthvidhani.idempotencyoutbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffTest {

  private final Backoff backoff = new Backoff(Duration.ofMillis(200), Duration.ofSeconds(30));

  @Test
  void doublesPerAttempt() {
    assertThat(backoff.delayAfter(0)).isEqualTo(Duration.ofMillis(200));
    assertThat(backoff.delayAfter(1)).isEqualTo(Duration.ofMillis(400));
    assertThat(backoff.delayAfter(2)).isEqualTo(Duration.ofMillis(800));
    assertThat(backoff.delayAfter(3)).isEqualTo(Duration.ofMillis(1600));
  }

  @Test
  void isMonotonicUntilTheCap() {
    Duration previous = Duration.ZERO;
    for (int attempt = 0; attempt < 20; attempt++) {
      Duration delay = backoff.delayAfter(attempt);
      assertThat(delay).isGreaterThanOrEqualTo(previous);
      previous = delay;
    }
  }

  @Test
  void neverExceedsTheCap() {
    assertThat(backoff.delayAfter(10)).isEqualTo(Duration.ofSeconds(30));
    assertThat(backoff.delayAfter(100)).isEqualTo(Duration.ofSeconds(30));
    assertThat(backoff.delayAfter(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void rejectsInvalidConfiguration() {
    assertThatThrownBy(() -> new Backoff(Duration.ZERO, Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Backoff(Duration.ofSeconds(2), Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> backoff.delayAfter(-1)).isInstanceOf(IllegalArgumentException.class);
  }
}
