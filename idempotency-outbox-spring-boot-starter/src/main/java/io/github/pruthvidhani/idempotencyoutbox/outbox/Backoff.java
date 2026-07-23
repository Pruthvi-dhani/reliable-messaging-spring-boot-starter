package io.github.pruthvidhani.idempotencyoutbox.outbox;

import java.time.Duration;

/**
 * Exponential backoff schedule for publish retries: {@code base * 2^attempts}, capped.
 *
 * <p>attempt 0 → base, 1 → 2×base, 2 → 4×base, ... never exceeding {@code cap}.
 */
public record Backoff(Duration base, Duration cap) {

  public Backoff {
    if (base.isNegative() || base.isZero()) {
      throw new IllegalArgumentException("backoff base must be positive");
    }
    if (cap.compareTo(base) < 0) {
      throw new IllegalArgumentException("backoff cap must be >= base");
    }
  }

  /** Delay before the next attempt, given the number of attempts already made. */
  public Duration delayAfter(int attemptsMade) {
    if (attemptsMade < 0) {
      throw new IllegalArgumentException("attemptsMade must be >= 0");
    }
    // Guard the shift: past 62 bits (or on overflow) we are far beyond any sane cap anyway
    if (attemptsMade >= 62) {
      return cap;
    }
    long multiplier = 1L << attemptsMade;
    Duration candidate;
    try {
      candidate = base.multipliedBy(multiplier);
    } catch (ArithmeticException overflow) {
      return cap;
    }
    return candidate.compareTo(cap) > 0 ? cap : candidate;
  }
}
