package io.github.pruthvidhani.idempotencyoutbox.testsupport;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Clock} whose instant tests can move forward at will — so TTL/expiry behavior is tested
 * deterministically instead of sleeping on wall-clock time.
 */
public final class MutableClock extends Clock {

  private final AtomicReference<Instant> instant;

  public MutableClock(Instant start) {
    this.instant = new AtomicReference<>(start);
  }

  public void advanceBy(Duration duration) {
    instant.updateAndGet(current -> current.plus(duration));
  }

  public void setTo(Instant newInstant) {
    instant.set(newInstant);
  }

  @Override
  public Instant instant() {
    return instant.get();
  }

  @Override
  public ZoneId getZone() {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return this;
  }
}
