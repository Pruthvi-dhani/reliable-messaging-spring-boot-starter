package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Reclaims storage by deleting dedupe entries past their {@code expires_at}. Runs on a fixed delay
 * in a background thread (registered as a {@link SmartLifecycle} bean).
 *
 * <p>Expired entries are already treated as absent by the store (an expired key behaves like a
 * fresh request); this sweeper only frees the rows. Any throwable from a pass is logged and
 * swallowed so the loop keeps running.
 */
public class IdempotencySweeper implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(IdempotencySweeper.class);

  private final IdempotencyStore store;
  private final Clock clock;
  private final Duration interval;
  private ScheduledExecutorService executor;
  private volatile boolean running;

  public IdempotencySweeper(IdempotencyStore store, Clock clock, Duration interval) {
    this.store = store;
    this.clock = clock;
    this.interval = interval;
  }

  /**
   * Deletes all entries expired as of now.
   *
   * @return number of entries deleted
   */
  public int sweep() {
    int deleted = store.deleteExpired(clock.instant());
    if (deleted > 0) {
      log.debug("Idempotency sweeper deleted {} expired entries", deleted);
    }
    return deleted;
  }

  private void safeSweep() {
    try {
      sweep();
    } catch (Throwable t) {
      log.warn("Idempotency sweep failed; will retry on next interval", t);
    }
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "idempotency-sweeper");
              thread.setDaemon(true);
              return thread;
            });
    executor.scheduleWithFixedDelay(
        this::safeSweep, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    running = true;
    log.info("Idempotency sweeper started (interval {})", interval);
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    executor.shutdownNow();
    running = false;
    log.info("Idempotency sweeper stopped");
  }

  @Override
  public boolean isRunning() {
    return running;
  }
}
