package io.github.pruthvidhani.idempotencyoutbox.outbox;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs {@link OutboxPoller#pollOnce()} on a fixed delay in a background thread. Registered as a
 * {@link SmartLifecycle} bean so Spring starts it after the context is ready and stops it on
 * shutdown. Any throwable from a pass is logged and swallowed — the loop must survive transient
 * DB/broker outages (that is the outbox's whole job).
 */
public class OutboxPollerScheduler implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(OutboxPollerScheduler.class);

  private final OutboxPoller poller;
  private final Duration pollInterval;
  private ScheduledExecutorService executor;
  private volatile boolean running;

  public OutboxPollerScheduler(OutboxPoller poller, Duration pollInterval) {
    this.poller = poller;
    this.pollInterval = pollInterval;
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "outbox-poller");
              thread.setDaemon(true);
              return thread;
            });
    executor.scheduleWithFixedDelay(
        this::safePollOnce, pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    running = true;
    log.info("Outbox poller started (interval {})", pollInterval);
  }

  private void safePollOnce() {
    try {
      poller.pollOnce();
    } catch (Throwable t) {
      log.warn("Outbox poll pass failed; will retry on next interval", t);
    }
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    executor.shutdownNow();
    running = false;
    log.info("Outbox poller stopped");
  }

  @Override
  public boolean isRunning() {
    return running;
  }
}
