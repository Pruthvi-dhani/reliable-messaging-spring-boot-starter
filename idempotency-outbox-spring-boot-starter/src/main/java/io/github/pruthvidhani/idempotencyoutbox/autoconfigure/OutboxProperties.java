package io.github.pruthvidhani.idempotencyoutbox.autoconfigure;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the transactional outbox, bound from the {@code outbox.*} property tree.
 *
 * <pre>{@code
 * outbox:
 *   enabled: true
 *   poll-interval: 500ms
 *   batch-size: 100
 *   max-attempts: 8
 *   publish-timeout: 15s
 *   backoff:
 *     base: 200ms
 *     cap: 30s
 * }</pre>
 */
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

  /** Master switch for the outbox feature. */
  private boolean enabled = true;

  /** How often the poller drains a batch of pending events. */
  private Duration pollInterval = Duration.ofMillis(500);

  /** Maximum events locked and processed per poll pass. */
  private int batchSize = 100;

  /** Attempts before an event is dead-lettered. */
  private int maxAttempts = 8;

  /** Per-publish broker ack timeout; exceeding it counts as a failed attempt. */
  private Duration publishTimeout = Duration.ofSeconds(15);

  private final Backoff backoff = new Backoff();

  /** Exponential backoff between publish retries: {@code base * 2^attempts}, capped. */
  public static class Backoff {
    /** Delay after the first failure; doubles each subsequent failure. */
    private Duration base = Duration.ofMillis(200);

    /** Upper bound on the retry delay. */
    private Duration cap = Duration.ofSeconds(30);

    public Duration getBase() {
      return base;
    }

    public void setBase(Duration base) {
      this.base = base;
    }

    public Duration getCap() {
      return cap;
    }

    public void setCap(Duration cap) {
      this.cap = cap;
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Duration getPollInterval() {
    return pollInterval;
  }

  public void setPollInterval(Duration pollInterval) {
    this.pollInterval = pollInterval;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public void setMaxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
  }

  public Duration getPublishTimeout() {
    return publishTimeout;
  }

  public void setPublishTimeout(Duration publishTimeout) {
    this.publishTimeout = publishTimeout;
  }

  public Backoff getBackoff() {
    return backoff;
  }
}
