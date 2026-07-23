package io.github.pruthvidhani.idempotencyoutbox.autoconfigure;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for request idempotency, bound from the {@code idempotency.*} property tree.
 *
 * <pre>{@code
 * idempotency:
 *   enabled: true
 *   default-ttl: 24h
 *   duplicate-wait: 5s
 *   duplicate-poll-interval: 100ms
 *   sweeper:
 *     enabled: true
 *     interval: 5m
 * }</pre>
 */
@ConfigurationProperties(prefix = "idempotency")
public class IdempotencyProperties {

  /** Master switch for the idempotency feature. */
  private boolean enabled = true;

  /** TTL applied when an {@code @Idempotent} method does not specify one. */
  private Duration defaultTtl = Duration.ofHours(24);

  /** How long a concurrent duplicate waits for the in-flight original before a 409. */
  private Duration duplicateWait = Duration.ofSeconds(5);

  /** How often that waiting duplicate re-checks for the original's completion. */
  private Duration duplicatePollInterval = Duration.ofMillis(100);

  private final Sweeper sweeper = new Sweeper();

  /** Background reclamation of expired dedupe entries. */
  public static class Sweeper {
    /** Whether to run the expired-entry sweeper. */
    private boolean enabled = true;

    /** How often the sweeper deletes rows past their {@code expires_at}. */
    private Duration interval = Duration.ofMinutes(5);

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public Duration getInterval() {
      return interval;
    }

    public void setInterval(Duration interval) {
      this.interval = interval;
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Duration getDefaultTtl() {
    return defaultTtl;
  }

  public void setDefaultTtl(Duration defaultTtl) {
    this.defaultTtl = defaultTtl;
  }

  public Duration getDuplicateWait() {
    return duplicateWait;
  }

  public void setDuplicateWait(Duration duplicateWait) {
    this.duplicateWait = duplicateWait;
  }

  public Duration getDuplicatePollInterval() {
    return duplicatePollInterval;
  }

  public void setDuplicatePollInterval(Duration duplicatePollInterval) {
    this.duplicatePollInterval = duplicatePollInterval;
  }

  public Sweeper getSweeper() {
    return sweeper;
  }
}
