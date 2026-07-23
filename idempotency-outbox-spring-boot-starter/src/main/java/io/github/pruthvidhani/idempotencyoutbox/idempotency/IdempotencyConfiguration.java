package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.store.jdbc.JdbcIdempotencyStore;
import io.github.pruthvidhani.idempotencyoutbox.web.IdempotencyExceptionAdvice;
import java.time.Clock;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Interim wiring for the idempotency feature: consumers {@code @Import} this until the Stage 3
 * auto-configuration (with {@code @ConfigurationProperties}-driven values) replaces it.
 *
 * <p>Defaults: 24h TTL, 5s wait budget for concurrent duplicates, 100ms poll interval.
 */
@Configuration
@EnableAspectJAutoProxy
public class IdempotencyConfiguration {

  /** System clock by default; tests override with an {@code @Primary} {@code MutableClock}. */
  @Bean
  public Clock idempotencyClock() {
    return Clock.systemUTC();
  }

  @Bean
  public IdempotencyKeyResolver idempotencyKeyResolver() {
    return new IdempotencyKeyResolver();
  }

  @Bean
  public RequestHasher requestHasher() {
    return new RequestHasher();
  }

  @Bean
  public IdempotencyStore idempotencyStore(JdbcTemplate jdbcTemplate, Clock clock) {
    return new JdbcIdempotencyStore(jdbcTemplate, clock);
  }

  /** Maps idempotency failures to 400/409 ProblemDetail responses for web consumers. */
  @Bean
  public IdempotencyExceptionAdvice idempotencyExceptionAdvice() {
    return new IdempotencyExceptionAdvice();
  }

  @Bean
  public IdempotencyAspect idempotencyAspect(
      IdempotencyKeyResolver keyResolver,
      RequestHasher hasher,
      IdempotencyStore store,
      Clock clock) {
    return new IdempotencyAspect(
        keyResolver,
        hasher,
        store,
        clock,
        Duration.ofHours(24),
        Duration.ofSeconds(5),
        Duration.ofMillis(100));
  }
}
