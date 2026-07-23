package io.github.pruthvidhani.idempotencyoutbox.autoconfigure;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyAspect;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyKeyResolver;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyMetrics;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyStore;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.Idempotent;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.MicrometerIdempotencyMetrics;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.RequestHasher;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.store.jdbc.JdbcIdempotencyStore;
import io.github.pruthvidhani.idempotencyoutbox.web.IdempotencyExceptionAdvice;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Auto-configuration for request idempotency. Active when {@link Idempotent} and {@link
 * JdbcTemplate} are on the classpath and {@code idempotency.enabled} is not {@code false}. Every
 * bean is {@link ConditionalOnMissingBean}, so a consumer can override any piece (e.g. a custom
 * {@link IdempotencyStore} or {@link Clock}).
 */
@AutoConfiguration(after = DataSourceAutoConfiguration.class)
@ConditionalOnClass({Idempotent.class, JdbcTemplate.class})
@ConditionalOnProperty(prefix = "idempotency", name = "enabled", havingValue = "true",
    matchIfMissing = true)
@EnableConfigurationProperties(IdempotencyProperties.class)
@EnableAspectJAutoProxy
public class IdempotencyAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public Clock idempotencyOutboxClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean
  public IdempotencyKeyResolver idempotencyKeyResolver() {
    return new IdempotencyKeyResolver();
  }

  @Bean
  @ConditionalOnMissingBean
  public RequestHasher requestHasher() {
    return new RequestHasher();
  }

  @Bean
  @ConditionalOnMissingBean
  public IdempotencyStore idempotencyStore(JdbcTemplate jdbcTemplate, Clock clock) {
    return new JdbcIdempotencyStore(jdbcTemplate, clock);
  }

  /**
   * Uses the app's {@link MeterRegistry} if one is configured, otherwise a throwaway registry so
   * metric recording is always safe (just not exported).
   */
  @Bean
  @ConditionalOnMissingBean
  public IdempotencyMetrics idempotencyMetrics(ObjectProvider<MeterRegistry> meterRegistry) {
    return new MicrometerIdempotencyMetrics(
        meterRegistry.getIfAvailable(SimpleMeterRegistry::new));
  }

  @Bean
  @ConditionalOnMissingBean
  public IdempotencyAspect idempotencyAspect(
      IdempotencyKeyResolver keyResolver,
      RequestHasher hasher,
      IdempotencyStore store,
      Clock clock,
      IdempotencyProperties properties,
      IdempotencyMetrics metrics) {
    return new IdempotencyAspect(
        keyResolver,
        hasher,
        store,
        clock,
        properties.getDefaultTtl(),
        properties.getDuplicateWait(),
        properties.getDuplicatePollInterval(),
        metrics);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
  public IdempotencyExceptionAdvice idempotencyExceptionAdvice() {
    return new IdempotencyExceptionAdvice();
  }
}
