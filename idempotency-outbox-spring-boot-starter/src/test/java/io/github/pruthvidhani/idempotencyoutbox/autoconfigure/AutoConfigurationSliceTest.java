package io.github.pruthvidhani.idempotencyoutbox.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyAspect;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyStore;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxPoller;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxPublisher;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Verifies the auto-configuration wiring in isolation with {@link ApplicationContextRunner}: beans
 * present when enabled, absent when disabled, and overridable by the consumer.
 */
class AutoConfigurationSliceTest {

  // An H2 datasource is enough for bean wiring (no SQL is executed here).
  private final ApplicationContextRunner idempotencyRunner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  DataSourceAutoConfiguration.class,
                  JdbcTemplateAutoConfiguration.class,
                  IdempotencyAutoConfiguration.class))
          .withPropertyValues(
              "spring.datasource.url="
                  + EmbeddedDatabaseConnection.H2.getUrl("idem-slice"),
              "spring.datasource.driver-class-name=org.h2.Driver");

  private final ApplicationContextRunner outboxRunner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  DataSourceAutoConfiguration.class,
                  JdbcTemplateAutoConfiguration.class,
                  DataSourceTransactionManagerAutoConfiguration.class,
                  KafkaAutoConfiguration.class,
                  OutboxAutoConfiguration.class))
          .withPropertyValues(
              "spring.datasource.url=" + EmbeddedDatabaseConnection.H2.getUrl("outbox-slice"),
              "spring.datasource.driver-class-name=org.h2.Driver",
              "spring.kafka.bootstrap-servers=localhost:9092");

  @Test
  void idempotencyBeansPresentByDefault() {
    idempotencyRunner.run(
        context ->
            assertThat(context)
                .hasSingleBean(IdempotencyAspect.class)
                .hasSingleBean(IdempotencyStore.class)
                .hasSingleBean(IdempotencyProperties.class));
  }

  @Test
  void idempotencyBeansAbsentWhenDisabled() {
    idempotencyRunner
        .withPropertyValues("idempotency.enabled=false")
        .run(
            context ->
                assertThat(context)
                    .doesNotHaveBean(IdempotencyAspect.class)
                    .doesNotHaveBean(IdempotencyStore.class));
  }

  @Test
  void customStoreOverridesTheDefault() {
    idempotencyRunner
        .withUserConfiguration(CustomStoreConfig.class)
        .run(
            context -> {
              assertThat(context).hasSingleBean(IdempotencyStore.class);
              assertThat(context.getBean(IdempotencyStore.class))
                  .isInstanceOf(NoOpIdempotencyStore.class);
            });
  }

  @Test
  void ttlPropertyFlowsIntoTheAspect() {
    idempotencyRunner
        .withPropertyValues("idempotency.default-ttl=1h")
        .run(
            context ->
                assertThat(context.getBean(IdempotencyProperties.class).getDefaultTtl())
                    .isEqualTo(Duration.ofHours(1)));
  }

  @Test
  void outboxBeansPresentByDefault() {
    outboxRunner.run(
        context ->
            assertThat(context)
                .hasSingleBean(OutboxPublisher.class)
                .hasSingleBean(OutboxPoller.class)
                .hasSingleBean(OutboxProperties.class));
  }

  @Test
  void outboxBeansAbsentWhenDisabled() {
    outboxRunner
        .withPropertyValues("outbox.enabled=false")
        .run(
            context ->
                assertThat(context)
                    .doesNotHaveBean(OutboxPublisher.class)
                    .doesNotHaveBean(OutboxPoller.class));
  }

  @Test
  void outboxBatchSizePropertyBinds() {
    outboxRunner
        .withPropertyValues("outbox.batch-size=42")
        .run(
            context ->
                assertThat(context.getBean(OutboxProperties.class).getBatchSize()).isEqualTo(42));
  }

  @Configuration
  static class CustomStoreConfig {
    @Bean
    IdempotencyStore idempotencyStore() {
      return new NoOpIdempotencyStore();
    }
  }

  /** Minimal consumer-supplied store to prove {@code @ConditionalOnMissingBean} backs off. */
  static class NoOpIdempotencyStore implements IdempotencyStore {
    @Override
    public Optional<io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyRecord> find(
        String key) {
      return Optional.empty();
    }

    @Override
    public boolean putInProgress(
        String key, String requestHash, java.time.Instant createdAt, java.time.Instant expiresAt) {
      return true;
    }

    @Override
    public void complete(String key, byte[] responsePayload, Integer responseStatus) {}

    @Override
    public void remove(String key) {}

    @Override
    public int deleteExpired(java.time.Instant now) {
      return 0;
    }
  }
}
