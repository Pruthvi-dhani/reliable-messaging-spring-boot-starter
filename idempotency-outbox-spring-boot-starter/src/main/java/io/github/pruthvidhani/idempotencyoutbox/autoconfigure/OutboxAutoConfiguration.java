package io.github.pruthvidhani.idempotencyoutbox.autoconfigure;

import io.github.pruthvidhani.idempotencyoutbox.outbox.Backoff;
import io.github.pruthvidhani.idempotencyoutbox.outbox.EventPublisher;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxPoller;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxPollerScheduler;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxPublisher;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxStore;
import io.github.pruthvidhani.idempotencyoutbox.outbox.TopicResolver;
import io.github.pruthvidhani.idempotencyoutbox.outbox.publisher.kafka.KafkaEventPublisher;
import io.github.pruthvidhani.idempotencyoutbox.outbox.store.jdbc.JdbcOutboxStore;
import java.time.Clock;
import java.util.Map;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Auto-configuration for the transactional outbox. Active when Kafka and JDBC are on the classpath
 * and {@code outbox.enabled} is not {@code false}. Loads after Kafka/JDBC auto-config so its
 * infrastructure beans are available. Every bean is {@link ConditionalOnMissingBean} for override.
 */
@AutoConfiguration(after = {KafkaAutoConfiguration.class, DataSourceAutoConfiguration.class})
@ConditionalOnClass({KafkaTemplate.class, JdbcTemplate.class})
@ConditionalOnProperty(prefix = "outbox", name = "enabled", havingValue = "true",
    matchIfMissing = true)
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public Clock idempotencyOutboxClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean
  public OutboxStore outboxStore(JdbcTemplate jdbcTemplate) {
    return new JdbcOutboxStore(jdbcTemplate);
  }

  @Bean
  @ConditionalOnMissingBean
  public OutboxPublisher outboxPublisher(OutboxStore outboxStore, Clock clock) {
    return new OutboxPublisher(outboxStore, clock);
  }

  @Bean
  @ConditionalOnMissingBean
  public TopicResolver topicResolver() {
    return TopicResolver.byAggregateType();
  }

  /**
   * Dedicated String/String template built from the app's {@code spring.kafka.*} properties. Named
   * so it does not collide with Boot's autoconfigured {@code <Object, Object>} template.
   */
  @Bean
  @ConditionalOnMissingBean(name = "outboxKafkaTemplate")
  public KafkaTemplate<String, String> outboxKafkaTemplate(KafkaProperties properties) {
    Map<String, Object> producerProperties = properties.buildProducerProperties(null);
    producerProperties.put("key.serializer", StringSerializer.class);
    producerProperties.put("value.serializer", StringSerializer.class);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProperties));
  }

  @Bean
  @ConditionalOnMissingBean
  public EventPublisher outboxEventPublisher(
      KafkaTemplate<String, String> outboxKafkaTemplate, OutboxProperties properties) {
    return new KafkaEventPublisher(outboxKafkaTemplate, properties.getPublishTimeout());
  }

  @Bean
  @ConditionalOnMissingBean
  public OutboxPoller outboxPoller(
      OutboxStore outboxStore,
      EventPublisher outboxEventPublisher,
      TopicResolver topicResolver,
      PlatformTransactionManager transactionManager,
      Clock clock,
      OutboxProperties properties) {
    return new OutboxPoller(
        outboxStore,
        outboxEventPublisher,
        topicResolver,
        new TransactionTemplate(transactionManager),
        clock,
        new Backoff(properties.getBackoff().getBase(), properties.getBackoff().getCap()),
        properties.getBatchSize(),
        properties.getMaxAttempts());
  }

  @Bean
  @ConditionalOnMissingBean
  public OutboxPollerScheduler outboxPollerScheduler(
      OutboxPoller outboxPoller, OutboxProperties properties) {
    return new OutboxPollerScheduler(outboxPoller, properties.getPollInterval());
  }
}
