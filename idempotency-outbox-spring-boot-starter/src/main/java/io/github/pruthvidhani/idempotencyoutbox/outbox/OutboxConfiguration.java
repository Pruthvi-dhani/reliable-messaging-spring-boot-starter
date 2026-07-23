package io.github.pruthvidhani.idempotencyoutbox.outbox;

import io.github.pruthvidhani.idempotencyoutbox.outbox.publisher.kafka.KafkaEventPublisher;
import io.github.pruthvidhani.idempotencyoutbox.outbox.store.jdbc.JdbcOutboxStore;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Interim wiring for the outbox feature: consumers {@code @Import} this (alongside
 * {@code IdempotencyConfiguration}, which provides the {@code Clock}) until the Stage 3
 * auto-configuration replaces it.
 *
 * <p>Interim defaults: poll every 500ms, batch 100, backoff 200ms doubling to a 30s cap, dead
 * letter after 8 attempts, 15s Kafka ack timeout.
 */
@Configuration
public class OutboxConfiguration {

  @Bean
  public OutboxStore outboxStore(JdbcTemplate jdbcTemplate) {
    return new JdbcOutboxStore(jdbcTemplate);
  }

  @Bean
  public OutboxPublisher outboxPublisher(OutboxStore outboxStore, Clock clock) {
    return new OutboxPublisher(outboxStore, clock);
  }

  @Bean
  public TopicResolver topicResolver() {
    return TopicResolver.byAggregateType();
  }

  /**
   * Dedicated String/String template for outbox publishing, built from the app's {@code
   * spring.kafka.*} properties (Boot's autoconfigured template is {@code <Object, Object>}, which
   * does not match what the outbox needs).
   */
  @Bean
  public KafkaTemplate<String, String> outboxKafkaTemplate(KafkaProperties properties) {
    Map<String, Object> producerProperties = properties.buildProducerProperties(null);
    producerProperties.put("key.serializer", StringSerializer.class);
    producerProperties.put("value.serializer", StringSerializer.class);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProperties));
  }

  @Bean
  public EventPublisher outboxEventPublisher(KafkaTemplate<String, String> outboxKafkaTemplate) {
    return new KafkaEventPublisher(outboxKafkaTemplate, Duration.ofSeconds(15));
  }

  @Bean
  public OutboxPoller outboxPoller(
      OutboxStore outboxStore,
      EventPublisher outboxEventPublisher,
      TopicResolver topicResolver,
      PlatformTransactionManager transactionManager,
      Clock clock) {
    return new OutboxPoller(
        outboxStore,
        outboxEventPublisher,
        topicResolver,
        new TransactionTemplate(transactionManager),
        clock,
        new Backoff(Duration.ofMillis(200), Duration.ofSeconds(30)),
        100,
        8);
  }

  @Bean
  public OutboxPollerScheduler outboxPollerScheduler(OutboxPoller outboxPoller) {
    return new OutboxPollerScheduler(outboxPoller, Duration.ofMillis(500));
  }
}
