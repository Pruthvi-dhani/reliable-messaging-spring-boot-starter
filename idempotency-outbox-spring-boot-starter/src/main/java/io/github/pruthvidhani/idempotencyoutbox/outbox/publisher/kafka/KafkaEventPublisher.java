package io.github.pruthvidhani.idempotencyoutbox.outbox.publisher.kafka;

import io.github.pruthvidhani.idempotencyoutbox.outbox.EventPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Kafka implementation of {@link EventPublisher}.
 *
 * <p>The message key is the aggregate id: Kafka assigns all messages with one key to one
 * partition, and a partition is totally ordered — which is exactly the per-aggregate ordering
 * guarantee the outbox promises. Headers (including the stable event id for consumer dedupe) are
 * copied onto the Kafka record.
 *
 * <p>Publishing is synchronous per the SPI contract: the method returns only after the broker
 * acknowledges, so the poller can safely mark the row PUBLISHED afterwards. Any failure (including
 * the ack timeout) surfaces as an exception → the poller reschedules with backoff.
 */
public class KafkaEventPublisher implements EventPublisher {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final Duration sendTimeout;

  public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate, Duration sendTimeout) {
    this.kafkaTemplate = kafkaTemplate;
    this.sendTimeout = sendTimeout;
  }

  @Override
  public void publish(String topic, String key, String payload, Map<String, String> headers)
      throws Exception {
    var record = new ProducerRecord<>(topic, key, payload);
    headers.forEach(
        (name, value) -> record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));
    kafkaTemplate.send(record).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
