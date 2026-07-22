package io.github.pruthvidhani.idempotencyoutbox.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * Stage 0 infrastructure check: proves {@link AbstractKafkaIT} starts a real broker by doing a
 * produce-consume roundtrip. Later stages replace this with real outbox publisher tests.
 */
class KafkaConnectivityIT extends AbstractKafkaIT {

  @Test
  void producesAndConsumesRoundtrip() {
    String topic = "connectivity-check";
    String payload = UUID.randomUUID().toString();

    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
      producer.send(new ProducerRecord<>(topic, "key", payload));
      producer.flush();
    }

    try (var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "connectivity-check",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName()))) {
      consumer.subscribe(List.of(topic));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(15));
      assertThat(records.count()).isGreaterThanOrEqualTo(1);
      assertThat(records.iterator().next().value()).isEqualTo(payload);
    }
  }
}
