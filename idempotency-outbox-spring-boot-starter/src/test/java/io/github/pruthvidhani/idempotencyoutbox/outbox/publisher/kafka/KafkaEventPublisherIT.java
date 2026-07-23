package io.github.pruthvidhani.idempotencyoutbox.outbox.publisher.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pruthvidhani.idempotencyoutbox.testsupport.AbstractKafkaIT;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaEventPublisherIT extends AbstractKafkaIT {

  private KafkaEventPublisher newPublisher(String bootstrap, Duration timeout) {
    var producerFactory =
        new DefaultKafkaProducerFactory<String, String>(
            Map.of(
                "bootstrap.servers", bootstrap,
                "key.serializer", org.apache.kafka.common.serialization.StringSerializer.class,
                "value.serializer", org.apache.kafka.common.serialization.StringSerializer.class,
                "max.block.ms", String.valueOf(timeout.toMillis())));
    return new KafkaEventPublisher(new KafkaTemplate<>(producerFactory), timeout);
  }

  private List<ConsumerRecord<String, String>> consumeAll(String topic, int expectedCount) {
    try (var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "publisher-it-" + topic,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName()))) {
      consumer.subscribe(List.of(topic));
      List<ConsumerRecord<String, String>> records = new ArrayList<>();
      long deadline = System.currentTimeMillis() + 20_000;
      while (records.size() < expectedCount && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(250)).forEach(records::add);
      }
      return records;
    }
  }

  @Test
  void publishesPayloadKeyAndHeaders() throws Exception {
    var publisher = newPublisher(bootstrapServers(), Duration.ofSeconds(15));

    publisher.publish(
        "publisher-roundtrip",
        "order-1",
        "{\"orderId\":\"order-1\"}",
        Map.of("outbox-event-id", "evt-123", "outbox-event-type", "OrderPlaced"));

    List<ConsumerRecord<String, String>> records = consumeAll("publisher-roundtrip", 1);
    assertThat(records).hasSize(1);
    ConsumerRecord<String, String> record = records.get(0);
    assertThat(record.key()).isEqualTo("order-1");
    assertThat(record.value()).isEqualTo("{\"orderId\":\"order-1\"}");
    assertThat(new String(record.headers().lastHeader("outbox-event-id").value(),
            StandardCharsets.UTF_8))
        .isEqualTo("evt-123");
    assertThat(new String(record.headers().lastHeader("outbox-event-type").value(),
            StandardCharsets.UTF_8))
        .isEqualTo("OrderPlaced");
  }

  @Test
  void sameAggregateIdAlwaysLandsInTheSamePartition() throws Exception {
    // Multi-partition topic: only key-based partitioning keeps one aggregate's events ordered
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic("publisher-partitioning", 4, (short) 1))).all().get();
    }
    var publisher = newPublisher(bootstrapServers(), Duration.ofSeconds(15));

    for (int i = 0; i < 6; i++) {
      publisher.publish(
          "publisher-partitioning", "order-42", "{\"seq\":" + i + "}", Map.of());
    }

    List<ConsumerRecord<String, String>> records = consumeAll("publisher-partitioning", 6);
    assertThat(records).hasSize(6);
    Set<Integer> partitions = new java.util.HashSet<>();
    records.forEach(r -> partitions.add(r.partition()));
    assertThat(partitions).as("one key → one partition → total order").hasSize(1);
    assertThat(records)
        .extracting(ConsumerRecord::value)
        .containsExactly(
            "{\"seq\":0}", "{\"seq\":1}", "{\"seq\":2}", "{\"seq\":3}", "{\"seq\":4}",
            "{\"seq\":5}");
  }

  @Test
  void unreachableBrokerSurfacesAsException() {
    var publisher = newPublisher("localhost:1", Duration.ofMillis(600)); // nothing listens on :1

    assertThatThrownBy(
            () -> publisher.publish("any-topic", "key", "{}", Map.of()))
        .isInstanceOf(Exception.class);
  }
}
