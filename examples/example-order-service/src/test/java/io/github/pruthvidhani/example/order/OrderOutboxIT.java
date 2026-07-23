package io.github.pruthvidhani.example.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/** End-to-end: transactional order + outbox, poller → Kafka, idempotent consumer. */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class OrderOutboxIT {

  @Container @ServiceConnection
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

  // @ServiceConnection does not recognize ConfluentKafkaContainer on Spring Boot 3.3, so wire
  // the bootstrap servers explicitly.
  @Container
  static ConfluentKafkaContainer kafka =
      new ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.1");

  @DynamicPropertySource
  static void kafkaProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private OrderRepository orders;
  @Autowired private OrderPlacedConsumer consumer;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private KafkaTemplate<String, String> outboxKafkaTemplate;
  @Autowired private OutboxPublisher outboxPublisher;
  @Autowired private PlatformTransactionManager transactionManager;

  private static final String BODY =
      "{\"customerId\":\"cust-1\",\"amountPence\":4999,\"currency\":\"GBP\"}";

  private MvcResult placeOrder(String idempotencyKey) throws Exception {
    return mvc.perform(
            post("/orders")
                .header("Idempotency-Key", idempotencyKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(BODY))
        .andExpect(status().isOk())
        .andReturn();
  }

  @Test
  void placingAnOrderPersistsItPublishesToKafkaAndConsumesExactlyOnce() throws Exception {
    MvcResult result = placeOrder("checkout-" + UUID.randomUUID());
    JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
    String orderId = body.get("orderId").asText();

    // The order row exists, and its outbox event was recorded
    String eventId =
        jdbc.queryForObject(
            "select id from outbox_events where aggregate_id = ?", String.class, orderId);
    assertThat(eventId).isNotNull();

    // The poller publishes it; the idempotent consumer processes it exactly once
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(consumer.timesProcessed(eventId)).isEqualTo(1));

    // Outbox row ends PUBLISHED (drained)
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertThat(
                        jdbc.queryForObject(
                            "select status from outbox_events where id = ?::uuid",
                            String.class,
                            eventId))
                    .isEqualTo("PUBLISHED"));
  }

  @Test
  void businessTransactionRollbackRemovesBothOrderAndOutboxEvent() {
    long ordersBefore = orders.count();
    long pendingBefore = countByStatus("PENDING");
    String orderId = UUID.randomUUID().toString();
    var request = new OrderRequest("cust-rollback", 4999, "GBP");

    // Replicate place()'s body — order insert + outbox record — then fail before commit,
    // exactly as if a downstream call had thrown after both writes.
    var tx = new TransactionTemplate(transactionManager);
    assertThatThrownBy(
            () ->
                tx.executeWithoutResult(
                    status -> {
                      orders.insert(orderId, request);
                      outboxPublisher.record(
                          "order",
                          orderId,
                          "OrderPlaced",
                          new OrderPlacedEvent(orderId, "cust-rollback", 4999, "GBP"));
                      throw new RuntimeException("boom after writes");
                    }))
        .hasMessage("boom after writes");

    // Neither the order nor the event survived — atomic rollback (no dual-write window)
    assertThat(orders.count()).isEqualTo(ordersBefore);
    assertThat(countByStatus("PENDING")).isEqualTo(pendingBefore);
  }

  @Test
  void duplicateDeliveryIsProcessedOnceByTheIdempotentConsumer() {
    String eventId = UUID.randomUUID().toString();
    String orderId = UUID.randomUUID().toString();
    String payload =
        "{\"orderId\":\"" + orderId + "\",\"customerId\":\"c\",\"amountPence\":1,\"currency\":\"GBP\"}";

    // Simulate an at-least-once redelivery: the SAME event id published twice
    sendToConsumer(orderId, payload, eventId);
    sendToConsumer(orderId, payload, eventId);

    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(consumer.timesProcessed(eventId)).isEqualTo(1));
    // Give any duplicate a chance to (wrongly) slip through, then confirm still exactly one
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(4))
        .untilAsserted(() -> assertThat(consumer.timesProcessed(eventId)).isEqualTo(1));
  }

  private void sendToConsumer(String key, String payload, String eventId) {
    var record = new ProducerRecord<>("order.events", key, payload);
    record.headers().add(OutboxPublisher.EVENT_ID_HEADER, eventId.getBytes(StandardCharsets.UTF_8));
    outboxKafkaTemplate.send(record);
  }

  private long countByStatus(String status) {
    Integer count =
        jdbc.queryForObject(
            "select count(*) from outbox_events where status = ?", Integer.class, status);
    return count == null ? 0 : count;
  }
}
