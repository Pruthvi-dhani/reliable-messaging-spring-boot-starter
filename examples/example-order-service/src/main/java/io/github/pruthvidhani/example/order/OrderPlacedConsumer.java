package io.github.pruthvidhani.example.order;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.Idempotent;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxPublisher;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Downstream consumer of {@code OrderPlaced}. Stands in for e.g. inventory reservation — the "side
 * effect" that must happen exactly once per order.
 *
 * <p>The outbox delivers <b>at-least-once</b> (a poller crash after Kafka ack but before marking
 * the row PUBLISHED will republish). {@code @Idempotent} keyed on the stable event id collapses
 * those duplicates to a single effect. {@code hashBody = false}: the event id is authoritative, so
 * no request-hash comparison is needed.
 */
@Component
public class OrderPlacedConsumer {

  private final List<String> processedOrderIds = new CopyOnWriteArrayList<>();

  @KafkaListener(topics = "order.events", groupId = "order-fulfilment")
  @Idempotent(key = "#eventId", hashBody = false)
  public void onOrderPlaced(
      @Payload String payload, @Header(OutboxPublisher.EVENT_ID_HEADER) String eventId) {
    // Real code would reserve inventory / send confirmation here — recorded for the test.
    processedOrderIds.add(eventId);
  }

  public List<String> processedEventIds() {
    return List.copyOf(processedOrderIds);
  }

  public int timesProcessed(String eventId) {
    return (int) processedOrderIds.stream().filter(eventId::equals).count();
  }
}
