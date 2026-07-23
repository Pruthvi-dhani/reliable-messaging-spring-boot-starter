package io.github.pruthvidhani.idempotencyoutbox.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;

/**
 * What business code calls to emit an event. Call it <b>inside the same transaction</b> as the
 * business write — the event row commits or rolls back together with the business data, and no
 * broker I/O happens on the request path (the poller publishes later).
 *
 * <pre>{@code
 * @Transactional
 * public OrderResponse place(OrderRequest request) {
 *   orders.insert(order);
 *   outboxPublisher.record("order", order.id(), "OrderPlaced", toEventPayload(order));
 *   return response;
 * }
 * }</pre>
 */
public class OutboxPublisher {

  /** Message header carrying the stable event id — consumers dedupe on it. */
  public static final String EVENT_ID_HEADER = "outbox-event-id";

  /** Message header carrying the event type. */
  public static final String EVENT_TYPE_HEADER = "outbox-event-type";

  private final OutboxStore store;
  private final Clock clock;
  private final ObjectMapper payloadMapper;

  public OutboxPublisher(OutboxStore store, Clock clock) {
    this.store = store;
    this.clock = clock;
    this.payloadMapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();
  }

  /**
   * Records an event for reliable publication.
   *
   * @param aggregateType kind of aggregate (drives topic resolution), e.g. {@code "order"}
   * @param aggregateId the aggregate's id (Kafka key — per-aggregate ordering)
   * @param eventType what happened, e.g. {@code "OrderPlaced"}
   * @param payload event body; serialized to JSON
   * @return the stable event id (also sent as the {@value #EVENT_ID_HEADER} header)
   */
  public UUID record(String aggregateType, String aggregateId, String eventType, Object payload) {
    UUID eventId = UUID.randomUUID();
    String json;
    try {
      json = payloadMapper.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Outbox payload for event type '" + eventType + "' is not serializable to JSON", e);
    }
    var event =
        new OutboxEvent(
            eventId,
            aggregateType,
            aggregateId,
            eventType,
            json,
            Map.of(EVENT_ID_HEADER, eventId.toString(), EVENT_TYPE_HEADER, eventType),
            OutboxEvent.Status.PENDING,
            0,
            clock.instant(), // first attempt due immediately
            clock.instant(),
            null);
    store.record(event);
    return eventId;
  }
}
