package io.github.pruthvidhani.example.order;

import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxPublisher;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Places an order: persists it AND records an {@code OrderPlaced} outbox event in the <b>same
 * transaction</b>. Either both commit or neither does — no dual-write window where an order exists
 * with no event (or an event fires for an order that rolled back).
 *
 * <p>No broker I/O happens here; the outbox poller publishes the recorded event later. The charge
 * counter makes double-execution observable for the idempotency demo.
 */
@Service
public class OrderService {

  private final OrderRepository orders;
  private final OutboxPublisher outboxPublisher;
  private final AtomicInteger charges = new AtomicInteger();

  public OrderService(OrderRepository orders, OutboxPublisher outboxPublisher) {
    this.orders = orders;
    this.outboxPublisher = outboxPublisher;
  }

  @Transactional
  public OrderResponse place(OrderRequest request) {
    charges.incrementAndGet(); // the side effect that must happen exactly once per order
    String orderId = UUID.randomUUID().toString();

    orders.insert(orderId, request);
    outboxPublisher.record(
        "order",
        orderId,
        "OrderPlaced",
        new OrderPlacedEvent(
            orderId, request.customerId(), request.amountPence(), request.currency()));

    return new OrderResponse(orderId, request.customerId(), request.amountPence(), "CONFIRMED");
  }

  public int timesCharged() {
    return charges.get();
  }
}
