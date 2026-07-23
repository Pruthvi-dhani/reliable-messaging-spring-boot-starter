package io.github.pruthvidhani.example.order;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Service;

/**
 * Stand-in for the real business logic: "charges the card" and creates the order.
 *
 * <p>The charge counter exists to make double-execution observable — the whole point of the
 * idempotency demo is that retries must NOT increment it. (In Stage 2 this service will also
 * persist the order and record an OrderPlaced outbox event in the same transaction.)
 */
@Service
public class OrderService {

  private final AtomicInteger charges = new AtomicInteger();

  public OrderResponse place(OrderRequest request) {
    charges.incrementAndGet(); // the side effect that must happen exactly once per order
    return new OrderResponse(
        UUID.randomUUID().toString(), request.customerId(), request.amountPence(), "CONFIRMED");
  }

  public int timesCharged() {
    return charges.get();
  }
}
