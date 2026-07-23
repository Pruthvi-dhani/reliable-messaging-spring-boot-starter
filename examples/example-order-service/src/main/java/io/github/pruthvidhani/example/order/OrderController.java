package io.github.pruthvidhani.example.order;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.Idempotent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Idempotent order placement. The client supplies an {@code Idempotency-Key} header per checkout
 * attempt; retries with the same key replay the original confirmation instead of charging twice.
 *
 * <p>{@code required = false} on the header lets the starter's fail-closed handling produce the
 * explanatory 400 (rather than Spring's generic missing-header error).
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

  private final OrderService orderService;

  public OrderController(OrderService orderService) {
    this.orderService = orderService;
  }

  @PostMapping
  @Idempotent(key = "#idempotencyKey", ttl = "24h")
  public OrderResponse place(
      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
      @RequestBody OrderRequest request) {
    return orderService.place(request);
  }
}
