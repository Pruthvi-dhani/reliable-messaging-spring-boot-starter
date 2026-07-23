package io.github.pruthvidhani.example.order;

/** Confirmation returned to the customer; replayed as-is on idempotent retries. */
public record OrderResponse(String orderId, String customerId, int amountPence, String status) {}
