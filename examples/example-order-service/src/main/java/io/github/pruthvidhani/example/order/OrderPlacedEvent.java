package io.github.pruthvidhani.example.order;

/** The event published to Kafka when an order is placed. Carries the stable event id in headers. */
public record OrderPlacedEvent(String orderId, String customerId, int amountPence, String currency) {}
