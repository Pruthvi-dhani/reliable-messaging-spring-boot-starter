package io.github.pruthvidhani.example.order;

/** What the customer submits at checkout. */
public record OrderRequest(String customerId, int amountPence, String currency) {}
