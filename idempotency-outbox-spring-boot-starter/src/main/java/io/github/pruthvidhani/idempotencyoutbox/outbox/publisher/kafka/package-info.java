/**
 * Kafka implementation of the {@code EventPublisher} SPI ({@code KafkaEventPublisher}); publishes
 * with Kafka key = {@code aggregateId} to preserve per-aggregate ordering.
 */
package io.github.pruthvidhani.idempotencyoutbox.outbox.publisher.kafka;
