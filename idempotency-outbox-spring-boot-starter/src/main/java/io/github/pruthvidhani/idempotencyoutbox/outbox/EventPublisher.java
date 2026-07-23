package io.github.pruthvidhani.idempotencyoutbox.outbox;

import java.util.Map;

/**
 * Broker-agnostic publish SPI. Default implementation is Kafka ({@code publisher.kafka});
 * alternatives (Rabbit, SNS, a Debezium-style bridge) plug in here.
 */
public interface EventPublisher {

  /**
   * Publishes synchronously; returns only once the broker has acknowledged the message.
   *
   * @param topic destination topic
   * @param key partitioning key (the aggregate id — preserves per-aggregate ordering)
   * @param payload message body (JSON)
   * @param headers message headers (carries the event id for consumer-side dedupe)
   * @throws Exception any failure — the poller treats it as a failed attempt and applies backoff
   */
  void publish(String topic, String key, String payload, Map<String, String> headers)
      throws Exception;
}
