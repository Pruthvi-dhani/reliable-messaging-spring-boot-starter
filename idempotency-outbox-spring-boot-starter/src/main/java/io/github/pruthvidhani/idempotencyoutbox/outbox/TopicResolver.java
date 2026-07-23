package io.github.pruthvidhani.idempotencyoutbox.outbox;

/** Maps an outbox event to its destination topic. */
@FunctionalInterface
public interface TopicResolver {

  String topicFor(OutboxEvent event);

  /**
   * Convention default: {@code <aggregateType>.events} — e.g. aggregate type {@code "order"}
   * publishes to topic {@code "order.events"}.
   */
  static TopicResolver byAggregateType() {
    return event -> event.aggregateType() + ".events";
  }
}
