package io.github.pruthvidhani.idempotencyoutbox.testsupport;

import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * Base class for integration tests that need a real Kafka broker.
 *
 * <p>Same singleton-container pattern as {@link AbstractPostgresIT}: started once, shared across
 * all extending test classes, reaped by Ryuk at JVM exit.
 */
public abstract class AbstractKafkaIT {

  // Never closed by design: singleton container, reaped by Ryuk at JVM exit.
  @SuppressWarnings("resource")
  protected static final ConfluentKafkaContainer KAFKA =
      new ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.1");

  static {
    KAFKA.start();
  }

  protected static String bootstrapServers() {
    return KAFKA.getBootstrapServers();
  }
}
