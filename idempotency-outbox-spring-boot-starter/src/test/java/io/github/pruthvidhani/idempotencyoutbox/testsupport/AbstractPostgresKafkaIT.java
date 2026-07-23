package io.github.pruthvidhani.idempotencyoutbox.testsupport;

/**
 * Base for end-to-end tests needing BOTH Postgres and Kafka. Java allows one superclass, so this
 * extends {@link AbstractPostgresIT} and reaches the Kafka singleton via same-package access —
 * touching {@code AbstractKafkaIT.KAFKA} triggers its static initializer (container start) on
 * first use, exactly like inheriting from it would.
 */
public abstract class AbstractPostgresKafkaIT extends AbstractPostgresIT {

  protected static String kafkaBootstrapServers() {
    return AbstractKafkaIT.KAFKA.getBootstrapServers();
  }
}
