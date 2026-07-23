package io.github.pruthvidhani.example.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Example e-commerce order service — exercises both starter features: idempotent {@code POST
 * /orders} and transactional outbox &rarr; Kafka &rarr; idempotent consumer.
 *
 * <p>The starter's beans are wired entirely by auto-configuration: adding the dependency and a
 * few {@code idempotency.*} / {@code outbox.*} properties is all that is required — no
 * {@code @Import} or manual bean definitions.
 */
@SpringBootApplication
public class ExampleOrderServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(ExampleOrderServiceApplication.class, args);
  }
}
