package io.github.pruthvidhani.example.order;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * Example e-commerce order service.
 *
 * <p>Stage 1: idempotent {@code POST /orders}. The outbox &rarr; Kafka &rarr; consumer flow
 * arrives in Stage 2. The {@code @Import} is interim wiring until the starter ships
 * auto-configuration in Stage 3 — after that, the dependency alone suffices.
 */
@SpringBootApplication
@Import(IdempotencyConfiguration.class)
public class ExampleOrderServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(ExampleOrderServiceApplication.class, args);
  }
}
