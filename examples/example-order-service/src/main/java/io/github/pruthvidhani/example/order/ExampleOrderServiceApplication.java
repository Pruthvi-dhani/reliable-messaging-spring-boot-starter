package io.github.pruthvidhani.example.order;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyConfiguration;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * Example e-commerce order service — exercises both starter features: idempotent {@code POST
 * /orders} and transactional outbox &rarr; Kafka &rarr; idempotent consumer.
 *
 * <p>The {@code @Import}s are interim wiring until the starter ships auto-configuration in Stage 3
 * — after that, the dependency alone suffices.
 */
@SpringBootApplication
@Import({IdempotencyConfiguration.class, OutboxConfiguration.class})
public class ExampleOrderServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(ExampleOrderServiceApplication.class, args);
  }
}
