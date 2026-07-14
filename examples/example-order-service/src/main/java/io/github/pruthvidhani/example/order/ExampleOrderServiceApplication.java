package io.github.pruthvidhani.example.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Example e-commerce order service.
 *
 * <p>Stage 0: a bootable skeleton only. The idempotent {@code POST /orders} endpoint arrives in
 * Stage 1 and the outbox &rarr; Kafka &rarr; consumer flow in Stage 2.
 */
@SpringBootApplication
public class ExampleOrderServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(ExampleOrderServiceApplication.class, args);
  }
}
