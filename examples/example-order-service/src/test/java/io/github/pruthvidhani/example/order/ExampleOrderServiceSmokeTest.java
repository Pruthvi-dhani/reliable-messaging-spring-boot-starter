package io.github.pruthvidhani.example.order;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Stage 0 smoke test: keeps Surefire honest without needing infrastructure.
 *
 * <p>A full {@code @SpringBootTest} context-load test arrives once the app wires a datasource and
 * Kafka in Stage 1/2 (it would fail here without those services running).
 */
class ExampleOrderServiceSmokeTest {

  @Test
  void applicationClassIsPresent() {
    assertThat(ExampleOrderServiceApplication.class.getPackageName())
        .isEqualTo("io.github.pruthvidhani.example.order");
  }
}
