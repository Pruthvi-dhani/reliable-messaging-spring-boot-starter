package io.github.pruthvidhani.idempotencyoutbox;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Stage 0 smoke test: keeps Surefire honest (something to run on every build) and proves the
 * unit-test toolchain works without needing Docker.
 */
class StarterSmokeTest {

  @Test
  void runsOnJava21OrNewer() {
    assertThat(Runtime.version().feature()).isGreaterThanOrEqualTo(21);
  }
}
