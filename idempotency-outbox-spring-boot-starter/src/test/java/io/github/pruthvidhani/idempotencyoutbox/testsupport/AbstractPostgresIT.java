package io.github.pruthvidhani.idempotencyoutbox.testsupport;

import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base class for integration tests that need a real Postgres.
 *
 * <p>Uses the singleton-container pattern: the container is started once (static initializer) and
 * shared by every test class extending this base, instead of paying the startup cost per class.
 * Testcontainers' Ryuk sidecar reaps the container when the JVM exits, so there is deliberately no
 * stop() call.
 */
public abstract class AbstractPostgresIT {

  // Never closed by design: singleton container, reaped by Ryuk at JVM exit.
  @SuppressWarnings("resource")
  protected static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("testdb")
          .withUsername("test")
          .withPassword("test");

  static {
    POSTGRES.start();
  }

  protected static String jdbcUrl() {
    return POSTGRES.getJdbcUrl();
  }

  protected static String username() {
    return POSTGRES.getUsername();
  }

  protected static String password() {
    return POSTGRES.getPassword();
  }
}
