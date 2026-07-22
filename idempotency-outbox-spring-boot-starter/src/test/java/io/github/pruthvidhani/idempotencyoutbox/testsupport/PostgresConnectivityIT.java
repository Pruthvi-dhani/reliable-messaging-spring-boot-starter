package io.github.pruthvidhani.idempotencyoutbox.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Stage 0 infrastructure check: proves {@link AbstractPostgresIT} starts a real Postgres that we
 * can connect to and query. Later stages replace this with real store integration tests.
 */
class PostgresConnectivityIT extends AbstractPostgresIT {

  @Test
  void canConnectAndQuery() throws Exception {
    try (Connection connection = DriverManager.getConnection(jdbcUrl(), username(), password());
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select version()")) {
      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getString(1)).startsWith("PostgreSQL 16");
    }
  }
}
