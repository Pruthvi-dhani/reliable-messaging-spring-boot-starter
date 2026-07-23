package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.store.jdbc.JdbcIdempotencyStore;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.AbstractPostgresIT;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.MutableClock;
import java.time.Duration;
import java.time.Instant;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class IdempotencySweeperIT extends AbstractPostgresIT {

  private static final Duration TTL = Duration.ofHours(24);
  private static final Instant START = Instant.parse("2026-01-01T10:00:00Z");

  private static JdbcTemplate jdbc;
  private static MutableClock clock;
  private static JdbcIdempotencyStore store;
  private static IdempotencySweeper sweeper;

  @BeforeAll
  static void setUp() {
    var dataSource = new DriverManagerDataSource(jdbcUrl(), username(), password());
    Flyway.configure().dataSource(dataSource).load().migrate();
    jdbc = new JdbcTemplate(dataSource);
    clock = new MutableClock(START);
    store = new JdbcIdempotencyStore(jdbc, clock);
    sweeper = new IdempotencySweeper(store, clock, Duration.ofMinutes(5));
  }

  @BeforeEach
  void resetState() {
    jdbc.update("truncate table idempotency_keys");
    clock.setTo(START);
  }

  private void claim(String key) {
    Instant now = clock.instant();
    store.putInProgress(key, "hash", now, now.plus(TTL));
  }

  private int rowCount() {
    Integer count = jdbc.queryForObject("select count(*) from idempotency_keys", Integer.class);
    return count == null ? 0 : count;
  }

  @Test
  void deletesOnlyExpiredEntries() {
    claim("expired-1");
    claim("expired-2");
    clock.advanceBy(TTL.plusMinutes(1)); // the two above are now past expiry
    claim("live-1"); // claimed at the later instant, still within TTL

    int deleted = sweeper.sweep();

    assertThat(deleted).isEqualTo(2);
    assertThat(rowCount()).isEqualTo(1);
    assertThat(store.find("live-1")).isPresent();
  }

  @Test
  void sweepIsANoOpWhenNothingIsExpired() {
    claim("live-1");
    claim("live-2");

    assertThat(sweeper.sweep()).isZero();
    assertThat(rowCount()).isEqualTo(2);
  }
}
