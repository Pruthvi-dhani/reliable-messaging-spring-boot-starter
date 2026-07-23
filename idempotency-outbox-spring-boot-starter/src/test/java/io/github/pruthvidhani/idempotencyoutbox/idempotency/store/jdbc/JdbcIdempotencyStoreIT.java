package io.github.pruthvidhani.idempotencyoutbox.idempotency.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyRecord;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.AbstractPostgresIT;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.MutableClock;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class JdbcIdempotencyStoreIT extends AbstractPostgresIT {

  private static final Duration TTL = Duration.ofHours(24);

  private static JdbcTemplate jdbc;
  private static MutableClock clock;
  private static JdbcIdempotencyStore store;

  @BeforeAll
  static void setUpDatabase() {
    var dataSource = new DriverManagerDataSource(jdbcUrl(), username(), password());
    Flyway.configure().dataSource(dataSource).load().migrate();
    jdbc = new JdbcTemplate(dataSource);
    clock = new MutableClock(Instant.parse("2026-01-01T10:00:00Z"));
    store = new JdbcIdempotencyStore(jdbc, clock);
  }

  @BeforeEach
  void resetState() {
    jdbc.update("truncate table idempotency_keys");
    clock.setTo(Instant.parse("2026-01-01T10:00:00Z"));
  }

  private boolean claim(String key, String hash) {
    Instant now = clock.instant();
    return store.putInProgress(key, hash, now, now.plus(TTL));
  }

  @Test
  void firstClaimWinsSecondLoses() {
    assertThat(claim("key-1", "hash-a")).isTrue();
    assertThat(claim("key-1", "hash-a")).isFalse();
  }

  @Test
  void claimedKeyIsFoundInProgressWithItsHash() {
    claim("key-1", "hash-a");

    IdempotencyRecord record = store.find("key-1").orElseThrow();

    assertThat(record.status()).isEqualTo(IdempotencyRecord.Status.IN_PROGRESS);
    assertThat(record.requestHash()).isEqualTo("hash-a");
    assertThat(record.responsePayload()).isNull();
    assertThat(record.responseStatus()).isNull();
  }

  @Test
  void completeStoresReplayableResponse() {
    claim("key-1", "hash-a");
    byte[] payload = "{\"orderId\":\"o-1\"}".getBytes(StandardCharsets.UTF_8);

    store.complete("key-1", payload, 201);

    IdempotencyRecord record = store.find("key-1").orElseThrow();
    assertThat(record.status()).isEqualTo(IdempotencyRecord.Status.COMPLETED);
    assertThat(record.responsePayload()).isEqualTo(payload);
    assertThat(record.responseStatus()).isEqualTo(201);
  }

  @Test
  void removeReleasesTheKeyForRetry() {
    claim("key-1", "hash-a");

    store.remove("key-1");

    assertThat(store.find("key-1")).isEmpty();
    assertThat(claim("key-1", "hash-a")).isTrue();
  }

  @Test
  void expiredEntryIsAbsentAndReclaimable() {
    claim("key-1", "hash-a");
    store.complete("key-1", "cached".getBytes(StandardCharsets.UTF_8), 200);

    clock.advanceBy(TTL.plusMinutes(1));

    assertThat(store.find("key-1")).as("expired entry must look absent").isEmpty();
    assertThat(claim("key-1", "hash-b")).as("expired entry must be stealable").isTrue();

    IdempotencyRecord stolen = store.find("key-1").orElseThrow();
    assertThat(stolen.status()).isEqualTo(IdempotencyRecord.Status.IN_PROGRESS);
    assertThat(stolen.requestHash()).isEqualTo("hash-b");
    assertThat(stolen.responsePayload()).as("stale cached response must be cleared").isNull();
  }

  @Test
  void liveEntryIsNotStealable() {
    claim("key-1", "hash-a");

    clock.advanceBy(TTL.minusMinutes(1)); // still within TTL

    assertThat(claim("key-1", "hash-b")).isFalse();
    assertThat(store.find("key-1").orElseThrow().requestHash()).isEqualTo("hash-a");
  }

  @Test
  void deleteExpiredRemovesOnlyExpiredRows() {
    claim("expired-key", "hash-a");
    clock.advanceBy(TTL.plusMinutes(1));
    claim("live-key", "hash-b"); // claimed at the later instant, so still live

    int deleted = store.deleteExpired(clock.instant());

    assertThat(deleted).isEqualTo(1);
    assertThat(jdbc.queryForObject("select count(*) from idempotency_keys", Integer.class))
        .isEqualTo(1);
    assertThat(store.find("live-key")).isPresent();
  }

  @Test
  void exactlyOneConcurrentClaimWins() throws Exception {
    int contenders = 16;
    ExecutorService pool = Executors.newFixedThreadPool(contenders);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Boolean>> results = new java.util.ArrayList<>();
      for (int i = 0; i < contenders; i++) {
        results.add(
            pool.submit(
                () -> {
                  start.await();
                  return claim("contended-key", "hash-a");
                }));
      }
      start.countDown(); // release all contenders at once

      long wins = 0;
      for (Future<Boolean> result : results) {
        if (result.get(30, TimeUnit.SECONDS)) {
          wins++;
        }
      }
      assertThat(wins).isEqualTo(1);
    } finally {
      pool.shutdownNow();
    }
  }
}
