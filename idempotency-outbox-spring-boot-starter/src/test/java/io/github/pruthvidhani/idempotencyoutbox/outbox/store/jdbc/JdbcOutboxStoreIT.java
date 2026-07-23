package io.github.pruthvidhani.idempotencyoutbox.outbox.store.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxEvent;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.AbstractPostgresIT;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

class JdbcOutboxStoreIT extends AbstractPostgresIT {

  private static JdbcTemplate jdbc;
  private static JdbcOutboxStore store;
  private static TransactionTemplate tx;
  private static MutableClock clock;

  @BeforeAll
  static void setUpDatabase() {
    var dataSource = new DriverManagerDataSource(jdbcUrl(), username(), password());
    Flyway.configure().dataSource(dataSource).load().migrate();
    jdbc = new JdbcTemplate(dataSource);
    store = new JdbcOutboxStore(jdbc);
    tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    clock = new MutableClock(Instant.parse("2026-01-01T10:00:00Z"));
  }

  @BeforeEach
  void resetState() {
    jdbc.update("truncate table outbox_events");
    clock.setTo(Instant.parse("2026-01-01T10:00:00Z"));
  }

  private OutboxEvent newEvent(String aggregateId) {
    Instant now = clock.instant();
    UUID id = UUID.randomUUID();
    return new OutboxEvent(
        id,
        "order",
        aggregateId,
        "OrderPlaced",
        "{\"orderId\":\"" + aggregateId + "\"}",
        Map.of("outbox-event-id", id.toString()),
        OutboxEvent.Status.PENDING,
        0,
        now,
        now,
        null);
  }

  private OutboxEvent recordEvent(String aggregateId) {
    OutboxEvent event = newEvent(aggregateId);
    tx.executeWithoutResult(status -> store.record(event));
    return event;
  }

  private List<OutboxEvent> lockBatchInTx(int limit) {
    return tx.execute(status -> store.lockNextBatch(limit, clock.instant()));
  }

  @Test
  void recordedEventRoundTripsAllFields() {
    OutboxEvent recorded = recordEvent("order-1");

    List<OutboxEvent> batch = lockBatchInTx(10);

    assertThat(batch).hasSize(1);
    OutboxEvent read = batch.get(0);
    assertThat(read.id()).isEqualTo(recorded.id());
    assertThat(read.aggregateType()).isEqualTo("order");
    assertThat(read.aggregateId()).isEqualTo("order-1");
    assertThat(read.eventType()).isEqualTo("OrderPlaced");
    // jsonb normalizes formatting, so assert content rather than exact string
    assertThat(read.payload()).contains("orderId").contains("order-1");
    assertThat(read.headers()).containsEntry("outbox-event-id", recorded.id().toString());
    assertThat(read.status()).isEqualTo(OutboxEvent.Status.PENDING);
    assertThat(read.attempts()).isZero();
    assertThat(read.createdAt()).isEqualTo(recorded.createdAt());
    assertThat(read.publishedAt()).isNull();
  }

  @Test
  void batchReturnsEventsInInsertionOrder() {
    OutboxEvent first = recordEvent("order-1");
    clock.advanceBy(Duration.ofMillis(10));
    OutboxEvent second = recordEvent("order-2");
    clock.advanceBy(Duration.ofMillis(10));
    OutboxEvent third = recordEvent("order-3");

    List<OutboxEvent> batch = lockBatchInTx(10);

    assertThat(batch).extracting(OutboxEvent::id)
        .containsExactly(first.id(), second.id(), third.id());
  }

  @Test
  void eventsNotYetDueAreNotReturned() {
    OutboxEvent event = recordEvent("order-1");
    tx.executeWithoutResult(
        status ->
            store.reschedule(event.id(), 1, clock.instant().plus(Duration.ofSeconds(30))));

    assertThat(lockBatchInTx(10)).isEmpty();

    clock.advanceBy(Duration.ofSeconds(31));
    assertThat(lockBatchInTx(10)).hasSize(1);
  }

  @Test
  void publishedEventsAreTerminal() {
    OutboxEvent event = recordEvent("order-1");

    tx.executeWithoutResult(status -> store.markPublished(event.id(), clock.instant()));

    assertThat(lockBatchInTx(10)).isEmpty();
    assertThat(store.countPending()).isZero();
  }

  @Test
  void deadEventsAreTerminalAndExcludedFromPendingCount() {
    OutboxEvent event = recordEvent("order-1");
    recordEvent("order-2");

    tx.executeWithoutResult(status -> store.markDead(event.id()));

    List<OutboxEvent> batch = lockBatchInTx(10);
    assertThat(batch).hasSize(1);
    assertThat(batch.get(0).aggregateId()).isEqualTo("order-2");
    assertThat(store.countPending()).isEqualTo(1);
  }

  @Test
  void rescheduleUpdatesAttemptsAndDueTime() {
    OutboxEvent event = recordEvent("order-1");
    Instant nextDue = clock.instant().plus(Duration.ofMillis(400));

    tx.executeWithoutResult(status -> store.reschedule(event.id(), 1, nextDue));

    clock.advanceBy(Duration.ofMillis(400));
    List<OutboxEvent> batch = lockBatchInTx(10);
    assertThat(batch).hasSize(1);
    assertThat(batch.get(0).attempts()).isEqualTo(1);
    assertThat(batch.get(0).nextAttemptAt()).isEqualTo(nextDue);
  }

  @Test
  void concurrentPollersLockDisjointBatches() throws Exception {
    recordEvent("order-1");
    clock.advanceBy(Duration.ofMillis(10));
    recordEvent("order-2");
    clock.advanceBy(Duration.ofMillis(10));
    recordEvent("order-3");

    CountDownLatch firstBatchLocked = new CountDownLatch(1);
    CountDownLatch releaseFirstTx = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      // Poller A: locks 2 rows and HOLDS its transaction open
      Future<List<OutboxEvent>> pollerA =
          pool.submit(
              () ->
                  tx.execute(
                      status -> {
                        List<OutboxEvent> batch = store.lockNextBatch(2, clock.instant());
                        firstBatchLocked.countDown();
                        try {
                          releaseFirstTx.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        return batch;
                      }));

      // Poller B: while A holds its locks, must skip A's rows instead of blocking
      assertThat(firstBatchLocked.await(30, TimeUnit.SECONDS)).isTrue();
      List<OutboxEvent> batchB =
          pool.submit(() -> lockBatchInTx(10)).get(30, TimeUnit.SECONDS);
      releaseFirstTx.countDown();
      List<OutboxEvent> batchA = pollerA.get(30, TimeUnit.SECONDS);

      assertThat(batchA).hasSize(2);
      assertThat(batchB).hasSize(1);
      assertThat(batchB.get(0).id())
          .as("poller B must see only the row poller A did not lock")
          .isNotIn(batchA.stream().map(OutboxEvent::id).toList());
    } finally {
      pool.shutdownNow();
    }
  }
}
