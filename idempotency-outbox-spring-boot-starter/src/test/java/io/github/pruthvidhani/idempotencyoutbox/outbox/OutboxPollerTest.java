package io.github.pruthvidhani.idempotencyoutbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.pruthvidhani.idempotencyoutbox.testsupport.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

class OutboxPollerTest {

  private static final Instant START = Instant.parse("2026-01-01T10:00:00Z");
  private static final Duration BASE = Duration.ofMillis(200);

  private final OutboxStore store = mock(OutboxStore.class);
  private final EventPublisher publisher = mock(EventPublisher.class);
  private final OutboxMetrics metrics = mock(OutboxMetrics.class);
  private final MutableClock clock = new MutableClock(START);
  private final OutboxPoller poller =
      new OutboxPoller(
          store,
          publisher,
          TopicResolver.byAggregateType(),
          TransactionOperations.withoutTransaction(),
          clock,
          new Backoff(BASE, Duration.ofSeconds(30)),
          100,
          3, // maxAttempts
          metrics);

  private OutboxEvent event(String aggregateId, int attempts) {
    return new OutboxEvent(
        UUID.randomUUID(),
        "order",
        aggregateId,
        "OrderPlaced",
        "{}",
        Map.of("outbox-event-id", "x"),
        OutboxEvent.Status.PENDING,
        attempts,
        START,
        START,
        null);
  }

  @BeforeEach
  void batchIsEmptyByDefault() {
    when(store.lockNextBatch(anyInt(), any())).thenReturn(List.of());
  }

  @Test
  void publishesEachEventAndMarksItPublished() throws Exception {
    OutboxEvent event = event("order-1", 0);
    when(store.lockNextBatch(anyInt(), any())).thenReturn(List.of(event));

    int published = poller.pollOnce();

    assertThat(published).isEqualTo(1);
    verify(publisher).publish("order.events", "order-1", "{}", event.headers());
    verify(store).markPublished(event.id(), clock.instant());
    verify(metrics).recordPublished(any());
  }

  @Test
  void failureReschedulesWithExponentialBackoff() throws Exception {
    OutboxEvent firstFailure = event("order-1", 0);
    when(store.lockNextBatch(anyInt(), any())).thenReturn(List.of(firstFailure));
    doThrow(new RuntimeException("broker down"))
        .when(publisher)
        .publish(anyString(), anyString(), anyString(), anyMap());

    assertThat(poller.pollOnce()).isZero();

    // attempts 0 -> 1, due after base delay
    verify(store).reschedule(firstFailure.id(), 1, START.plus(BASE));
    verify(store, never()).markDead(any());
    verify(store, never()).markPublished(any(), any());
    verify(metrics).recordRetried();
    verify(metrics, never()).recordPublished(any());
  }

  @Test
  void backoffGrowsWithPriorAttempts() throws Exception {
    OutboxEvent thirdFailure = event("order-1", 1); // one attempt already made
    when(store.lockNextBatch(anyInt(), any())).thenReturn(List.of(thirdFailure));
    doThrow(new RuntimeException("still down"))
        .when(publisher)
        .publish(anyString(), anyString(), anyString(), anyMap());

    poller.pollOnce();

    verify(store).reschedule(thirdFailure.id(), 2, START.plus(BASE.multipliedBy(2)));
  }

  @Test
  void exhaustedRetriesDeadLetterTheEvent() throws Exception {
    OutboxEvent lastChance = event("order-1", 2); // maxAttempts=3 → this failure is final
    when(store.lockNextBatch(anyInt(), any())).thenReturn(List.of(lastChance));
    doThrow(new RuntimeException("permanently broken"))
        .when(publisher)
        .publish(anyString(), anyString(), anyString(), anyMap());

    poller.pollOnce();

    verify(store).markDead(lastChance.id());
    verify(store, never()).reschedule(any(), anyInt(), any());
    verify(metrics).recordDead();
  }

  @Test
  void failedAggregateStallsItsLaterEventsButNotOtherAggregates() throws Exception {
    OutboxEvent failing = event("order-1", 0);
    OutboxEvent mustStall = event("order-1", 0); // same aggregate, later in batch
    OutboxEvent unaffected = event("order-2", 0); // different aggregate
    when(store.lockNextBatch(anyInt(), any()))
        .thenReturn(List.of(failing, mustStall, unaffected));
    doThrow(new RuntimeException("fail order-1"))
        .when(publisher)
        .publish(anyString(), eq("order-1"), anyString(), anyMap());

    int published = poller.pollOnce();

    assertThat(published).isEqualTo(1);
    // failing: real attempt → attempts incremented
    verify(store).reschedule(failing.id(), 1, START.plus(BASE));
    // order-1 saw exactly ONE publish attempt (the failing head) — mustStall was never tried
    verify(publisher).publish(anyString(), eq("order-1"), anyString(), anyMap());
    verify(store).reschedule(mustStall.id(), 0, START.plus(BASE));
    // order-2 unaffected
    verify(store).markPublished(unaffected.id(), clock.instant());
  }

  @Test
  void emptyBatchDoesNothing() {
    assertThat(poller.pollOnce()).isZero();
  }
}
