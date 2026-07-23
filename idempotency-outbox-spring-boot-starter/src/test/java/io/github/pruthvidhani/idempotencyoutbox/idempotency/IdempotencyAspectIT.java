package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pruthvidhani.idempotencyoutbox.autoconfigure.IdempotencyAutoConfiguration;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.AbstractPostgresIT;
import io.github.pruthvidhani.idempotencyoutbox.testsupport.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.web.bind.annotation.RequestBody;

/** End-to-end semantics of {@code @Idempotent} through a real Spring AOP proxy + real Postgres. */
@SpringJUnitConfig(IdempotencyAspectIT.TestConfig.class)
class IdempotencyAspectIT extends AbstractPostgresIT {

  record PaymentRequest(String customerId, int amountPence) {}

  record PaymentResult(String paymentId, int amountPence) {}

  // Test-observable state lives in STATIC fields, not on the service bean: the injected service
  // is a CGLIB proxy, and field access on a proxy hits the proxy's (uninitialized) fields rather
  // than the advised target — only method calls are forwarded.
  static final AtomicInteger executions = new AtomicInteger();
  static final AtomicInteger eventExecutions = new AtomicInteger();
  static volatile boolean failNextCharge;
  static volatile Duration chargeDelay = Duration.ZERO;

  /** Test double for a business service; counts real executions to prove dedupe. */
  static class PaymentService {

    @Idempotent(key = "#headers['Idempotency-Key']", ttl = "24h")
    public PaymentResult charge(Map<String, String> headers, @RequestBody PaymentRequest request)
        throws InterruptedException {
      executions.incrementAndGet();
      if (failNextCharge) {
        failNextCharge = false;
        throw new IllegalStateException("simulated downstream failure");
      }
      if (!chargeDelay.isZero()) {
        Thread.sleep(chargeDelay.toMillis());
      }
      return new PaymentResult(UUID.randomUUID().toString(), request.amountPence());
    }

    /** Broker-consumer shape: void, key is authoritative, no body hash. */
    @Idempotent(key = "#event['eventId']", hashBody = false)
    public void handleEvent(Map<String, String> event) {
      eventExecutions.incrementAndGet();
    }
  }

  @Configuration
  @Import(IdempotencyAutoConfiguration.class)
  static class TestConfig {

    @Bean
    DataSource dataSource() {
      var dataSource = new DriverManagerDataSource(jdbcUrl(), username(), password());
      Flyway.configure().dataSource(dataSource).load().migrate();
      return dataSource;
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
      return new JdbcTemplate(dataSource);
    }

    @Bean
    @Primary
    MutableClock mutableClock() {
      return new MutableClock(Instant.parse("2026-01-01T10:00:00Z"));
    }

    @Bean
    PaymentService paymentService() {
      return new PaymentService();
    }
  }

  @Autowired private PaymentService service;
  @Autowired private MutableClock clock;
  @Autowired private JdbcTemplate jdbc;

  private static final Map<String, String> KEY_A = Map.of("Idempotency-Key", "key-a");

  @BeforeEach
  void resetState() {
    jdbc.update("truncate table idempotency_keys");
    clock.setTo(Instant.parse("2026-01-01T10:00:00Z"));
    executions.set(0);
    eventExecutions.set(0);
    failNextCharge = false;
    chargeDelay = Duration.ZERO;
  }

  @Test
  void firstCallExecutesTheMethod() throws Exception {
    PaymentResult result = service.charge(KEY_A, new PaymentRequest("cust-1", 4999));

    assertThat(result.amountPence()).isEqualTo(4999);
    assertThat(executions).hasValue(1);
  }

  @Test
  void duplicateReplaysCachedResponseWithoutReExecuting() throws Exception {
    PaymentResult first = service.charge(KEY_A, new PaymentRequest("cust-1", 4999));
    PaymentResult replayed = service.charge(KEY_A, new PaymentRequest("cust-1", 4999));

    // Same paymentId proves the cached response was replayed, not regenerated
    assertThat(replayed).isEqualTo(first);
    assertThat(executions).hasValue(1);
  }

  @Test
  void sameKeyDifferentBodyIsRejectedAsConflict() throws Exception {
    service.charge(KEY_A, new PaymentRequest("cust-1", 4999));

    assertThatThrownBy(() -> service.charge(KEY_A, new PaymentRequest("cust-1", 9999)))
        .isInstanceOf(IdempotencyConflictException.class);
    assertThat(executions).hasValue(1);
  }

  @Test
  void headerChangesDoNotCauseConflictsBecauseOnlyTheBodyIsHashed() throws Exception {
    Map<String, String> sameKeyExtraHeaders =
        Map.of("Idempotency-Key", "key-a", "X-Trace-Id", "trace-123");

    PaymentResult first = service.charge(KEY_A, new PaymentRequest("cust-1", 4999));
    PaymentResult replayed =
        service.charge(sameKeyExtraHeaders, new PaymentRequest("cust-1", 4999));

    assertThat(replayed).isEqualTo(first);
    assertThat(executions).hasValue(1);
  }

  @Test
  void missingKeyFailsClosed() {
    assertThatThrownBy(() -> service.charge(Map.of(), new PaymentRequest("cust-1", 4999)))
        .isInstanceOf(IdempotencyKeyMissingException.class);
    assertThat(executions).hasValue(0);
  }

  @Test
  void retryAfterTtlExpiryIsAFreshRequest() throws Exception {
    PaymentResult first = service.charge(KEY_A, new PaymentRequest("cust-1", 4999));

    clock.advanceBy(Duration.ofHours(24).plusMinutes(1));
    PaymentResult second = service.charge(KEY_A, new PaymentRequest("cust-1", 4999));

    assertThat(second.paymentId()).isNotEqualTo(first.paymentId());
    assertThat(executions).hasValue(2);
  }

  @Test
  void failedExecutionReleasesTheKeyForRetry() throws Exception {
    failNextCharge = true;
    assertThatThrownBy(() -> service.charge(KEY_A, new PaymentRequest("cust-1", 4999)))
        .isInstanceOf(IllegalStateException.class);

    // The failed attempt must not have burned the key
    PaymentResult retried = service.charge(KEY_A, new PaymentRequest("cust-1", 4999));

    assertThat(retried.amountPence()).isEqualTo(4999);
    assertThat(executions).hasValue(2);
  }

  @Test
  void voidBrokerStyleHandlerDeduplicatesOnKeyAlone() {
    Map<String, String> event = Map.of("eventId", "evt-1", "payload", "a");
    Map<String, String> duplicateWithDifferentPayload = Map.of("eventId", "evt-1", "payload", "b");

    service.handleEvent(event);
    service.handleEvent(duplicateWithDifferentPayload); // hashBody=false: key alone decides

    assertThat(eventExecutions).hasValue(1);
  }

  @Test
  void concurrentDuplicatesExecuteOnceAndAllReceiveTheSameResponse() throws Exception {
    chargeDelay = Duration.ofMillis(200); // force overlap
    int contenders = 8;
    ExecutorService pool = Executors.newFixedThreadPool(contenders);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<PaymentResult>> futures = new ArrayList<>();
      for (int i = 0; i < contenders; i++) {
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  return service.charge(KEY_A, new PaymentRequest("cust-1", 4999));
                }));
      }
      start.countDown();

      List<PaymentResult> results = new ArrayList<>();
      for (Future<PaymentResult> future : futures) {
        results.add(future.get(30, TimeUnit.SECONDS));
      }

      assertThat(executions).hasValue(1);
      assertThat(results).allSatisfy(result -> assertThat(result).isEqualTo(results.get(0)));
    } finally {
      pool.shutdownNow();
    }
  }
}
