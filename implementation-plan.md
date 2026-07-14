# Implementation Plan — Staged Delivery

Detailed, stage-by-stage build plan derived from the milestones in
[plan.md](plan.md) §9. Each stage lists **concrete implementation steps**, a
**testing plan for the changes in that stage**, and **exit criteria** that must be
green before moving on.

Guiding principle: every stage ends with something *tested and demonstrable* — no
stage leaves untested code behind. The library-side work in Stages 1 and 2 is
independent (idempotency vs. outbox touch different code paths), but both surface
through the one `example-order-service` app — Stage 2 extends the same `POST /orders`
endpoint Stage 1 introduces, so build Stage 1 first for the shared example.

---

## Stage 0 — Scaffolding & CI (milestone M0)

**Goal:** two buildable, independent modules (the standalone starter + a standalone
example consumer) with per-module CI and a reusable Testcontainers base, so every later
stage lands on green infrastructure.

### Implementation steps
1. **No aggregator/parent POM.** Each module is standalone and uses
   `spring-boot-starter-parent` for dependency + plugin version management (build-time
   only, invisible to consumers of the published starter).
2. Create two independent module POMs:
   - `idempotency-outbox-spring-boot-starter` — **the whole library in one module**, with
     the package layout from plan.md §3 (`idempotency/`, `outbox/`, `web/`,
     `autoconfigure/`, plus `store/jdbc` and `publisher/kafka` impl sub-packages).
   - `examples/example-order-service` — independent consumer app that depends on the
     starter by coordinates (like a real customer); exercises both features (idempotent
     order placement + outbox → Kafka → idempotent consumer).
3. Add build plugins in the **library** module: Failsafe (`*IT` in the verify phase;
   Surefire `*Test` and release 21 come from the Spring parent), JaCoCo coverage,
   Spotless/formatter. The example keeps a minimal build (spring-boot-maven-plugin).
4. Add Testcontainers + JUnit 5 to the library module's test scope; write an abstract
   `AbstractPostgresIT` and `AbstractKafkaIT` base class (singleton containers, reused
   across the module's tests).
5. `docker-compose.yml` at repo root (Postgres + Kafka + Zookeeper/KRaft) for local
   example runs.
6. **Two independent GitHub Actions workflows** (per-module, path-filtered): each does
   checkout → set up JDK 21 → `mvn verify` → upload coverage; cache Maven deps; run on
   push + PR. The example's pipeline resolves the starter from the local Maven repo
   (install the starter first) until it is published to a registry.
7. Root `README.md` stub + a short README in the example module.

### Testing plan
- A trivial `SmokeTest` in each module (asserts context/utility loads) so Surefire has
  something to run and CI proves the wiring.
- One integration test that **starts the Postgres container and the Kafka container**
  and asserts connectivity — proves the Testcontainers base classes work in CI.
- CI must pass on a throwaway PR before Stage 0 is considered done.

### Exit criteria
- `mvn clean verify` green for **each** module (starter, then example) locally and in
  their respective GitHub Actions pipelines.
- Testcontainers spins up Postgres + Kafka in CI.
- Coverage report published as a CI artifact.

---

## Stage 1 — Idempotency vertical slice (milestone M1)

**Goal:** a working `@Idempotent` on a real endpoint, backed by Postgres, with replay
correctness, 409-on-different-body, and fail-closed on missing key.

### Implementation steps
1. **`idempotency/` package — annotation & contracts**
   - `@Idempotent(key, ttl, hashBody, include)` annotation.
   - `IdempotencyKeyResolver` — evaluates the SpEL expression against method args
     (`MethodBasedEvaluationContext`), stringifies the result, fails closed on
     null/blank.
   - `RequestHasher` — canonicalize target payload (stable JSON) → SHA-256 hex.
   - `IdempotencyStore` SPI: `find(key)`, `putInProgress(key, hash)`,
     `complete(key, response, status)`, `deleteExpired(now)`.
   - Domain records: `IdempotencyRecord(key, hash, payload, status, createdAt, expiresAt)`,
     `IdempotencyConflictException`, `IdempotencyKeyMissingException`.
2. **`idempotency/store/jdbc/` package — store impl**
   - Flyway migration `V1__idempotency_keys.sql` (table + `expires_at` index) per the
     schema in plan.md §4.1.
   - `JdbcIdempotencyStore` implementing the SPI; use `INSERT ... ON CONFLICT DO NOTHING`
     to make the in-progress insert the concurrency gate.
3. **`idempotency/` package — the aspect**
   - `IdempotencyAspect` (`@Around` on `@Idempotent`) implementing the plan.md §4.1
     flow: resolve key → hash → lookup → hit(same)/hit(diff→409)/miss(run+persist).
   - Runs inside the caller's transaction (`@Transactional` propagation documented).
   - Concurrent-duplicate handling: on unique-violation from the in-progress insert,
     treat as a racing duplicate (short retry/wait then read the completed row).
4. **`web/` package (minimal wiring for this stage)**
   - `@ControllerAdvice` mapping `IdempotencyConflictException → 409` and
     `IdempotencyKeyMissingException → 400`.
   - Wire aspect + store beans (full auto-config comes in Stage 3; a manual `@Bean`
     config is fine here).
5. **example-order-service (idempotency slice)**
   - `POST /orders` with `@Idempotent(key = "#headers['Idempotency-Key']", ttl="24h")`
     returning an `OrderResponse`; in-memory "charge" side effect with a counter to prove
     single execution on retry. (The outbox half of this example lands in Stage 2.)

### Testing plan
- **Unit**
  - `IdempotencyKeyResolver`: header extraction, composed expressions, null → throws.
  - `RequestHasher`: determinism (same body → same hash; reordered JSON fields → same
    hash; changed value → different hash).
- **Integration (Postgres Testcontainer)**
  - First call executes the method once and caches; side-effect counter == 1.
  - Duplicate call (same key, same body) returns the **cached response**, counter still 1.
  - Same key + **different body** → `IdempotencyConflictException` / HTTP 409.
  - **Missing** `Idempotency-Key` → HTTP 400 (fail closed).
  - **TTL expiry**: advance clock past `expires_at` (inject a `Clock`), retry treated as
    a fresh request; counter increments.
  - **Concurrency**: fire N parallel identical requests (executor + latch) → method runs
    exactly once, all callers get the same response.
- **Web-layer** (`@WebMvcTest` or full `@SpringBootTest`) on the example endpoint
  covering the 200/409/400 status mapping.

### Exit criteria
- All idempotency semantics above pass against a real Postgres container.
- `example-order-service` order-placement endpoint demonstrably dedupes retries end-to-end.

---

## Stage 2 — Outbox vertical slice (milestone M2)

**Goal:** events written in the business transaction reach Kafka in per-aggregate order,
with retry + dead-lettering, and provably no publish on the request path.

### Implementation steps
1. **`outbox/` package — contracts**
   - `OutboxEvent` record + `OutboxStore` SPI: `record(event)`, `pollBatch(limit)`
     (locks rows), `markPublished(id)`, `reschedule(id, nextAttemptAt, attempts)`,
     `markDead(id)`.
   - `EventPublisher` SPI (broker-agnostic): `publish(topic, key, payload, headers)`.
   - `OutboxPublisher` façade the business code calls: `record(aggregateType,
     aggregateId, eventType, payload)` — inserts via the store, no broker I/O.
   - `Backoff` helper (exponential base + cap) and `TopicResolver` strategy.
2. **`outbox/store/jdbc/` package — store impl**
   - Flyway migration `V2__outbox_events.sql` (table + `(status, next_attempt_at)` index)
     per plan.md §4.2.
   - `JdbcOutboxStore`; `pollBatch` uses `SELECT ... FOR UPDATE SKIP LOCKED`
     ordered by `created_at`, to support multiple poller instances.
3. **`outbox/publisher/kafka/` package — publisher impl**
   - `KafkaEventPublisher` implementing `EventPublisher`; Kafka key = `aggregate_id`
     for per-aggregate ordering; maps headers.
4. **`outbox/` package — the poller**
   - `OutboxPoller` (scheduled loop / dedicated thread): poll batch → publish each →
     `markPublished` on success; on failure increment attempts, `reschedule` via backoff;
     at `attempts >= max` → `markDead`. Emit log/metric on dead-letter.
5. **example-order-service (outbox slice)**
   - Extend the same `POST /orders`: within one `@Transactional`, insert the order **and**
     `outboxPublisher.record("order", orderId, "OrderPlaced", payload)`.
   - A `@KafkaListener` consumer that records received events (for the test assertions),
     itself `@Idempotent(key = "#event.eventId")` to demonstrate both halves composing
     into exactly-once effect within a single example.

### Testing plan
- **Unit**
  - `Backoff` schedule math (monotonic, capped).
  - `OutboxPoller` state machine with a mocked store + publisher: success →
    `markPublished`; failure → `reschedule` with incremented attempts; max → `markDead`.
  - `TopicResolver` mapping.
- **Integration (Postgres + Kafka Testcontainers)**
  - **Atomic write**: business insert + outbox insert commit together; if the business TX
    rolls back, **no** outbox row exists.
  - **No publish on request path**: immediately after the request returns, the row is
    `PENDING` and Kafka has nothing yet; after the poller runs it's `PUBLISHED` and the
    consumer received it.
  - **Per-aggregate ordering**: record several events for the same `aggregate_id`; assert
    the consumer sees them in insertion order.
  - **Retry**: make the publisher fail K times (test double / broker paused) → row
    retries with growing backoff, then succeeds and is `PUBLISHED`.
  - **Dead-letter**: force permanent failure → row ends `DEAD` after `max-attempts`,
    dead-letter metric/log emitted.
  - **Multi-instance safety**: run two pollers against one DB → `SKIP LOCKED` prevents
    double-publish (assert each event delivered once under normal operation).

### Exit criteria
- Business rollback leaves no outbox row; committed writes always publish.
- Retry + dead-letter paths verified against real Kafka.
- `example-order-service` shows order → outbox → Kafka → idempotent consumer end-to-end.

---

## Stage 3 — Auto-configuration, metrics, sweeper, chaos (milestone M3)

**Goal:** make it a true "drop-in" starter, add observability + expiry reclamation, and
prove no event loss under failure.

### Implementation steps
1. **`autoconfigure/` package — auto-configuration**
   - `IdempotencyAutoConfiguration` and `OutboxAutoConfiguration` classes registered via
     `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
   - `@ConditionalOnClass` / `@ConditionalOnProperty` gating; sensible bean defaults with
     `@ConditionalOnMissingBean` so consumers can override.
   - `@ConfigurationProperties` binding the full `idempotency.*` / `outbox.*` tree from
     plan.md §5; generate metadata (`spring-configuration-metadata.json`).
   - Flyway migrations shipped in the library module's `db/migration` with a documented
     opt-out for teams managing their own schema.
2. **metrics (Micrometer)**
   - Idempotency: hit/miss/conflict counters, dedupe hit-rate gauge.
   - Outbox: publish latency timer, publish/retry/dead counters, **lag** gauge
     (pending count and age of oldest pending event, in events and seconds).
3. **sweeper**
   - Scheduled `IdempotencySweeper` deleting rows past `expires_at` on
     `idempotency.sweeper.interval`.
4. **chaos test harness**
   - Test control to kill/restart the poller (or pause the Kafka container) mid-batch.

### Testing plan
- **Auto-config slice tests** (`ApplicationContextRunner`)
  - Beans present when properties enabled; **absent** when `enabled=false`.
  - Consumer-provided `@Bean` overrides the default (`@ConditionalOnMissingBean`).
  - Property binding: custom `poll-interval`, `batch-size`, `max-attempts`, `default-ttl`
    land on the right beans.
- **Metrics tests**: drive hits/misses/failures, assert the expected meters exist with
  correct values in a `SimpleMeterRegistry`.
- **Sweeper test** (Postgres): seed expired + live rows, run sweeper, assert only expired
  rows removed.
- **Chaos test (the headline test)**: publish a known set of N events; kill the publisher
  mid-batch and restart (and separately: drop+restore the Kafka container); assert every
  event eventually lands on Kafka **at-least-once** (no loss) and consumer-side
  `@Idempotent` collapses any duplicates to exactly-once *effect*.
- **Starter smoke**: a minimal app depending only on the starter + a datasource boots and
  both features work with zero manual bean config.

### Exit criteria
- A bare app with just the starter dependency + properties gets both features.
- Chaos test proves no event loss across publisher/broker failure.
- Metrics visible for dedupe hit rate and outbox lag/latency.

---

## Stage 4 — Docs & benchmarks (milestone M4)

**Goal:** ship the senior-IC write-up and the numbers that back the resume bullet.

### Implementation steps
1. **README**
   - Architecture diagram (idempotency path + outbox path).
   - Delivery-semantics section: at-least-once guarantee, assumptions, degradation per
     failure mode (plan.md §6.6).
   - Decision log covering all six items in plan.md §6.
   - Quick-start: dependency + properties + minimal example.
2. **Benchmark harness**
   - A load driver (e.g. k6/JMeter or a small JMH/HTTP client) hitting the example apps
     under `docker-compose`.
   - Measure: request throughput, p50/p99 latency (idempotent endpoint, hit vs. miss),
     dedupe hit rate, outbox publish latency, outbox lag under sustained load.
   - Capture results into a README benchmark table with the test environment noted.

### Testing plan
- Benchmark scripts are **repeatable**: a `make bench` / script that stands up compose,
  runs the load, and prints the table — re-runnable in CI (smoke scale) and locally
  (full scale).
- A CI job runs the benchmark at small scale purely to guard against the harness rotting
  (not for performance gating).
- Doc examples are compile-checked (the quick-start snippet matches a real example app).

### Exit criteria
- README complete with diagram, semantics, decision log, and a populated benchmark table.
- Benchmark harness re-runnable via a single command.

---

## Stage 5 — Stretch (post-MVP, as time allows)

Each stretch item is independently shippable and follows the same "code + tests" rule.

1. **LISTEN/NOTIFY tailer** — Postgres notification on outbox insert wakes the poller for
   lower latency. *Tests:* latency improvement vs. polling; correctness unchanged;
   fallback to polling when a notification is missed.
2. **Debezium outbox bridge** — swappable CDC publisher behind the `EventPublisher`/poll
   abstraction. *Tests:* same delivery-semantics suite runs against the CDC path.
3. **Pluggable dedupe SPI — Redis/DynamoDB impl** — second `IdempotencyStore`. *Tests:*
   run the Stage 1 idempotency suite against the alternative store (shared contract test).
4. **Docs site** (mkdocs/Docusaurus) with a "five common pitfalls" page. *Tests:* docs
   build in CI; internal links checked.

---

## Cross-cutting testing conventions
- **Unit tests** (`*Test`, Surefire) — no containers, fast, run on every build.
- **Integration tests** (`*IT`, Failsafe) — Testcontainers, run in `verify`.
- **Shared contract tests** — SPI conformance suites reused by every store/publisher impl
  (pays off directly for the Stage 5 alternative implementations).
- **Deterministic time** — inject `java.time.Clock` everywhere TTL/backoff/expiry matter,
  so tests never sleep on wall-clock.
- Coverage gate enforced in CI from Stage 0 onward.
