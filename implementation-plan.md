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

## Stage 0 — Scaffolding & CI (milestone M0) — ✅ DONE (as built)

**Goal:** two buildable, independent modules (the standalone starter + a standalone
example consumer) with per-module CI and a reusable Testcontainers base, so every later
stage lands on green infrastructure.

### What was built
1. **No aggregator/parent POM.** Each module is standalone with
   `spring-boot-starter-parent` **3.3.5** as its parent (build-time only, invisible to
   consumers). Java 21 via `java.version`; builds run on a newer local JDK targeting 21.
2. **Two independent module POMs:**
   - `idempotency-outbox-spring-boot-starter` — the whole library in one module;
     coordinates `io.github.pruthvidhani:idempotency-outbox-spring-boot-starter:0.1.0-SNAPSHOT`.
     Package skeleton created as `package-info.java` files per plan.md §3
     (`idempotency/`, `idempotency/store/jdbc/`, `outbox/`, `outbox/store/jdbc/`,
     `outbox/publisher/kafka/`, `web/`, `autoconfigure/`), plus an empty
     `src/main/resources/db/migration/` for Flyway.
     Decision: **`spring-boot-starter-web` is a first-class (non-optional) dependency**
     — initial target is web consumers only.
   - `examples/example-order-service` — independent consumer app
     (`io.github.pruthvidhani.example:example-order-service`), depends on the starter by
     coordinates; bootable `@SpringBootApplication` skeleton + `application.yml` pointing
     at the compose stack. Endpoint/consumer arrive in Stages 1–2.
3. **Build plugins (library):** Failsafe (`*IT` → verify phase), JaCoCo
   (**report-only for now** — the coverage gate threshold is deliberately deferred to
   Stage 1 when real code exists), Spotless with safe rules only (import order, unused
   imports, trailing whitespace, newline at EOF). Example keeps a minimal build
   (spring-boot-maven-plugin only).
4. **Testcontainers base classes** in the library's `testsupport` package:
   `AbstractPostgresIT` (`postgres:16-alpine`) and `AbstractKafkaIT`
   (`ConfluentKafkaContainer`, `confluentinc/cp-kafka:7.7.1`) — singleton-container
   pattern (static init, shared per JVM, reaped by Ryuk; no explicit stop).
   Note: required overriding the parent-managed `testcontainers.version` to **1.20.4**
   (Spring Boot 3.3.5 manages 1.19.8, which predates `ConfluentKafkaContainer`).
5. **`docker-compose.yml`** at repo root: Postgres 16 (host port **5433** — 5432 was
   occupied by an unidentified local service; example config updated to match) and
   single-node **KRaft** Kafka (no Zookeeper) on 9092 with dual listeners
   (host + in-network), healthchecks on both. Same images as the ITs. Validated up →
   healthy → query → down.
6. **Two GitHub Actions workflows** (path-filtered, JDK 21 temurin, Maven cache):
   - `idempotent-library-ci` — `./mvnw -B verify` on the library; uploads the JaCoCo
     report as an artifact.
   - `example-order-service-ci` — installs the starter (`-DskipTests`) then verifies the
     example. Triggers on **example and starter paths** (the example consumes the
     starter's SNAPSHOT, so starter changes can break it); drop the extra trigger once
     the starter is published to a registry.
   **Maven Wrapper** (3.9.9) committed in each module (`mvnw` +
   `maven-wrapper.properties`; the jar is gitignored — the script bootstraps it).
7. **READMEs:** root stub (project pitch, status banner, layout, build/run/CI
   instructions) + example README. **`.gitignore`** added (`target/`, IDE, OS files) and
   15 previously tracked `target/` files untracked via `git rm --cached`.

### Testing done (as built)
- `StarterSmokeTest` (library) and `ExampleOrderServiceSmokeTest` (example) — trivial
  unit tests so Surefire always has work; the example deliberately avoids a
  `@SpringBootTest` context-load until Stage 1 (would need live infra).
- **Two connectivity ITs instead of the planned single combined one** (Java single
  inheritance; also exercises each base class the way real ITs will):
  `PostgresConnectivityIT` (JDBC connect + `select version()`) and `KafkaConnectivityIT`
  (full produce → consume roundtrip).
- Verified locally end-to-end: starter `./mvnw clean install` → BUILD SUCCESS (3 tests);
  example `./mvnw clean verify` resolving the installed SNAPSHOT → BUILD SUCCESS.

### Exit criteria
- [x] `mvn clean verify` green for each module locally (starter, then example).
- [x] Testcontainers spins up Postgres + Kafka (locally; CI expected to match on
      `ubuntu-latest`, which ships Docker).
- [x] Coverage report generated and wired for upload as a CI artifact.
- [ ] **Open:** both pipelines green on GitHub — requires the Stage 0 commit to be
      pushed (workflows have never run yet).

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
- Coverage: JaCoCo report generated from Stage 0; the enforced gate threshold is added
  in Stage 1 once real code exists (a gate on skeleton code would be meaningless).
