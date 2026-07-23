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

## Stage 1 — Idempotency vertical slice (milestone M1) — ✅ DONE (as built)

**Goal:** a working `@Idempotent` on a real endpoint, backed by Postgres, with replay
correctness, 409-on-different-body, and fail-closed on missing key.

### What was built
1. **`idempotency/` package — annotation & contracts**
   - `@Idempotent(key, ttl, hashBody, hashOf)`. Deviations from the original sketch:
     `include` was dropped; **`hashOf`** (SpEL) was added instead — it selects *what* to
     hash, defaulting to the `@RequestBody` parameter, falling back to all arguments
     (see plan.md §6 decision 2). `ttl` empty → configured default (24h interim).
   - `IdempotencyKeyResolver` — SpEL vs. method args (`MethodBasedEvaluationContext`,
     parsed-expression cache); fails closed on null/blank **and on evaluation errors**
     (dominant runtime cause is a missing client header → 400, not 500). A separate
     `evaluate()` method serves `hashOf`, where failures ARE developer bugs and propagate.
   - `RequestHasher` — canonical JSON (properties alphabetized, map entries sorted, ISO
     dates) → SHA-256 lowercase hex.
   - `IdempotencyStore` SPI: `find`, `putInProgress` (atomic claim), `complete`,
     **`remove`** (added beyond the plan: a failed execution must release the claim so
     the client can retry — otherwise the key is burned), `deleteExpired`.
   - Domain: `IdempotencyRecord` (+ `Status IN_PROGRESS|COMPLETED`), exceptions:
     `IdempotencyConflictException` (409), `IdempotencyKeyMissingException` (400), and
     **`IdempotencyInProgressException`** (409 + `Retry-After`, added for the
     concurrent-duplicate wait-budget-exhausted case, mirroring Stripe).
2. **`idempotency/store/jdbc/` — store impl**
   - Flyway `V1__idempotency_keys.sql` (table + status CHECK + `expires_at` index).
   - `JdbcIdempotencyStore` (JdbcTemplate + injected `Clock`). The claim is
     `INSERT ... ON CONFLICT DO UPDATE ... WHERE expired` — one atomic statement with
     three outcomes: fresh insert wins / live row loses / **expired row is stolen**
     (stale response wiped, re-claimed IN_PROGRESS). The steal clause closes the gap
     where `find()` says absent (expired) but a plain DO-NOTHING insert says taken.
     `find()` filters `expires_at > now`, enforcing expired-equals-absent.
3. **`idempotency/` — the aspect**
   - `IdempotencyAspect` (`@Around`): resolve key → hash target → single **claim loop**:
     claim won → execute + cache (JSON via Jackson; method threw → `remove()` +
     rethrow); claim lost → conflict check first (409), replay if COMPLETED, else poll
     within a wait budget (interim 5s/100ms). The loop re-attempts the claim each pass,
     so a duplicate waiting on a winner that *fails* takes over the freed claim and
     executes — the retry succeeds without a client round-trip.
   - Deviation: the planned "runs inside the request transaction" coupling was not
     needed — the claim row is written before execution and removed on failure, which
     makes the dedupe protocol transaction-independent.
   - Not yet done (carried to later): `response_status` column is not populated; replay
     returns 200 + cached DTO. `ResponseEntity`/custom-status support deferred.
4. **`web/` — advice + wiring**
   - `IdempotencyExceptionAdvice` (`@RestControllerAdvice`) returns **RFC-7807
     ProblemDetail bodies** with explanatory titles/details (per review decision, the
     client always sees *why*): missing key → 400, conflict → 409, in-progress → 409
     with `Retry-After: 1`.
   - `IdempotencyConfiguration` — interim `@Import`-able wiring (clock, resolver,
     hasher, store, aspect, advice); Stage 3 auto-config replaces it.
5. **example-order-service (idempotency slice)**
   - `POST /orders` on `OrderController`, key = `#idempotencyKey` bound via
     `@RequestHeader("Idempotency-Key", required = false) String`. **Pattern
     deliberately changed from the `#headers[...]` map form**: HTTP/2 lowercases header
     names, so a literal map lookup breaks; `@RequestHeader` binding is case-insensitive.
     `required=false` lets the starter's explanatory 400 fire instead of Spring's generic
     error. Document as the recommended pattern.
   - `OrderService` with an observable charge counter; Flyway runs the starter's
     migration on app boot (drop-in schema story proven by the tests).

### Testing done (as built)
- **Unit (13)**: resolver (header extraction, composed keys, absent/blank/unevaluable →
  fail closed) and hasher (determinism, map-order independence, value-change detection,
  args-order sensitivity, hex format, null payload).
- **Store IT (8, real Postgres)**: claim won/lost, in-progress read-back, complete →
  replayable fields, remove releases, expired-absent + steal, live-not-stealable,
  sweeper deletes only expired, 16-thread race → exactly one winner.
- **Aspect IT (9, Spring AOP proxy + Postgres)**: single execution, cached replay (same
  `paymentId`), different-body 409, header-change ≠ conflict (body-only hashing), missing
  key fail-closed, TTL expiry = fresh request (via injected `MutableClock` — no
  sleeping), failed execution releases key, void broker-style `hashBody=false` dedupe,
  8 concurrent duplicates → one execution + identical responses for all callers.
- **Web IT (4, example module, MockMvc + `@ServiceConnection` Testcontainers)**: 200 +
  replay with exactly one charge, 409 ProblemDetail with explanatory detail, 400
  ProblemDetail for missing header, independent keys → independent orders.
- **Coverage gate now enforced**: JaCoCo `check` at 80% line on the library bundle
  (`web/` excluded — the advice is exercised end-to-end by the example's ITs).
- Debugging notes for the record: two test bugs found and fixed en route (an
  `invokeAll`/latch deadlock in the store concurrency test; direct field access on a
  CGLIB-proxied bean in the aspect IT — fields aren't forwarded to the target, which
  incidentally validates the documented self-invocation/proxy caveat).

### Exit criteria
- [x] All idempotency semantics pass against a real Postgres container (17 ITs library,
      4 ITs example).
- [x] `example-order-service` order-placement endpoint demonstrably dedupes retries
      end-to-end (same confirmation replayed, charge counter unchanged).
- [x] Coverage gate active and green (≥80% line).

---

## Stage 2 — Outbox vertical slice (milestone M2) — ✅ DONE (as built)

**Goal:** events written in the business transaction reach Kafka in per-aggregate order,
with retry + dead-lettering, and provably no publish on the request path.

### What was built
1. **`outbox/` package — contracts**
   - `OutboxEvent` record (+ `Status PENDING|PUBLISHED|DEAD`); `OutboxStore` SPI: `record`,
     `lockNextBatch(limit, now)`, `markPublished`, `reschedule`, `markDead`, `countPending`.
   - `EventPublisher` SPI (broker-agnostic, synchronous ack-before-return).
   - `OutboxPublisher` façade: `record(aggregateType, aggregateId, eventType, payload)`.
     Mints a stable event id **at record time**, carried as the `outbox-event-id` message
     header (+ `outbox-event-type`) so consumers dedupe on it; serializes the payload to
     JSON and **fails fast** in the business TX if it can't. First attempt due immediately.
   - `Backoff` (exponential ×2, capped, overflow-guarded) and `TopicResolver` (default
     convention `<aggregateType>.events`).
2. **`outbox/store/jdbc/` — store impl**
   - Flyway `V2__outbox_events.sql`. **Deviation:** publish order is a DB-assigned
     `seq bigint generated always as identity`, **not `created_at`** — timestamps collide
     within a millisecond under load and would then order randomly, silently breaking
     per-aggregate ordering (the end-to-end IT caught exactly this). Index is
     `(status, next_attempt_at, seq)`.
   - `JdbcOutboxStore`; `lockNextBatch` = `... WHERE status='PENDING' AND next_attempt_at
     <= ? ORDER BY seq LIMIT ? FOR UPDATE SKIP LOCKED`. Pure (timestamps passed in, no
     Clock). `jsonb` payload/headers.
3. **`outbox/publisher/kafka/` — publisher impl**
   - `KafkaEventPublisher` on `KafkaTemplate`; Kafka key = `aggregateId` (delegates
     per-aggregate ordering to Kafka's per-partition total order); copies headers;
     synchronous with a bounded ack wait; any failure (incl. timeout) throws → poller
     retries.
4. **`outbox/` — the poller**
   - `OutboxPoller.pollOnce()` runs one batch in a single transaction (the SKIP LOCKED
     locks live only that long — also what makes multiple instances safe). Per event:
     publish → `markPublished`; failure → backoff `reschedule` or `markDead` at
     `maxAttempts`. **Added beyond the plan — per-aggregate stalling:** when an aggregate's
     event fails, its *later* events in the same batch are rescheduled **untried** (attempts
     unchanged) so a retry can't overtake the failed event and reorder the aggregate.
   - `OutboxPollerScheduler` — `SmartLifecycle` fixed-delay loop that swallows/logs any
     throwable so it survives transient DB/broker outages.
   - `OutboxConfiguration` — interim `@Import`-able wiring (replaced by Stage 3 auto-config).
5. **example-order-service (outbox slice)**
   - `OrderService.place` is now `@Transactional`: inserts the order (own `orders` table,
     `V1000` migration) **and** `outboxPublisher.record("order", orderId, "OrderPlaced", …)`
     in one transaction.
   - `OrderPlacedConsumer` — `@KafkaListener` + `@Idempotent(key = "#eventId",
     hashBody = false)` keyed on the event-id header, closing the exactly-once-effect loop.

### Testing done (as built)
- **Unit (15)**: `Backoff` (doubling, monotonic, capped, validation); `OutboxPublisher`
  (event construction, header contract, distinct ids, fail-fast serialization, topic
  convention); `OutboxPoller` state machine with mocked store/publisher (publish→mark,
  backoff grows with attempts, dead-letter at max, **per-aggregate stall vs. unaffected
  aggregate**, empty batch).
- **Store IT (7, real Postgres, real transactions)**: field round-trip through `jsonb`,
  insertion-order batches, not-yet-due excluded, published/dead terminal, reschedule
  updates attempts+due, and **two concurrent pollers holding separate transactions lock
  disjoint batches** (SKIP LOCKED — no double-grab, no blocking).
- **Kafka publisher IT (3, real broker)**: payload/key/headers roundtrip; **4-partition
  topic → same aggregate id lands in one partition in order** (vacuous on 1 partition);
  unreachable broker → exception.
- **Poller IT (3, Postgres + Kafka)**: record-in-TX → poll → consumed in per-aggregate
  order with event-id headers (`countPending()==3` before the poll proves no
  request-path publish); transient failure → backoff → retry succeeds; permanent failure
  → `DEAD` after max attempts.
- **Example outbox IT (3, full app context, Postgres + Kafka)**: order → Kafka →
  consumed **exactly once**; **atomic rollback** (order + outbox both vanish on failure —
  no dual-write window); **duplicate delivery** (same event id twice) → consumer
  processes once.
- Coverage gate extended: `web/`, interim `*Configuration`, and `OutboxPollerScheduler`
  excluded (all exercised by the example module's ITs, not library-local tests).
- Debugging notes: `@ServiceConnection` doesn't support `ConfluentKafkaContainer` on Boot
  3.3.5 → used `@DynamicPropertySource` for Kafka; Mockito can't spy concrete classes on
  Java 25 (ByteBuddy lag) → the rollback test uses a real `TransactionTemplate` instead of
  `@SpyBean` (a more faithful atomicity proof anyway).

### Exit criteria
- [x] Business rollback leaves no outbox row; committed writes always publish.
- [x] Retry + dead-letter paths verified against real Kafka.
- [x] Per-aggregate ordering holds (proven via multi-partition topic + DB sequence).
- [x] `example-order-service` shows order → outbox → Kafka → idempotent consumer,
      deduping duplicate deliveries to exactly-once effect.

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
