# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Spring Boot 3.3 / Java 21 starter that provides two features: request idempotency (`@Idempotent`, backed by a Postgres dedupe table) and the transactional outbox (events written in the business transaction, then published to Kafka by a background poller). `plan.md` holds the architecture and design decisions. `implementation-plan.md` is the staged, as-built record. Stages 0–4 are done. Stage 5 (AWS/Terraform under a future `deploy/` tree) is planned but not built. When you finish a stage or milestone, update both docs in the same as-built style.

## Repository layout: two independent Maven projects

There is **no shared parent POM** and no multi-module reactor. Each project has its own `./mvnw` and uses `spring-boot-starter-parent` 3.3.5 directly:

- `idempotency-outbox-spring-boot-starter/`: the library.
- `examples/example-order-service/`: a demo app that consumes the starter as a plain `0.1.0-SNAPSHOT` dependency **resolved from the local Maven repo**. After changing the starter, run `./mvnw install` in the starter before building the example, or the example builds against a stale jar.

## Commands

Docker must be running. All `*IT` tests use Testcontainers (`postgres:16-alpine`, `confluentinc/cp-kafka:7.7.1`).

```bash
# Starter: unit tests (Surefire) + ITs (Failsafe) + Spotless check + JaCoCo 80% line gate
cd idempotency-outbox-spring-boot-starter && ./mvnw clean verify
./mvnw install -DskipTests          # publish to ~/.m2 for the example

# Example (needs the starter installed first)
cd examples/example-order-service && ./mvnw clean verify

# Formatting: Spotless runs `check` during the build and fails it on violations
./mvnw spotless:apply

# Single unit test
./mvnw test -Dtest=BackoffTest
./mvnw test -Dtest='OutboxPollerTest#someMethod'

# Single integration test (skip unit tests; skip JaCoCo because a partial run fails the 80% gate)
./mvnw verify -Dit.test=OutboxPollerIT -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true

# Run the example locally
docker compose up -d                 # Postgres on localhost:5433 (not 5432), Kafka on 9092
cd examples/example-order-service && ./mvnw spring-boot:run
```

CI (`.github/workflows/`) has path-filtered workflows. The library workflow runs `./mvnw -B verify`. The example workflow installs the starter with `-DskipTests` and then verifies the example, and it also triggers on starter changes. There is also a gitleaks secret scan (`.gitleaks.toml`).

## Architecture

Package root: `io.github.pruthvidhani.idempotencyoutbox`. Each feature defines an SPI interface, and its implementation sits in a sub-package: `idempotency/store/jdbc`, `outbox/store/jdbc`, `outbox/publisher/kafka`.

**Auto-configuration** (`autoconfigure/`, registered in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`):
- `IdempotencyAutoConfiguration` and `OutboxAutoConfiguration` are gated on `idempotency.enabled` / `outbox.enabled` (both default to true). Every bean is `@ConditionalOnMissingBean`, so consumers can override any piece. Keep that convention for new beans.
- Both configurations declare a `Clock` bean under the same name, `idempotencyOutboxClock`, so only one gets registered. Inject `Clock` wherever time matters (TTL, backoff, expiry, lag). Tests use `testsupport/MutableClock` and never sleep on wall-clock time.
- Metrics beans fall back to a throwaway `SimpleMeterRegistry` when the app has no `MeterRegistry`, so the aspect and poller never null-check.
- The outbox uses its own `KafkaTemplate<String,String>` named `outboxKafkaTemplate`, built from `spring.kafka.*` properties. This keeps it from colliding with Boot's `<Object,Object>` template.

**Idempotency path:** `IdempotencyAspect` (`@Around("@annotation(idempotent)")`) works in this order:
1. Resolve the key via SpEL (`IdempotencyKeyResolver`). A missing key fails closed with a 400 (`IdempotencyKeyMissingException`).
2. Hash the `@RequestBody` arg, or the `hashOf` SpEL target, as canonical JSON with SHA-256 (`RequestHasher`).
3. Claim the key atomically with `INSERT ... ON CONFLICT`.
4. The winner runs the method and caches the serialized response. If the method throws, the winner releases the claim.
5. A loser that sent a different hash gets a 409 (`IdempotencyConflictException`). Otherwise the loser replays the cached response, or polls up to `duplicate-wait` for the winner and then throws `IdempotencyInProgressException`.

`web/IdempotencyExceptionAdvice` maps these exceptions to HTTP and only registers in servlet apps. The same annotation also dedupes Kafka consumers via `@Idempotent(key = "#eventId", hashBody = false)`.

**Outbox path:**
1. `OutboxPublisher.record(...)` writes a row. It must be called inside the caller's `@Transactional` and does no broker I/O.
2. `OutboxPollerScheduler` calls `OutboxPoller.pollOnce()` every `poll-interval`.
3. One pass runs in a **single transaction**, because the `FOR UPDATE SKIP LOCKED` row locks from `lockNextBatch` only live inside a transaction. Those locks are also what make multiple poller instances safe.
4. Each event becomes one of:
   - **published**: marked `PUBLISHED`.
   - **failed**: rescheduled with exponential `Backoff`, or marked `DEAD` once `max-attempts` is reached.
   - **stalled**: a later event of an aggregate that already failed in this pass is rescheduled untried, so per-aggregate ordering is preserved.
5. The Kafka key is `aggregateId`, and the topic comes from `TopicResolver` (default `<aggregateType>.events`). The stable event id goes in the `outbox-event-id` header (`OutboxPublisher.EVENT_ID_HEADER`).

Delivery is at-least-once. The consumer-side `@Idempotent` provides the exactly-once effect.

**Schema:** The starter ships Flyway migrations `V1__idempotency_keys.sql` and `V2__outbox_events.sql` in `db/migration`. Consumer apps version their own migrations from `V1000` (see the example) to avoid collisions. Treat shipped migrations as immutable and add new versions instead.

## Testing conventions

- `*Test` files are Surefire unit tests with no containers. `*IT` files are Failsafe Testcontainers tests that run in `verify`.
- ITs extend `testsupport/AbstractPostgresIT`, `AbstractKafkaIT`, or `AbstractPostgresKafkaIT`. These are singleton containers started in static initializers and reaped by Ryuk, with no `stop()` call by design.
- Auto-config wiring is tested with `ApplicationContextRunner` against H2 (`AutoConfigurationSliceTest`). No SQL runs in those tests.
- The JaCoCo gate (80% line coverage, BUNDLE) excludes `**/web/**`, `**/*Configuration.class`, and `OutboxPollerScheduler`. The example module's ITs cover those end to end.
- `OutboxChaosIT` covers the failure-mode guarantees listed in the README's "Delivery semantics" table. Keep that table and the tests consistent.

## Style

Spotless enforces import order, unused-import removal, trailing whitespace, and a final newline. It does not reformat code bodies, so match the existing 2-space Google-style formatting by hand.
