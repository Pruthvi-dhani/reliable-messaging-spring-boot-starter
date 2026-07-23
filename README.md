# Idempotency + Transactional Outbox — Spring Boot Starter

A drop-in Spring Boot starter for two near-universal backend problems:

1. **Request idempotency** — safely absorb client retries with an `@Idempotent`
   annotation backed by a Postgres dedupe table (client-supplied `Idempotency-Key`
   header, cached-response replay, `409` on same-key/different-body).
2. **Reliable event publishing** — the transactional outbox pattern: events are written
   in the same database transaction as the business change, then published to Kafka by a
   background poller with retry, backoff, and dead-lettering. At-least-once delivery;
   combined with consumer-side `@Idempotent`, exactly-once *effect*.

> **Status: under construction.** Both core features are implemented and fully tested:
> **request idempotency** (`@Idempotent` — SpEL keys, canonical-JSON hashing, Postgres
> dedupe store, cached-response replay, ProblemDetail 400/409) and the **transactional
> outbox** (record-in-transaction, `FOR UPDATE SKIP LOCKED` poller, Kafka publishing keyed
> by aggregate for per-aggregate ordering, exponential-backoff retry + dead-lettering).
> The example app demonstrates both end to end — idempotent `POST /orders` plus outbox →
> Kafka → `@Idempotent` consumer (exactly-once effect). Next: auto-configuration, metrics,
> and the chaos test (Stage 3). See [plan.md](plan.md) for the design and
> [implementation-plan.md](implementation-plan.md) for the staged, as-built build record.

## Repository layout

Two **independent** Maven projects — no shared parent. The example consumes the starter
exactly the way a real application would: as a plain dependency.

```
├── idempotency-outbox-spring-boot-starter/   the library (Spring Boot starter)
├── examples/example-order-service/           demo app: e-commerce order flow using both features
├── docker-compose.yml                        local Postgres + Kafka for running the example
├── plan.md                                   architecture & design decisions
└── implementation-plan.md                    staged implementation plan
```

## Building

Each module builds on its own with the Maven Wrapper (no local Maven needed).
Integration tests use [Testcontainers](https://testcontainers.com/), so Docker must be
running.

```bash
# 1. Build the starter (unit tests + Postgres/Kafka integration tests)
cd idempotency-outbox-spring-boot-starter
./mvnw clean verify

# 2. Install it to the local Maven repo so the example can resolve it
./mvnw install

# 3. Build the example
cd ../examples/example-order-service
./mvnw clean verify
```

## Running the example locally

```bash
docker compose up -d          # Postgres (localhost:5433) + Kafka (localhost:9092)
cd examples/example-order-service
./mvnw spring-boot:run        # app on http://localhost:8080
```

## CI

Two independent GitHub Actions pipelines:

- [`idempotent-library-ci`](.github/workflows/idempotent-library-ci.yml) — builds and
  verifies the library (including Testcontainers integration tests) on starter changes.
- [`example-order-service-ci`](.github/workflows/example-order-service-ci.yml) —
  installs the starter, then builds the example on example or starter changes.

## Tech

Java 21 · Spring Boot 3.3 · PostgreSQL 16 · Apache Kafka (KRaft) · Flyway · Micrometer ·
Testcontainers · GitHub Actions
