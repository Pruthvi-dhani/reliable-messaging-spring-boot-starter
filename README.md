# Idempotency + Transactional Outbox — Spring Boot Starter

A drop-in Spring Boot starter for two near-universal backend problems:

1. **Request idempotency** — safely absorb client retries with an `@Idempotent`
   annotation backed by a Postgres dedupe table (client-supplied `Idempotency-Key`
   header, cached-response replay, `409` on same-key/different-body).
2. **Reliable event publishing** — the transactional outbox pattern: events are written
   in the same database transaction as the business change, then published to Kafka by a
   background poller with retry, backoff, and dead-lettering. At-least-once delivery;
   combined with consumer-side `@Idempotent`, exactly-once *effect*.

> **Status: under construction.** Stage 0 (project scaffolding, CI, Testcontainers
> infrastructure) is in place; the features themselves land next. See
> [plan.md](plan.md) for the design and [implementation-plan.md](implementation-plan.md)
> for the staged build plan.

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

- [`starter-ci`](.github/workflows/starter-ci.yml) — builds and verifies the library
  (including Testcontainers integration tests) on starter changes.
- [`example-ci`](.github/workflows/example-ci.yml) — installs the starter, then builds
  the example on example or starter changes.

## Tech

Java 21 · Spring Boot 3.3 · PostgreSQL 16 · Apache Kafka (KRaft) · Flyway · Micrometer ·
Testcontainers · GitHub Actions
