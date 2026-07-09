# Idempotency + Transactional Outbox Spring Boot Starter — Plan

A drop-in Spring Boot starter that solves two near-universal backend problems:

1. **Request idempotency** — safely handle client retries via an `@Idempotent`
   annotation backed by a Postgres dedupe table.
2. **Reliable event publishing** — the transactional outbox pattern (write to DB +
   publish to broker, atomically, at-least-once).

Both are well-understood patterns that are tedious and error-prone to roll by hand.
This starter packages them behind auto-configuration so a consuming service adds a
dependency, sets a handful of properties, and gets both.

---

## 1. Goals & Non-Goals

### Goals (MVP)
- `@Idempotent` annotation usable on Spring controller/service methods.
- Postgres-backed dedupe store with request-hash + cached response + TTL expiry.
- Replay protection: same key + different body → `409 Conflict`.
- Transactional outbox: events written in the same TX as the business write.
- Background poller that publishes pending outbox events to Kafka, in order, with
  retry (exponential backoff) and dead-lettering on max attempts.
- Spring Boot auto-configuration + typed configuration properties.
- Two runnable example apps in the repo.
- Testcontainers-based integration tests (Postgres + Kafka), including a chaos test
  proving no event loss when the publisher is killed mid-flight.
- A README with an architecture diagram, delivery-semantics section, and a benchmark
  table.

### Non-Goals (MVP)
- Exactly-once end-to-end delivery (we provide at-least-once + consumer-side dedupe
  guidance). Documented explicitly.
- Brokers other than Kafka (SPI leaves room; only Kafka implemented).
- Dedupe stores other than Postgres (SPI leaves room; only Postgres implemented).
- Distributed/multi-region coordination.

### Stretch (post-MVP)
- Postgres `LISTEN/NOTIFY` outbox tailer for lower-latency publish vs. polling.
- Debezium-based outbox bridge as a swappable CDC alternative.
- Pluggable dedupe storage SPI — second impl backed by Redis or DynamoDB.
- Micrometer metrics: dedupe hit rate, outbox publish latency, outbox lag.
- Docs site (mkdocs/Docusaurus) with a "five common pitfalls" page.

---

## 2. Tech Stack
- **Java 21**, Spring Boot 3.x
- **PostgreSQL** (dedupe table + outbox table)
- **Apache Kafka** (event broker)
- **Flyway** for schema migrations shipped with the starter
- **Micrometer** for metrics
- **Testcontainers** (Postgres + Kafka) + JUnit 5 for tests
- **GitHub Actions** for CI (build, test, publish coverage)
- Build: **Maven** (multi-module)

---

## 3. Module Layout (multi-module Maven)

```
reliable-messaging-spring-boot-starter/         (parent POM)
├── idempotency-outbox-core/                     core domain + SPI, no Spring Boot AutoConfig
│   ├── idempotency/  (annotation, key resolver, store SPI, hasher)
│   └── outbox/       (OutboxEvent, OutboxStore SPI, publisher SPI, poller)
├── idempotency-outbox-postgres/                 Postgres impls of the store SPIs + Flyway
├── idempotency-outbox-kafka/                    Kafka publisher impl
├── idempotency-outbox-spring-boot-starter/      auto-config + properties + metrics wiring
├── examples/
│   ├── example-idempotent-payments/             @Idempotent payment-intent endpoint
│   └── example-order-outbox/                     order created → outbox → Kafka → consumer
└── plan.md / README.md
```

Rationale: keep `core` free of Spring Boot autoconfig so the patterns are testable in
isolation and the storage/broker bindings are swappable (supports the SPI stretch goals).

---

## 4. Component Design

### 4.1 Idempotency

**Annotation**
```java
@Idempotent(key = "#request.requestId", ttl = "24h")
```
- `key` — SpEL expression resolved against method args.
- `ttl` — how long a dedupe entry is honored (duration string).
- Optional: `hashBody` (default true), `include` for extra hash inputs.

**AOP interceptor flow**
1. Resolve the idempotency key from the SpEL expression.
2. Compute a request hash (canonicalized body → SHA-256; see decision in §6).
3. Within the current request transaction, look up the key in the dedupe table.
4. **Hit, same hash** → return the cached response payload (do not re-run method).
5. **Hit, different hash** → throw `IdempotencyConflictException` → mapped to `409`.
6. **Miss** → insert an in-progress row (unique constraint on key), run the method,
   persist `(key, hash, serialized response, created_at, expires_at)`, return result.
7. Concurrent duplicate racing on insert → catch unique-violation → treat as hit/wait.

**Dedupe table**
```
idempotency_keys(
  idempotency_key   text primary key,
  request_hash      text not null,
  response_payload  bytea,          -- serialized cached response
  response_status   int,            -- for HTTP method returns
  status            text,           -- IN_PROGRESS | COMPLETED
  created_at        timestamptz not null,
  expires_at        timestamptz not null
)
```
- Index on `expires_at` for the sweeper.
- A scheduled sweeper deletes expired rows (config-driven interval).

### 4.2 Transactional Outbox

**Write path** (in business transaction)
- Business code calls `OutboxPublisher.record(aggregateType, aggregateId, eventType, payload)`.
- Inserts a row into `outbox_events` in the **same** transaction as the business write.
- No broker I/O happens on the request path.

**Outbox table**
```
outbox_events(
  id              uuid primary key,
  aggregate_type  text not null,
  aggregate_id    text not null,      -- ordering key (partition per aggregate)
  event_type      text not null,
  payload         jsonb not null,
  headers         jsonb,
  status          text not null,      -- PENDING | PUBLISHED | DEAD
  attempts        int not null default 0,
  next_attempt_at timestamptz,
  created_at      timestamptz not null,
  published_at    timestamptz
)
```
- Index on `(status, next_attempt_at)` for the poller.

**Poller** (background, `@Scheduled` or dedicated thread)
- Selects a batch of `PENDING` rows ordered by `created_at` (and grouped/ordered per
  `aggregate_id`) using `SELECT ... FOR UPDATE SKIP LOCKED` to allow multiple instances.
- Publishes each to Kafka (key = `aggregate_id` so per-aggregate ordering holds).
- On success → mark `PUBLISHED`, set `published_at`.
- On failure → increment `attempts`, set `next_attempt_at` via exponential backoff;
  when `attempts >= max` → mark `DEAD` (dead-letter) and emit a metric/log.
- Config: poll interval, batch size, max attempts, backoff base/cap.

**Delivery semantics: at-least-once.** A crash after Kafka publish but before the DB
mark → the event republishes on restart. Documented; consumers must dedupe (which is
exactly what the idempotency half of this library enables downstream).

---

## 5. Auto-Configuration & Properties

```yaml
idempotency:
  enabled: true
  default-ttl: 24h
  sweeper:
    interval: 5m
outbox:
  enabled: true
  poll-interval: 500ms
  batch-size: 100
  max-attempts: 8
  backoff:
    base: 200ms
    cap: 30s
  kafka:
    topic-resolver: default   # aggregateType -> topic mapping strategy
```

- `@AutoConfiguration` classes gated on `@ConditionalOnClass` / `@ConditionalOnProperty`.
- Beans: key resolver, request hasher, dedupe store, idempotency aspect, outbox
  publisher, outbox poller, Kafka publisher, Micrometer binders.
- Flyway migrations for both tables ship in the Postgres module (with a documented
  opt-out for teams managing their own schema).

---

## 6. Architectural Decisions to Document (the senior-IC signal)

These get first-class treatment in the README:

1. **Store the response payload, not just the key.** So a client retrying after a
   network drop gets the *same* response, not just a "duplicate" acknowledgement.
2. **Request hash computation.** Canonicalized body only by default (stable JSON
   serialization); headers excluded unless opted in. Document what invalidates
   idempotency and why "same key, different body" is a client bug worth surfacing (409).
3. **TTL policy.** Entries expire after `ttl`; a retry after expiry is treated as a new
   request. Trade-off: storage growth vs. replay window. Sweeper reclaims space.
4. **Outbox polling as default, CDC as stretch.** Polling = operational simplicity, no
   Debezium/connector to run; cost is publish latency (bounded by poll interval).
   `LISTEN/NOTIFY` and Debezium are the low-latency upgrades.
5. **Ordering guarantees.** Per-aggregate ordering via Kafka key = `aggregate_id` +
   ordered polling; no global ordering guarantee. Explain why per-aggregate is the
   right granularity.
6. **Failure modes.** Publisher crash mid-batch, broker unavailable, DB unavailable —
   spell out what happens and what the guarantee degrades to in each case.

---

## 7. Testing Strategy
- **Unit**: SpEL key resolution, hashing determinism, backoff math, store SPI contracts.
- **Integration (Testcontainers Postgres + Kafka)**:
  - Idempotency: first call runs + caches; duplicate returns cached; concurrent
    duplicates race safely; different body → 409; expiry after TTL.
  - Outbox: event written in business TX is published exactly to Kafka; ordering per
    aggregate; retry + dead-letter on repeated failure.
- **Chaos test**: kill the publisher mid-batch (or drop the Kafka container), restart,
  assert every recorded event lands on Kafka exactly once *or more* (no loss), and
  consumer dedupe collapses duplicates.
- **CI**: GitHub Actions matrix, Testcontainers, coverage gate.

---

## 8. Deliverables
- Published starter artifact (local `install` for MVP; Maven Central/GitHub Packages later).
- Two example apps runnable via `docker compose up` (Postgres + Kafka) + `mvn spring-boot:run`.
- README: architecture diagram, delivery-semantics section, decision log (§6),
  benchmark table (throughput, p50/p99 latency, dedupe hit rate).

---

## 9. Milestones (target: ~2 weekends for MVP)

**M0 — Scaffolding**
- Parent POM, module skeletons, CI pipeline, Testcontainers base test.

**M1 — Idempotency vertical slice**
- Annotation, SpEL resolver, hasher, Postgres store, AOP aspect, 409 handling.
- Integration tests + `example-idempotent-payments`.

**M2 — Outbox vertical slice**
- Outbox table + migration, `OutboxPublisher.record`, poller with backoff + DLQ,
  Kafka publisher, per-aggregate ordering.
- Integration tests + `example-order-outbox`.

**M3 — Auto-config & polish**
- Auto-configuration classes, properties, Micrometer metrics, sweeper.
- Chaos test.

**M4 — Docs & benchmark**
- README (diagram, semantics, decision log), benchmark harness + results table.

**Stretch (as time allows)**
- LISTEN/NOTIFY tailer, Debezium bridge, Redis/DynamoDB dedupe SPI impl, docs site.

---

## 10. Open Questions
- Response serialization format for the cache — JSON vs. Java serialization vs. bytes
  pass-through? (Leaning JSON for portability; revisit for non-HTTP returns.)
- Topic naming/resolution strategy — convention (`<aggregateType>.events`) vs. explicit
  per-event config?
- Should the sweeper be in-process (`@Scheduled`) or delegated to a DB job? (In-process
  for MVP.)
- Multi-instance poller coordination — rely on `SKIP LOCKED` (chosen) vs. leader election?
