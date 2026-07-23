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
- Build: **Maven** (two independent modules — library + example — no shared parent;
  each uses `spring-boot-starter-parent` and has its own build/CI)

---

## 3. Module Layout (two independent modules — no shared parent)

The library is **one module** (the starter); the example is a **separate, independent
project** that depends on the starter by coordinates — exactly how a real customer wires
it up. There is **no aggregator/parent POM**: each module has its own POM, its own build,
and its own CI pipeline. Within the library, layering is enforced by package structure.

```
reliable-messaging-spring-boot-starter/              (repo root — no POM of its own)
├── idempotency-outbox-spring-boot-starter/          THE library — standalone module
│   ├── pom.xml                                       parent = spring-boot-starter-parent
│   ├── src/main/java/.../idempotencyoutbox/
│   │   ├── idempotency/    annotation, aspect, key resolver, hasher, store SPI
│   │   │   └── store/jdbc/ JdbcIdempotencyStore
│   │   ├── outbox/         OutboxEvent, store SPI, publisher SPI + façade, poller, backoff
│   │   │   ├── store/jdbc/         JdbcOutboxStore
│   │   │   └── publisher/kafka/    KafkaEventPublisher
│   │   ├── web/            ControllerAdvice (409 / 400 mapping)
│   │   └── autoconfigure/  auto-config classes + @ConfigurationProperties
│   └── src/main/resources/db/migration/  Flyway migrations
├── examples/
│   └── example-order-service/                        independent consumer app — depends on
│       ├── pom.xml                                   the starter like any third-party dep;
│       └── src/...                                   exercises BOTH features (idempotent
│                                                     order placement + outbox → Kafka →
│                                                     idempotent consumer)
└── plan.md / README.md
```

Rationale: two independent builds mirror reality — a customer's app never shares a parent
POM with the starter, it just declares a dependency on the published artifact. Keeping the
example standalone makes it double as a realistic consumer reference. Each module uses
`spring-boot-starter-parent` for dependency/plugin version management (build-time only,
invisible to consumers). Within the library, the `idempotency` / `outbox` / `autoconfigure`
package split keeps concerns separated, and the SPI interfaces (`IdempotencyStore`,
`OutboxStore`, `EventPublisher`) preserve the swappability story; the impl sub-packages
extract cleanly into their own modules later if the SPI stretch goals materialize.

Local dev / CI note: since there is no aggregator, the example resolves the starter from
the local Maven repo — `mvn install` the starter first, then build the example (or, once
published, the example pulls it from a registry and is fully decoupled).

---

## 4. Component Design

### 4.1 Idempotency

**Annotation**
```java
@Idempotent(key = "#headers['Idempotency-Key']", ttl = "24h")
```
- `key` — SpEL expression resolved against method args.
- `ttl` — how long a dedupe entry is honored (duration string).
- Optional: `hashBody` (default true) toggles request-hash verification; `hashOf` (SpEL)
  selects what to hash — default is the `@RequestBody` parameter (canonical JSON), see
  §6 decision 2.

**Key resolution — client-supplied header keys (chosen default)**
- The idempotency key is supplied by the **client** as an `Idempotency-Key` HTTP
  header (Stripe-style), not derived from the payload. Rationale: the client owns
  what "the same request" means and can retry a dropped request with the same key;
  the server does not have to guess a natural unique field.
- The SpEL expression reads the header off a method argument, e.g. a `Map<String,String>
  headers` param (`#headers['Idempotency-Key']`) or a typed request wrapper. For
  non-HTTP callers (Kafka consumers, internal service calls) the same expression form
  reads the equivalent field (`#event.eventId`) — the mechanism is unchanged, only the
  source differs.
- The resolver exposes each method parameter as a SpEL variable (`#paramName`),
  evaluates the expression, and stringifies the result to form the dedupe-table PK.
- **Transport-agnostic**: works on HTTP controllers, `@KafkaListener` consumers, and
  plain `@Service` methods, since interception is at the Spring-AOP method level.
  Caveat to document: self-invocation (`this.method()`) bypasses the proxy and the
  interceptor.

**Missing / unresolvable key — fail closed (chosen policy)**
- If the SpEL expression evaluates to null/blank (client omitted the header), the
  request is **rejected** — `400 Bad Request` with a clear "Idempotency-Key required"
  message — rather than silently falling back to no-dedupe. Fintech-safe default.

**Supported message / invocation types**
Interception is at the Spring-AOP method level, so the "message" is whatever the
annotated method receives. Three shapes are supported:

1. **HTTP requests (primary)** — key from the client header; the response is cached and
   replayed on retry so a dropped-then-retried `POST` never double-executes.
   ```java
   @PostMapping("/payments")
   @Idempotent(key = "#headers['Idempotency-Key']", ttl = "24h")
   public PaymentIntent create(@RequestHeader Map<String,String> headers,
                               @RequestBody PaymentRequest req) { ... }
   ```
2. **Message-broker consumers** — the consumed event is the message; key from the event
   id. Return type is typically `void`, so there is **no response to cache** — the
   guarantee is simply that the side-effect runs once. This is the consumer-side dedupe
   the (at-least-once) outbox half of the library relies on.
   ```java
   @KafkaListener(topics = "orders")
   @Idempotent(key = "#event.eventId")
   public void handle(OrderEvent event) { ... }
   ```
3. **Internal service-to-service calls** — any Spring-managed `@Service` method invoked
   through the proxy (e.g. from a scheduled job or another bean). Same mechanism.

Constraint (document in README): proxy-based AOP means a `this.method()`
self-invocation inside the same bean bypasses the interceptor.

**Key vs. request hash — two different questions.** The key answers *"is this the same
logical request?"* (client-supplied, the dedupe-table PK). The request hash answers
*"...and did they actually send the same thing?"* (canonicalized body → SHA-256). Same
key + same hash → replay the cached response (or no-op for `void`). Same key + different
hash → `409 Conflict` (client bug / replay-protection).

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
2. **Request hash computation — canonical JSON of the bound body, not the raw wire
   string.** *(Decided during Stage 1.)* The `@RequestBody`-bound object is re-serialized
   canonically (POJO properties alphabetized, map entries sorted by key, ISO dates) and
   SHA-256'd. Why not hash the raw request bytes:
   - Raw bytes make the hash depend on byte-level accidents that don't change meaning —
     serializer field ordering (e.g. Go maps randomize per run), whitespace/formatting,
     unicode escaping — so a legitimate application-level retry that *re-serializes* the
     same logical request can produce different bytes → **false 409**.
   - Raw-byte hashing is only safe when retries are byte-identical (SDK-level retry loops
     that resend the same buffered body). A drop-in starter serves arbitrary clients with
     app-level retry loops, so it cannot assume that.
   - Practically, by the time the AOP aspect runs, Spring has consumed the body into the
     POJO; capturing raw bytes would force request-wrapping plumbing on every consumer.
   Trade-off accepted: two raw bodies differing only in fields the DTO doesn't bind hash
   the same — semantically fine, since the server's behavior is identical for both. The
   hash covers *what the server consumes*, not every byte the client sent.
   **What gets hashed** is developer-controlled via `hashOf` (SpEL): explicit selection
   wins; default is the `@RequestBody` parameter; fallback (non-web methods) is all
   arguments. Headers are never hashed by default (volatile headers like trace IDs would
   cause false conflicts). `hashBody = false` disables verification entirely for cases
   where the key alone is authoritative (e.g. consuming outbox events by event id).
   Document why "same key, different body" is a client bug worth surfacing loudly (409):
   without the hash, a reused key silently replays the *wrong* cached response — silent
   data loss; with it, the bug surfaces at the client's first test.
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
- One example app (`example-order-service`) exercising both features, runnable via
  `docker compose up` (Postgres + Kafka) + `mvn spring-boot:run`.
- README: architecture diagram, delivery-semantics section, decision log (§6),
  benchmark table (throughput, p50/p99 latency, dedupe hit rate).

---

## 9. Milestones (target: ~2 weekends for MVP)

**M0 — Scaffolding** ✅ done
- Two independent module POMs (no shared parent), package skeletons, per-module CI
  pipelines + Maven wrappers, Testcontainers bases + connectivity ITs, compose stack.

**M1 — Idempotency vertical slice**
- Annotation, SpEL resolver, hasher, Postgres store, AOP aspect, 409 handling.
- Integration tests + idempotent order-placement endpoint in `example-order-service`.

**M2 — Outbox vertical slice**
- Outbox table + migration, `OutboxPublisher.record`, poller with backoff + DLQ,
  Kafka publisher, per-aggregate ordering.
- Integration tests + the outbox → Kafka → consumer flow in `example-order-service`.

**M3 — Auto-config & polish**
- Auto-configuration classes, properties, Micrometer metrics, sweeper.
- Chaos test.

**M4 — Docs & benchmark**
- README (diagram, semantics, decision log), benchmark harness + results table.

**Stretch (as time allows)**
- LISTEN/NOTIFY tailer, Debezium bridge, Redis/DynamoDB dedupe SPI impl, docs site.

> A detailed, stage-by-stage build plan with concrete steps, per-stage testing, and
> exit criteria lives in [implementation-plan.md](implementation-plan.md).

---

## 10. Open Questions
- ~~Key source — client-supplied header vs. payload-derived?~~ **Decided: client-supplied
  `Idempotency-Key` header (fail closed if missing).** See §4.1.
- Response serialization format for the cache — JSON vs. Java serialization vs. bytes
  pass-through? (Leaning JSON for portability; revisit for non-HTTP returns.)
- Topic naming/resolution strategy — convention (`<aggregateType>.events`) vs. explicit
  per-event config?
- Should the sweeper be in-process (`@Scheduled`) or delegated to a DB job? (In-process
  for MVP.)
- Multi-instance poller coordination — rely on `SKIP LOCKED` (chosen) vs. leader election?
