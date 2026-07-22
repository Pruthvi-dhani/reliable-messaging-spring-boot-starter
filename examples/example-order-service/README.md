# Example: Order Service

A standalone Spring Boot app that consumes the
`idempotency-outbox-spring-boot-starter` the same way a real project would — as a plain
Maven dependency, with its own independent build.

It demonstrates an e-commerce "place order" flow using **both** starter features:

- **Idempotent order placement** *(arrives in Stage 1)* — `POST /orders` guarded by
  `@Idempotent`: a client retry with the same `Idempotency-Key` header replays the
  cached response instead of charging twice; same key with a different body is rejected
  with `409`.
- **Outbox → Kafka → idempotent consumer** *(arrives in Stage 2)* — the order insert and
  an `OrderPlaced` outbox event commit in one transaction; a background poller publishes
  the event to Kafka; an `@Idempotent` Kafka consumer collapses duplicate deliveries to
  exactly-once effect.

> **Status:** Stage 0 — bootable skeleton only; the endpoint and consumer land with the
> stages above.

## Running

The starter must be in your local Maven repo first:

```bash
cd ../../idempotency-outbox-spring-boot-starter
./mvnw install
```

Then start the local infrastructure (from the repo root) and run the app:

```bash
docker compose up -d     # Postgres on localhost:5433, Kafka on localhost:9092
cd examples/example-order-service
./mvnw spring-boot:run   # app on http://localhost:8080
```

Configuration lives in [src/main/resources/application.yml](src/main/resources/application.yml)
and matches the compose stack's ports and credentials.
