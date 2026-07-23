# Example: Order Service

A standalone Spring Boot app that consumes the
`idempotency-outbox-spring-boot-starter` the same way a real project would — as a plain
Maven dependency, with its own independent build.

It demonstrates an e-commerce "place order" flow using **both** starter features:

- **Idempotent order placement** *(✅ live)* — `POST /orders` guarded by `@Idempotent`:
  a client retry with the same `Idempotency-Key` header replays the cached response
  instead of charging twice; same key with a different body is rejected with `409`; a
  missing key is rejected with an explanatory `400`.
- **Outbox → Kafka → idempotent consumer** *(arrives in Stage 2)* — the order insert and
  an `OrderPlaced` outbox event commit in one transaction; a background poller publishes
  the event to Kafka; an `@Idempotent` Kafka consumer collapses duplicate deliveries to
  exactly-once effect.

## Try it

With the app running (see below):

```bash
# Place an order — the client generates one key per checkout attempt
curl -s -X POST localhost:8080/orders \
  -H 'Idempotency-Key: attempt-1' -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","amountPence":4999,"currency":"GBP"}'
# → {"orderId":"<uuid>","customerId":"cust-1","amountPence":4999,"status":"CONFIRMED"}

# Retry (network dropped, user double-tapped): SAME response, no second charge
curl -s -X POST localhost:8080/orders \
  -H 'Idempotency-Key: attempt-1' -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","amountPence":4999,"currency":"GBP"}'
# → identical orderId as above

# Same key, different body → 409 with an explanatory ProblemDetail
curl -s -X POST localhost:8080/orders \
  -H 'Idempotency-Key: attempt-1' -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","amountPence":9999,"currency":"GBP"}'
# → {"status":409,"title":"Idempotency key conflict",...}

# Missing key → 400 telling you what to do
curl -s -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","amountPence":4999,"currency":"GBP"}'
# → {"status":400,"title":"Idempotency key required",...}
```

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
