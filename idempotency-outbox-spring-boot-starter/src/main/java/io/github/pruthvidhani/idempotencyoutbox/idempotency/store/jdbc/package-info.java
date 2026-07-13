/**
 * Postgres-backed implementation of the {@code IdempotencyStore} SPI ({@code JdbcIdempotencyStore}).
 *
 * <p>Isolated in its own package so it can be extracted into a separate module if an alternative
 * store (Redis, DynamoDB) is added later.
 */
package io.github.pruthvidhani.idempotencyoutbox.idempotency.store.jdbc;
