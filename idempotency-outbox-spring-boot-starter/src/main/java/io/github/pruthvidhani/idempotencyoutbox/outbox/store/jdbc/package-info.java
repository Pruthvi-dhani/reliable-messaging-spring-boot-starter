/**
 * Postgres-backed implementation of the {@code OutboxStore} SPI ({@code JdbcOutboxStore}), including
 * the {@code SELECT ... FOR UPDATE SKIP LOCKED} batch poll that supports multiple poller instances.
 */
package io.github.pruthvidhani.idempotencyoutbox.outbox.store.jdbc;
