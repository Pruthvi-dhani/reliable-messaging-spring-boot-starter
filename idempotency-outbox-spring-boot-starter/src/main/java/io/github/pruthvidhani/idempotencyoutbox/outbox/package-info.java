/**
 * Transactional outbox: the {@code OutboxEvent} record, {@code OutboxStore} and
 * {@code EventPublisher} SPIs, the {@code OutboxPublisher} façade business code calls, the
 * background {@code OutboxPoller}, and the backoff/topic-resolution helpers.
 *
 * <p>Store and broker implementations live in sub-packages ({@code store.jdbc},
 * {@code publisher.kafka}).
 */
package io.github.pruthvidhani.idempotencyoutbox.outbox;
