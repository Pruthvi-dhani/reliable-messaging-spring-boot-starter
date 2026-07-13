/**
 * Request idempotency: the {@code @Idempotent} annotation, SpEL key resolver, request hasher, the
 * AOP aspect, the {@code IdempotencyStore} SPI, and the dedupe domain records/exceptions.
 *
 * <p>Implementations of the store SPI live in sub-packages (e.g. {@code store.jdbc}).
 */
package io.github.pruthvidhani.idempotencyoutbox.idempotency;
