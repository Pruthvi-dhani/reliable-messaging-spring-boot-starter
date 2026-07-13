/**
 * Spring Boot auto-configuration and {@code @ConfigurationProperties} that wire the idempotency and
 * outbox beans from the {@code idempotency.*} / {@code outbox.*} property tree.
 *
 * <p>Registered via {@code META-INF/spring/.../AutoConfiguration.imports}; beans are gated with
 * {@code @ConditionalOnProperty} / {@code @ConditionalOnMissingBean} so consumers can override.
 */
package io.github.pruthvidhani.idempotencyoutbox.autoconfigure;
