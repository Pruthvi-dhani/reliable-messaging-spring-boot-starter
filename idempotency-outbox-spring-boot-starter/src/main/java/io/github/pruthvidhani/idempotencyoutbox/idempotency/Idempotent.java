package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as idempotent: repeated invocations carrying the same idempotency key are not
 * re-executed — the cached result of the first execution is returned instead.
 *
 * <p>Semantics (enforced by the interceptor):
 *
 * <ul>
 *   <li><b>First call</b> for a key: the method runs; its response is cached alongside a hash of
 *       the request.
 *   <li><b>Duplicate call</b> (same key, same request hash): the method does <i>not</i> run; the
 *       cached response is returned.
 *   <li><b>Conflict</b> (same key, different request hash): {@link IdempotencyConflictException}
 *       is thrown — mapped to HTTP 409 for web consumers.
 *   <li><b>Missing key</b> (expression resolves to null/blank): fail closed —
 *       {@link IdempotencyKeyMissingException}, mapped to HTTP 400.
 * </ul>
 *
 * <p>Interception is proxy-based Spring AOP: self-invocation ({@code this.method(...)}) bypasses
 * the idempotency check.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @PostMapping("/orders")
 * @Idempotent(key = "#headers['Idempotency-Key']", ttl = "24h")
 * public OrderResponse place(@RequestHeader Map<String, String> headers,
 *                            @RequestBody OrderRequest request) { ... }
 * }</pre>
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Idempotent {

  /**
   * SpEL expression resolving the idempotency key. Method parameters are available as variables
   * ({@code #paramName}). Must resolve to a non-blank value or the call is rejected (fail closed).
   */
  String key();

  /**
   * How long the dedupe entry is honored, as a duration string ({@code "24h"}, {@code "30m"},
   * {@code "PT15M"}). Empty means use the configured default ({@code idempotency.default-ttl}).
   */
  String ttl() default "";

  /**
   * Whether to hash the method arguments and reject a duplicate key whose request differs (409).
   * Disable only when the key alone is authoritative (e.g. consuming broker events whose id fully
   * identifies the payload).
   */
  boolean hashBody() default true;
}
