/**
 * Web integration: {@code @ControllerAdvice} mapping idempotency failures to HTTP status codes
 * ({@code IdempotencyConflictException} to 409, {@code IdempotencyKeyMissingException} to 400).
 *
 * <p>Only active when Spring MVC is on the classpath, so non-web consumers pay nothing for it.
 */
package io.github.pruthvidhani.idempotencyoutbox.web;
