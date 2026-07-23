package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * Intercepts {@link Idempotent} methods and enforces the dedupe semantics:
 *
 * <ol>
 *   <li>Resolve the key (fail closed if missing) and the request hash.
 *   <li>Atomically claim the key. The winner executes the method and caches its response; if the
 *       method throws, the claim is released so the client can retry.
 *   <li>A duplicate (claim lost) first checks for a same-key/different-hash conflict (409), then
 *       replays the cached response if the original completed, or waits briefly for it to complete.
 *       If the original is still running when the wait budget is exhausted, or vanished (its
 *       execution failed and released the claim), {@link IdempotencyInProgressException} tells the
 *       client to retry shortly — unless the claim can be re-won, in which case the duplicate
 *       becomes the new winner and executes.
 * </ol>
 */
@Aspect
public class IdempotencyAspect {

  private final IdempotencyKeyResolver keyResolver;
  private final RequestHasher hasher;
  private final IdempotencyStore store;
  private final Clock clock;
  private final Duration defaultTtl;
  private final Duration duplicateWaitBudget;
  private final Duration duplicatePollInterval;
  private final ObjectMapper responseMapper;

  public IdempotencyAspect(
      IdempotencyKeyResolver keyResolver,
      RequestHasher hasher,
      IdempotencyStore store,
      Clock clock,
      Duration defaultTtl,
      Duration duplicateWaitBudget,
      Duration duplicatePollInterval) {
    this.keyResolver = keyResolver;
    this.hasher = hasher;
    this.store = store;
    this.clock = clock;
    this.defaultTtl = defaultTtl;
    this.duplicateWaitBudget = duplicateWaitBudget;
    this.duplicatePollInterval = duplicatePollInterval;
    this.responseMapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();
  }

  @Around("@annotation(idempotent)")
  public Object aroundIdempotentMethod(ProceedingJoinPoint joinPoint, Idempotent idempotent)
      throws Throwable {
    Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
    Object[] args = joinPoint.getArgs();

    String key = keyResolver.resolve(idempotent.key(), method, args);
    String requestHash =
        idempotent.hashBody() ? hasher.hash(hashTarget(idempotent, method, args)) : "";
    Duration ttl =
        idempotent.ttl().isEmpty()
            ? defaultTtl
            : DurationStyle.detectAndParse(idempotent.ttl());

    long waitDeadlineNanos = System.nanoTime() + duplicateWaitBudget.toNanos();
    while (true) {
      Instant now = clock.instant();
      if (store.putInProgress(key, requestHash, now, now.plus(ttl))) {
        return executeAndCache(joinPoint, method, key);
      }

      Optional<IdempotencyRecord> existing = store.find(key);
      if (existing.isPresent()) {
        IdempotencyRecord record = existing.get();
        if (idempotent.hashBody() && !record.requestHash().equals(requestHash)) {
          throw new IdempotencyConflictException(key);
        }
        if (record.status() == IdempotencyRecord.Status.COMPLETED) {
          return replay(method, record);
        }
      }
      // Key is claimed but not completed (original still running), or vanished between our claim
      // attempt and the read (original failed and released it) — wait briefly, then loop back to
      // re-claim or replay.
      if (System.nanoTime() >= waitDeadlineNanos) {
        throw new IdempotencyInProgressException(key);
      }
      sleep(duplicatePollInterval);
    }
  }

  private Object executeAndCache(ProceedingJoinPoint joinPoint, Method method, String key)
      throws Throwable {
    Object result;
    try {
      result = joinPoint.proceed();
    } catch (Throwable executionFailure) {
      // Release the claim: a failed execution must not burn the key — the client retries.
      store.remove(key);
      throw executionFailure;
    }
    byte[] payload =
        method.getReturnType() == void.class ? null : responseMapper.writeValueAsBytes(result);
    store.complete(key, payload, null);
    return result;
  }

  private Object replay(Method method, IdempotencyRecord record) throws java.io.IOException {
    if (record.responsePayload() == null) {
      return null; // void method: the guarantee is single execution, there is nothing to replay
    }
    return responseMapper.readValue(
        record.responsePayload(),
        responseMapper.constructType(method.getGenericReturnType()));
  }

  /** hashOf expression if set; else the @RequestBody parameter; else all arguments. */
  private Object hashTarget(Idempotent idempotent, Method method, Object[] args) {
    if (!idempotent.hashOf().isEmpty()) {
      return keyResolver.evaluate(idempotent.hashOf(), method, args);
    }
    Annotation[][] parameterAnnotations = method.getParameterAnnotations();
    for (int i = 0; i < parameterAnnotations.length; i++) {
      for (Annotation annotation : parameterAnnotations[i]) {
        if (annotation instanceof RequestBody) {
          return args[i];
        }
      }
    }
    return args;
  }

  private static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for idempotent duplicate", e);
    }
  }
}
