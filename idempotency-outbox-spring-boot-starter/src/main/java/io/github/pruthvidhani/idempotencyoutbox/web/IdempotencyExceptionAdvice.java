package io.github.pruthvidhani.idempotencyoutbox.web;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyConflictException;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyInProgressException;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyKeyMissingException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps idempotency failures to HTTP responses with RFC-7807 {@link ProblemDetail} bodies, so the
 * client always sees <i>why</i> the request was rejected:
 *
 * <ul>
 *   <li>missing key → 400 with a "supply the Idempotency-Key header" detail
 *   <li>same key, different body → 409 (replay protection)
 *   <li>same key, original still executing → 409 with {@code Retry-After: 1}
 * </ul>
 */
@RestControllerAdvice
public class IdempotencyExceptionAdvice {

  @ExceptionHandler(IdempotencyKeyMissingException.class)
  public ProblemDetail handleMissingKey(IdempotencyKeyMissingException e) {
    ProblemDetail problem =
        ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    problem.setTitle("Idempotency key required");
    return problem;
  }

  @ExceptionHandler(IdempotencyConflictException.class)
  public ProblemDetail handleConflict(IdempotencyConflictException e) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    problem.setTitle("Idempotency key conflict");
    return problem;
  }

  @ExceptionHandler(IdempotencyInProgressException.class)
  public ResponseEntity<ProblemDetail> handleInProgress(IdempotencyInProgressException e) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    problem.setTitle("Request already in progress");
    return ResponseEntity.status(HttpStatus.CONFLICT).header("Retry-After", "1").body(problem);
  }
}
