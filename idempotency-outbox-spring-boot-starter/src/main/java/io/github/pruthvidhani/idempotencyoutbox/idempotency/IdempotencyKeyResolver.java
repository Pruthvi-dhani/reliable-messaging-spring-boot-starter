package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Resolves the idempotency key from the {@link Idempotent#key()} SpEL expression, evaluated against
 * the intercepted method's arguments (each parameter is available as {@code #paramName}).
 *
 * <p>Fail-closed: a null/blank result — or an expression that cannot be evaluated at all — throws
 * {@link IdempotencyKeyMissingException} rather than silently skipping deduplication.
 */
public class IdempotencyKeyResolver {

  private final SpelExpressionParser parser = new SpelExpressionParser();
  private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();
  private final Map<String, Expression> expressionCache = new ConcurrentHashMap<>();

  /**
   * Evaluates {@code expression} against the given method invocation.
   *
   * @return the resolved key, never null or blank
   * @throws IdempotencyKeyMissingException if the expression evaluates to null/blank or fails to
   *     evaluate
   */
  public String resolve(String expression, Method method, Object[] args) {
    Expression parsed = expressionCache.computeIfAbsent(expression, parser::parseExpression);
    Object value;
    try {
      var context = new MethodBasedEvaluationContext(args, method, args, parameterNames);
      value = parsed.getValue(context);
    } catch (EvaluationException e) {
      throw new IdempotencyKeyMissingException(expression);
    }
    if (value == null || value.toString().isBlank()) {
      throw new IdempotencyKeyMissingException(expression);
    }
    return value.toString();
  }
}
