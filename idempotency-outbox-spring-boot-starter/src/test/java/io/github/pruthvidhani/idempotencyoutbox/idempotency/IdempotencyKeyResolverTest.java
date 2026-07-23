package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IdempotencyKeyResolverTest {

  private final IdempotencyKeyResolver resolver = new IdempotencyKeyResolver();

  /** Fixture whose methods mirror the shapes the aspect will intercept. */
  @SuppressWarnings("unused")
  static class Fixture {
    public String byHeader(Map<String, String> headers, String body) {
      return body;
    }

    public void byParts(String userId, String action) {}
  }

  private static Method method(String name, Class<?>... params) throws NoSuchMethodException {
    return Fixture.class.getDeclaredMethod(name, params);
  }

  @Test
  void resolvesKeyFromHeaderMap() throws Exception {
    Map<String, String> headers = Map.of("Idempotency-Key", "key-123");

    String key =
        resolver.resolve(
            "#headers['Idempotency-Key']",
            method("byHeader", Map.class, String.class),
            new Object[] {headers, "body"});

    assertThat(key).isEqualTo("key-123");
  }

  @Test
  void resolvesComposedKeyFromMultipleArguments() throws Exception {
    String key =
        resolver.resolve(
            "#userId + ':' + #action",
            method("byParts", String.class, String.class),
            new Object[] {"user-1", "checkout"});

    assertThat(key).isEqualTo("user-1:checkout");
  }

  @Test
  void failsClosedWhenHeaderAbsent() throws Exception {
    Map<String, String> headers = new HashMap<>(); // no Idempotency-Key entry

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    "#headers['Idempotency-Key']",
                    method("byHeader", Map.class, String.class),
                    new Object[] {headers, "body"}))
        .isInstanceOf(IdempotencyKeyMissingException.class)
        .hasMessageContaining("Idempotency-Key");
  }

  @Test
  void failsClosedWhenKeyIsBlank() throws Exception {
    Map<String, String> headers = Map.of("Idempotency-Key", "   ");

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    "#headers['Idempotency-Key']",
                    method("byHeader", Map.class, String.class),
                    new Object[] {headers, "body"}))
        .isInstanceOf(IdempotencyKeyMissingException.class);
  }

  @Test
  void failsClosedWhenExpressionCannotBeEvaluated() throws Exception {
    assertThatThrownBy(
            () ->
                resolver.resolve(
                    "#nonexistent.field",
                    method("byParts", String.class, String.class),
                    new Object[] {"user-1", "checkout"}))
        .isInstanceOf(IdempotencyKeyMissingException.class);
  }
}
