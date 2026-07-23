package io.github.pruthvidhani.idempotencyoutbox.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class PropertiesBindingTest {

  private static <T> T bind(String prefix, Class<T> type, Map<String, String> properties) {
    var source = new MapConfigurationPropertySource(properties);
    return new Binder(source)
        .bind(prefix, Bindable.of(type))
        .orElseGet(
            () -> {
              try {
                return type.getDeclaredConstructor().newInstance();
              } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
              }
            });
  }

  @Test
  void idempotencyDefaultsAreSensible() {
    IdempotencyProperties props = bind("idempotency", IdempotencyProperties.class, Map.of());

    assertThat(props.isEnabled()).isTrue();
    assertThat(props.getDefaultTtl()).isEqualTo(Duration.ofHours(24));
    assertThat(props.getDuplicateWait()).isEqualTo(Duration.ofSeconds(5));
    assertThat(props.getDuplicatePollInterval()).isEqualTo(Duration.ofMillis(100));
    assertThat(props.getSweeper().isEnabled()).isTrue();
    assertThat(props.getSweeper().getInterval()).isEqualTo(Duration.ofMinutes(5));
  }

  @Test
  void idempotencyPropertiesBindFromTheTree() {
    IdempotencyProperties props =
        bind(
            "idempotency",
            IdempotencyProperties.class,
            Map.of(
                "idempotency.enabled", "false",
                "idempotency.default-ttl", "1h",
                "idempotency.duplicate-wait", "2s",
                "idempotency.duplicate-poll-interval", "50ms",
                "idempotency.sweeper.enabled", "false",
                "idempotency.sweeper.interval", "10m"));

    assertThat(props.isEnabled()).isFalse();
    assertThat(props.getDefaultTtl()).isEqualTo(Duration.ofHours(1));
    assertThat(props.getDuplicateWait()).isEqualTo(Duration.ofSeconds(2));
    assertThat(props.getDuplicatePollInterval()).isEqualTo(Duration.ofMillis(50));
    assertThat(props.getSweeper().isEnabled()).isFalse();
    assertThat(props.getSweeper().getInterval()).isEqualTo(Duration.ofMinutes(10));
  }

  @Test
  void outboxDefaultsAreSensible() {
    OutboxProperties props = bind("outbox", OutboxProperties.class, Map.of());

    assertThat(props.isEnabled()).isTrue();
    assertThat(props.getPollInterval()).isEqualTo(Duration.ofMillis(500));
    assertThat(props.getBatchSize()).isEqualTo(100);
    assertThat(props.getMaxAttempts()).isEqualTo(8);
    assertThat(props.getPublishTimeout()).isEqualTo(Duration.ofSeconds(15));
    assertThat(props.getBackoff().getBase()).isEqualTo(Duration.ofMillis(200));
    assertThat(props.getBackoff().getCap()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void outboxPropertiesBindFromTheTree() {
    OutboxProperties props =
        bind(
            "outbox",
            OutboxProperties.class,
            Map.of(
                "outbox.enabled", "false",
                "outbox.poll-interval", "250ms",
                "outbox.batch-size", "50",
                "outbox.max-attempts", "5",
                "outbox.publish-timeout", "8s",
                "outbox.backoff.base", "100ms",
                "outbox.backoff.cap", "1m"));

    assertThat(props.isEnabled()).isFalse();
    assertThat(props.getPollInterval()).isEqualTo(Duration.ofMillis(250));
    assertThat(props.getBatchSize()).isEqualTo(50);
    assertThat(props.getMaxAttempts()).isEqualTo(5);
    assertThat(props.getPublishTimeout()).isEqualTo(Duration.ofSeconds(8));
    assertThat(props.getBackoff().getBase()).isEqualTo(Duration.ofMillis(100));
    assertThat(props.getBackoff().getCap()).isEqualTo(Duration.ofMinutes(1));
  }
}
