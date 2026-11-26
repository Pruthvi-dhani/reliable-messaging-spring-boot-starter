package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RequestHasherTest {

  private final RequestHasher hasher = new RequestHasher();

  record OrderRequest(String customerId, int amountPence, String currency) {}

  record Payment(
      String zeta,
      int alpha,
      BigDecimal amount,
      Instant at,
      LocalDate day,
      Duration ttl,
      Map<String, Object> attrs,
      String nothing) {}

  @Test
  void sameInputAlwaysProducesSameHash() {
    var request = new OrderRequest("cust-1", 4999, "GBP");

    assertThat(hasher.hash(request)).isEqualTo(hasher.hash(request));
  }

  @Test
  void logicallyEqualObjectsProduceSameHash() {
    assertThat(hasher.hash(new OrderRequest("cust-1", 4999, "GBP")))
        .isEqualTo(hasher.hash(new OrderRequest("cust-1", 4999, "GBP")));
  }

  @Test
  void mapEntryOrderDoesNotAffectHash() {
    Map<String, Object> inOneOrder = new LinkedHashMap<>();
    inOneOrder.put("customerId", "cust-1");
    inOneOrder.put("amountPence", 4999);
    inOneOrder.put("currency", "GBP");

    Map<String, Object> inAnotherOrder = new LinkedHashMap<>();
    inAnotherOrder.put("currency", "GBP");
    inAnotherOrder.put("amountPence", 4999);
    inAnotherOrder.put("customerId", "cust-1");

    assertThat(hasher.hash(inOneOrder)).isEqualTo(hasher.hash(inAnotherOrder));
  }

  @Test
  void changedValueChangesHash() {
    assertThat(hasher.hash(new OrderRequest("cust-1", 4999, "GBP")))
        .isNotEqualTo(hasher.hash(new OrderRequest("cust-1", 5000, "GBP")));
  }

  @Test
  void argumentArrayHashingIsOrderSensitive() {
    Object[] args = {new OrderRequest("cust-1", 4999, "GBP"), "extra"};
    Object[] reordered = {"extra", new OrderRequest("cust-1", 4999, "GBP")};

    assertThat(hasher.hash(args)).isNotEqualTo(hasher.hash(reordered));
  }

  @Test
  void hashIsLowercaseSha256Hex() {
    assertThat(hasher.hash(new OrderRequest("cust-1", 4999, "GBP")))
        .matches("[0-9a-f]{64}");
  }

  /**
   * Pins the canonical form produced by the Jackson 2 releases. Stored hashes depend on it, so a
   * change here means a client retrying across an upgrade gets a false 409.
   */
  @Test
  void canonicalFormIsStableAcrossReleases() {
    Map<String, Object> attrs = new LinkedHashMap<>();
    attrs.put("z", 1);
    attrs.put("a", true);
    var payment =
        new Payment(
            "last",
            7,
            new BigDecimal("49.990"),
            Instant.parse("2026-01-02T03:04:05.123Z"),
            LocalDate.parse("2026-01-02"),
            Duration.ofSeconds(90),
            attrs,
            null);

    String jackson2Canonical =
        "{\"alpha\":7,\"amount\":49.990,\"at\":\"2026-01-02T03:04:05.123Z\","
            + "\"attrs\":{\"a\":true,\"z\":1},\"day\":\"2026-01-02\",\"nothing\":null,"
            + "\"ttl\":90.000000000,\"zeta\":\"last\"}";
    assertThat(hasher.hash(payment))
        .isEqualTo(hasher.hashRaw(jackson2Canonical))
        .isEqualTo("bba5c0dbc519b8796d9f8c616fbfe927fe36e9be71bc1d38cf167742aeec9dd6");
  }

  @Test
  void nullPayloadHashesConsistently() {
    assertThat(hasher.hash(null)).isEqualTo(hasher.hash(null)).matches("[0-9a-f]{64}");
  }
}
