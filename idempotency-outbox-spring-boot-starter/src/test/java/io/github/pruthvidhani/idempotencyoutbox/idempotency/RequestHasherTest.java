package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RequestHasherTest {

  private final RequestHasher hasher = new RequestHasher();

  record OrderRequest(String customerId, int amountPence, String currency) {}

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

  @Test
  void nullPayloadHashesConsistently() {
    assertThat(hasher.hash(null)).isEqualTo(hasher.hash(null)).matches("[0-9a-f]{64}");
  }
}
