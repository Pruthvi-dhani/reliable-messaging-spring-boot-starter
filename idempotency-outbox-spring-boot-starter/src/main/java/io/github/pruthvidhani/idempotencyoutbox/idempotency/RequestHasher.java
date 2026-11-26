package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Computes the request hash used to detect same-key/different-body conflicts.
 *
 * <p>The payload is serialized to <b>canonical JSON</b> — POJO properties sorted alphabetically,
 * map entries sorted by key — so that logically identical requests always produce the same bytes
 * regardless of field/entry ordering. The canonical bytes are hashed with SHA-256 and returned as
 * lowercase hex.
 *
 * <p>The mapper starts from Jackson 2 defaults so hashes stay identical to those computed by
 * earlier releases: a client retrying across an upgrade must not see a false 409.
 */
public class RequestHasher {

  private final ObjectMapper canonicalMapper;

  public RequestHasher() {
    this.canonicalMapper =
        JsonMapper.builderWithJackson2Defaults()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            // Jackson 3 treats a record's canonical constructor as a creator and would emit its
            // components first, in declaration order. Jackson 2 sorted records fully
            // alphabetically, and stored hashes depend on that.
            .configure(MapperFeature.SORT_CREATOR_PROPERTIES_FIRST, false)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .build();
  }

  /**
   * Hashes the given payload (typically the intercepted method's argument array or a selected
   * argument). Null hashes to the canonical hash of JSON {@code null}.
   */
  public String hash(Object payload) {
    try {
      byte[] canonicalJson = canonicalMapper.writeValueAsBytes(payload);
      return HexFormat.of().formatHex(sha256().digest(canonicalJson));
    } catch (JacksonException e) {
      throw new IllegalStateException(
          "Failed to serialize request payload for idempotency hashing", e);
    }
  }

  /** Hashes an already-serialized payload without re-canonicalizing (used in tests/tools). */
  public String hashRaw(String payload) {
    return HexFormat.of().formatHex(sha256().digest(payload.getBytes(StandardCharsets.UTF_8)));
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
