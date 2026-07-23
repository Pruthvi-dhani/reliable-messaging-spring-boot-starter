package io.github.pruthvidhani.idempotencyoutbox.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Computes the request hash used to detect same-key/different-body conflicts.
 *
 * <p>The payload is serialized to <b>canonical JSON</b> — POJO properties sorted alphabetically,
 * map entries sorted by key — so that logically identical requests always produce the same bytes
 * regardless of field/entry ordering. The canonical bytes are hashed with SHA-256 and returned as
 * lowercase hex.
 */
public class RequestHasher {

  private final ObjectMapper canonicalMapper;

  public RequestHasher() {
    this.canonicalMapper =
        JsonMapper.builder()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .addModule(new JavaTimeModule())
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
    } catch (JsonProcessingException e) {
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
