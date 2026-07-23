package io.github.pruthvidhani.idempotencyoutbox.idempotency.store.jdbc;

import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyRecord;
import io.github.pruthvidhani.idempotencyoutbox.idempotency.IdempotencyStore;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Postgres-backed {@link IdempotencyStore}.
 *
 * <p>The concurrency gate is {@code INSERT ... ON CONFLICT}: exactly one concurrent caller wins the
 * claim for a key. An existing row that is past its {@code expires_at} is treated as absent — the
 * insert "steals" it (conditional {@code DO UPDATE}) so an expired key behaves like a fresh
 * request without waiting for the sweeper.
 */
public class JdbcIdempotencyStore implements IdempotencyStore {

  private static final String FIND_SQL =
      """
      select idempotency_key, request_hash, response_payload, response_status, status,
             created_at, expires_at
        from idempotency_keys
       where idempotency_key = ? and expires_at > ?
      """;

  /** Insert wins; conflict on a live row loses; conflict on an expired row steals it. */
  private static final String CLAIM_SQL =
      """
      insert into idempotency_keys
             (idempotency_key, request_hash, response_payload, response_status, status,
              created_at, expires_at)
      values (?, ?, null, null, 'IN_PROGRESS', ?, ?)
      on conflict (idempotency_key) do update
         set request_hash = excluded.request_hash,
             response_payload = null,
             response_status = null,
             status = 'IN_PROGRESS',
             created_at = excluded.created_at,
             expires_at = excluded.expires_at
       where idempotency_keys.expires_at <= excluded.created_at
      """;

  private static final String COMPLETE_SQL =
      """
      update idempotency_keys
         set response_payload = ?, response_status = ?, status = 'COMPLETED'
       where idempotency_key = ?
      """;

  private static final String REMOVE_SQL =
      "delete from idempotency_keys where idempotency_key = ?";

  private static final String DELETE_EXPIRED_SQL =
      "delete from idempotency_keys where expires_at <= ?";

  private final JdbcTemplate jdbc;
  private final Clock clock;

  public JdbcIdempotencyStore(JdbcTemplate jdbc, Clock clock) {
    this.jdbc = jdbc;
    this.clock = clock;
  }

  @Override
  public Optional<IdempotencyRecord> find(String key) {
    var results =
        jdbc.query(
            FIND_SQL,
            (rs, rowNum) ->
                new IdempotencyRecord(
                    rs.getString("idempotency_key"),
                    rs.getString("request_hash"),
                    rs.getBytes("response_payload"),
                    rs.getObject("response_status", Integer.class),
                    IdempotencyRecord.Status.valueOf(rs.getString("status")),
                    rs.getTimestamp("created_at").toInstant(),
                    rs.getTimestamp("expires_at").toInstant()),
            key,
            Timestamp.from(clock.instant()));
    return results.stream().findFirst();
  }

  @Override
  public boolean putInProgress(String key, String requestHash, Instant createdAt, Instant expiresAt) {
    int rows =
        jdbc.update(
            CLAIM_SQL, key, requestHash, Timestamp.from(createdAt), Timestamp.from(expiresAt));
    return rows == 1;
  }

  @Override
  public void complete(String key, byte[] responsePayload, Integer responseStatus) {
    jdbc.update(COMPLETE_SQL, responsePayload, responseStatus, key);
  }

  @Override
  public void remove(String key) {
    jdbc.update(REMOVE_SQL, key);
  }

  @Override
  public int deleteExpired(Instant now) {
    return jdbc.update(DELETE_EXPIRED_SQL, Timestamp.from(now));
  }
}
