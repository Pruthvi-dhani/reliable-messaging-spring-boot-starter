package io.github.pruthvidhani.idempotencyoutbox.outbox.store.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxEvent;
import io.github.pruthvidhani.idempotencyoutbox.outbox.OutboxStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Postgres-backed {@link OutboxStore}.
 *
 * <p>{@code lockNextBatch} uses {@code SELECT ... FOR UPDATE SKIP LOCKED}: concurrent poller
 * instances each lock a disjoint set of due rows, so scaling out the service cannot double-publish
 * a row (while it is locked) — it must be called inside an active transaction that stays open
 * until the batch has been marked.
 */
public class JdbcOutboxStore implements OutboxStore {

  private static final String INSERT_SQL =
      """
      insert into outbox_events
             (id, aggregate_type, aggregate_id, event_type, payload, headers, status,
              attempts, next_attempt_at, created_at, published_at)
      values (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?, ?)
      """;

  /** Ordered by the DB-assigned sequence — created_at can collide and would order randomly. */
  private static final String LOCK_BATCH_SQL =
      """
      select id, aggregate_type, aggregate_id, event_type, payload, headers, status,
             attempts, next_attempt_at, created_at, published_at
        from outbox_events
       where status = 'PENDING' and next_attempt_at <= ?
       order by seq
       limit ?
         for update skip locked
      """;

  private static final String MARK_PUBLISHED_SQL =
      """
      update outbox_events
         set status = 'PUBLISHED', published_at = ?, next_attempt_at = null
       where id = ?
      """;

  private static final String RESCHEDULE_SQL =
      "update outbox_events set attempts = ?, next_attempt_at = ? where id = ?";

  private static final String MARK_DEAD_SQL =
      "update outbox_events set status = 'DEAD', next_attempt_at = null where id = ?";

  private static final String COUNT_PENDING_SQL =
      "select count(*) from outbox_events where status = 'PENDING'";

  private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {};

  private final JdbcTemplate jdbc;
  private final ObjectMapper headerMapper = new ObjectMapper();

  public JdbcOutboxStore(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public void record(OutboxEvent event) {
    jdbc.update(
        INSERT_SQL,
        event.id(),
        event.aggregateType(),
        event.aggregateId(),
        event.eventType(),
        event.payload(),
        writeHeaders(event.headers()),
        event.status().name(),
        event.attempts(),
        toTimestamp(event.nextAttemptAt()),
        toTimestamp(event.createdAt()),
        toTimestamp(event.publishedAt()));
  }

  @Override
  public List<OutboxEvent> lockNextBatch(int limit, Instant now) {
    return jdbc.query(LOCK_BATCH_SQL, this::mapRow, Timestamp.from(now), limit);
  }

  @Override
  public void markPublished(UUID id, Instant publishedAt) {
    jdbc.update(MARK_PUBLISHED_SQL, Timestamp.from(publishedAt), id);
  }

  @Override
  public void reschedule(UUID id, int attempts, Instant nextAttemptAt) {
    jdbc.update(RESCHEDULE_SQL, attempts, Timestamp.from(nextAttemptAt), id);
  }

  @Override
  public void markDead(UUID id) {
    jdbc.update(MARK_DEAD_SQL, id);
  }

  @Override
  public int countPending() {
    Integer count = jdbc.queryForObject(COUNT_PENDING_SQL, Integer.class);
    return count == null ? 0 : count;
  }

  private OutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new OutboxEvent(
        rs.getObject("id", UUID.class),
        rs.getString("aggregate_type"),
        rs.getString("aggregate_id"),
        rs.getString("event_type"),
        rs.getString("payload"),
        readHeaders(rs.getString("headers")),
        OutboxEvent.Status.valueOf(rs.getString("status")),
        rs.getInt("attempts"),
        toInstant(rs.getTimestamp("next_attempt_at")),
        toInstant(rs.getTimestamp("created_at")),
        toInstant(rs.getTimestamp("published_at")));
  }

  private String writeHeaders(Map<String, String> headers) {
    if (headers == null) {
      return null;
    }
    try {
      return headerMapper.writeValueAsString(headers);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Outbox headers are not serializable to JSON", e);
    }
  }

  private Map<String, String> readHeaders(String json) {
    if (json == null) {
      return Map.of();
    }
    try {
      return headerMapper.readValue(json, HEADERS_TYPE);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Corrupt outbox headers JSON", e);
    }
  }

  private static Timestamp toTimestamp(Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }

  private static Instant toInstant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }
}
