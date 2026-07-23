-- Transactional outbox. Events are inserted in the SAME transaction as the business write,
-- then published to the broker by a background poller.
--
-- status lifecycle: PENDING -> PUBLISHED (success, terminal)
--                   PENDING -> ... retries with backoff ... -> DEAD (exhausted, terminal)
create table outbox_events (
    id              uuid        not null primary key,
    -- Publish order. A DB-assigned sequence, NOT created_at: timestamps can collide within
    -- a millisecond and would then order randomly, breaking per-aggregate ordering.
    seq             bigint      generated always as identity,
    aggregate_type  text        not null,
    aggregate_id    text        not null,
    event_type      text        not null,
    payload         jsonb       not null,
    headers         jsonb,
    status          text        not null check (status in ('PENDING', 'PUBLISHED', 'DEAD')),
    attempts        integer     not null default 0,
    next_attempt_at timestamptz,
    created_at      timestamptz not null,
    published_at    timestamptz
);

-- For the poller: PENDING events due for an attempt, oldest first
create index idx_outbox_events_pending on outbox_events (status, next_attempt_at, seq);
