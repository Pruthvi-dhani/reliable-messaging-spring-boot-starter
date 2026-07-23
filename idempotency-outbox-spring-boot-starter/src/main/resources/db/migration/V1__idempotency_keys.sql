-- Dedupe table backing @Idempotent. One row per idempotency key.
--
-- status lifecycle: IN_PROGRESS (first request executing) -> COMPLETED (response cached).
-- Rows past expires_at are treated as absent (a retry after expiry is a fresh request)
-- and are physically removed by the sweeper (Stage 3).
create table idempotency_keys (
    idempotency_key  text        not null primary key,
    request_hash     text        not null,
    response_payload bytea,
    response_status  integer,
    status           text        not null check (status in ('IN_PROGRESS', 'COMPLETED')),
    created_at       timestamptz not null,
    expires_at       timestamptz not null
);

-- For the expiry sweeper (delete where expires_at <= now)
create index idx_idempotency_keys_expires_at on idempotency_keys (expires_at);
