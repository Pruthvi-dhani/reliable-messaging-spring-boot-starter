-- The example app's own business table. Versioned from V1000 so app migrations never collide
-- with the starter's shipped migrations (V1, V2, ...).
create table orders (
    order_id     text        not null primary key,
    customer_id  text        not null,
    amount_pence integer     not null,
    currency     text        not null,
    created_at   timestamptz not null default now()
);
