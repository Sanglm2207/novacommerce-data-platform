
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_status VARCHAR(30) NOT NULL,
    order_date TIMESTAMPTZ NOT NULL,
    total_items BIGINT NOT NULL,
    total_amount NUMERIC(18, 2) NOT NULL,
    has_successful_payment BOOLEAN NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl.checkpoints (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_created_at TIMESTAMPTZ NOT NULL,
    last_order_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO etl.checkpoints (
    pipeline_name,
    last_created_at,
    last_order_id
)
VALUES (
    'orders_pipeline',
    '-infinity'::timestamptz,
    0
)
ON CONFLICT (pipeline_name) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_source_orders_watermark
ON public.orders(created_at, order_id);
