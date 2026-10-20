CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL UNIQUE,
    full_name VARCHAR(150) NOT NULL,
    email VARCHAR(255) NOT NULL,
    source_created_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL UNIQUE,
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    current_price NUMERIC(15,2) NOT NULL CHECK (current_price >= 0),
    is_active BOOLEAN NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD (UTC)
    full_date DATE NOT NULL UNIQUE,
    calendar_year INTEGER NOT NULL,
    calendar_month INTEGER NOT NULL,
    calendar_day INTEGER NOT NULL,
    calendar_quarter INTEGER NOT NULL,
    iso_week INTEGER NOT NULL
);

-- Grain: exactly one source order_items row.
CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_item_id BIGINT PRIMARY KEY, -- lineage + idempotent natural row key
    order_id BIGINT NOT NULL,
    customer_key BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_key),
    product_key BIGINT NOT NULL REFERENCES warehouse.dim_product(product_key),
    order_date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    order_status VARCHAR(30) NOT NULL,
    has_successful_payment BOOLEAN NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(15,2) NOT NULL CHECK (unit_price >= 0),
    line_amount NUMERIC(18,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_order_date ON warehouse.fact_order_items(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON warehouse.fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON warehouse.fact_order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_id ON warehouse.fact_order_items(order_id);
