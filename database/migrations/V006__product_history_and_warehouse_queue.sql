-- Additive migration: do not rewrite Day 09 dimensions/facts or Day 06 CDC.
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE TABLE IF NOT EXISTS warehouse.dim_product_history (
    product_version_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL,
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    price NUMERIC(15,2) NOT NULL,
    is_active BOOLEAN NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL,
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (is_current = (valid_to IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_history_current
    ON warehouse.dim_product_history(product_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_product_history_lookup
    ON warehouse.dim_product_history(product_id, valid_from DESC);

-- Dirty-key journal. Event rows are independent of Day 06's consumed queue.
CREATE TABLE IF NOT EXISTS warehouse.refresh_queue (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('PRODUCT','ORDER','CUSTOMER')),
    entity_id BIGINT NOT NULL,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    processed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_warehouse_refresh_pending
    ON warehouse.refresh_queue(event_id) WHERE processed_at IS NULL;

-- Captured product snapshots preserve multiple updates even before ETL runs.
CREATE TABLE IF NOT EXISTS warehouse.product_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL,
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    price NUMERIC(15,2) NOT NULL,
    is_active BOOLEAN NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    processed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_product_events_pending ON warehouse.product_events(event_id) WHERE processed_at IS NULL;

CREATE OR REPLACE FUNCTION warehouse.track_product() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' OR (OLD.sku,OLD.product_name,OLD.price,OLD.is_active)
        IS DISTINCT FROM (NEW.sku,NEW.product_name,NEW.price,NEW.is_active) THEN
        INSERT INTO warehouse.product_events(product_id,sku,product_name,price,is_active)
        VALUES(NEW.product_id,NEW.sku,NEW.product_name,NEW.price,NEW.is_active);
        INSERT INTO warehouse.refresh_queue(entity_type,entity_id)
        VALUES('PRODUCT',NEW.product_id);
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_warehouse_product ON public.products;
CREATE TRIGGER trg_warehouse_product AFTER INSERT OR UPDATE ON public.products
FOR EACH ROW EXECUTE FUNCTION warehouse.track_product();

CREATE OR REPLACE FUNCTION warehouse.track_order() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO warehouse.refresh_queue(entity_type,entity_id) VALUES('ORDER',OLD.order_id);
        RETURN OLD;
    END IF;
    IF TG_OP = 'INSERT' OR OLD IS DISTINCT FROM NEW THEN
        INSERT INTO warehouse.refresh_queue(entity_type,entity_id) VALUES('ORDER',NEW.order_id);
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_warehouse_order ON public.orders;
CREATE TRIGGER trg_warehouse_order AFTER INSERT OR UPDATE OR DELETE ON public.orders
FOR EACH ROW EXECUTE FUNCTION warehouse.track_order();

CREATE OR REPLACE FUNCTION warehouse.track_order_child() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO warehouse.refresh_queue(entity_type,entity_id)
    VALUES ('ORDER',COALESCE(NEW.order_id,OLD.order_id));
    IF TG_OP = 'UPDATE' AND NEW.order_id IS DISTINCT FROM OLD.order_id THEN
        INSERT INTO warehouse.refresh_queue(entity_type,entity_id) VALUES('ORDER',OLD.order_id);
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;
DROP TRIGGER IF EXISTS trg_warehouse_order_item ON public.order_items;
CREATE TRIGGER trg_warehouse_order_item AFTER INSERT OR UPDATE OR DELETE ON public.order_items
FOR EACH ROW EXECUTE FUNCTION warehouse.track_order_child();
DROP TRIGGER IF EXISTS trg_warehouse_payment ON public.payments;
CREATE TRIGGER trg_warehouse_payment AFTER INSERT OR UPDATE OR DELETE ON public.payments
FOR EACH ROW EXECUTE FUNCTION warehouse.track_order_child();

CREATE OR REPLACE FUNCTION warehouse.track_customer() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO warehouse.refresh_queue(entity_type,entity_id) VALUES('CUSTOMER',NEW.customer_id);
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_warehouse_customer ON public.customers;
CREATE TRIGGER trg_warehouse_customer AFTER INSERT OR UPDATE ON public.customers
FOR EACH ROW EXECUTE FUNCTION warehouse.track_customer();

-- One-time historical bootstrap for existing Day 09 rows.
-- This is an 'as observed' baseline: earlier source changes cannot be reconstructed.
INSERT INTO warehouse.product_events(product_id,sku,product_name,price,is_active)
SELECT p.product_id,p.sku,p.product_name,p.price,p.is_active
FROM public.products p
WHERE NOT EXISTS (
  SELECT 1 FROM warehouse.dim_product_history h WHERE h.product_id=p.product_id
) AND NOT EXISTS (
  SELECT 1 FROM warehouse.product_events e WHERE e.product_id=p.product_id AND e.processed_at IS NULL
);
INSERT INTO warehouse.refresh_queue(entity_type,entity_id)
SELECT 'ORDER', o.order_id FROM public.orders o
WHERE NOT EXISTS (
    SELECT 1 FROM warehouse.refresh_queue q
    WHERE q.entity_type='ORDER' AND q.entity_id=o.order_id AND q.processed_at IS NULL
);
