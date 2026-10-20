-- Run after V001 and V002. Training database migration.
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.order_changes (
  change_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_id bigint NOT NULL,
  source_table text NOT NULL,
  operation text NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE','BACKFILL')),
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  processed_at timestamptz
);
CREATE INDEX IF NOT EXISTS idx_order_changes_pending
  ON etl.order_changes (change_id) WHERE processed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_order_changes_order
  ON etl.order_changes (order_id);

CREATE OR REPLACE FUNCTION etl.enqueue_order_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_order_id bigint;
BEGIN
  affected_order_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.order_id ELSE NEW.order_id END;
  INSERT INTO etl.order_changes(order_id, source_table, operation)
  VALUES (affected_order_id, TG_TABLE_NAME, TG_OP);
  RETURN COALESCE(NEW, OLD);
END;
$$;

DROP TRIGGER IF EXISTS trg_orders_change ON public.orders;
CREATE TRIGGER trg_orders_change AFTER INSERT OR UPDATE OR DELETE ON public.orders
FOR EACH ROW EXECUTE FUNCTION etl.enqueue_order_change();

DROP TRIGGER IF EXISTS trg_order_items_change ON public.order_items;
CREATE TRIGGER trg_order_items_change AFTER INSERT OR UPDATE OR DELETE ON public.order_items
FOR EACH ROW EXECUTE FUNCTION etl.enqueue_order_change();

DROP TRIGGER IF EXISTS trg_payments_change ON public.payments;
CREATE TRIGGER trg_payments_change AFTER INSERT OR UPDATE OR DELETE ON public.payments
FOR EACH ROW EXECUTE FUNCTION etl.enqueue_order_change();

-- One-time reconciliation, after installing triggers. Optional but strongly recommended.
-- INSERT INTO etl.order_changes(order_id, source_table, operation)
-- SELECT order_id, 'bootstrap', 'BACKFILL' FROM public.orders;
