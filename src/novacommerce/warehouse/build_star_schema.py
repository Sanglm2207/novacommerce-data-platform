"""Day 09: Initial/full-scan SCD1 dimension + order-item fact load.

Runs against the *existing* NovaCommerce PostgreSQL DB. Does not modify
OLTP, staging, CDC queues, or their checkpoint tables.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env")
LOG = logging.getLogger("novacommerce.warehouse")


def get_connection() -> psycopg.Connection:
    if os.getenv("POSTGRES_DB") != "novacommerce_oltp":
        raise RuntimeError("Expected POSTGRES_DB=novacommerce_oltp")
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


SQL_STEPS = (
    ("dim_customer", """
        INSERT INTO warehouse.dim_customer
          (customer_id, full_name, email, source_created_at)
        SELECT customer_id, full_name, email, created_at FROM public.customers
        ON CONFLICT (customer_id) DO UPDATE SET
          full_name = EXCLUDED.full_name,
          email = EXCLUDED.email,
          source_created_at = EXCLUDED.source_created_at,
          refreshed_at = NOW()
        WHERE (warehouse.dim_customer.full_name, warehouse.dim_customer.email,
               warehouse.dim_customer.source_created_at)
           IS DISTINCT FROM (EXCLUDED.full_name, EXCLUDED.email,
                             EXCLUDED.source_created_at)
    """),
    ("dim_product", """
        INSERT INTO warehouse.dim_product
          (product_id, sku, product_name, current_price, is_active)
        SELECT product_id, sku, product_name, price, is_active FROM public.products
        ON CONFLICT (product_id) DO UPDATE SET
          sku = EXCLUDED.sku,
          product_name = EXCLUDED.product_name,
          current_price = EXCLUDED.current_price,
          is_active = EXCLUDED.is_active,
          refreshed_at = NOW()
        WHERE (warehouse.dim_product.sku, warehouse.dim_product.product_name,
               warehouse.dim_product.current_price, warehouse.dim_product.is_active)
           IS DISTINCT FROM (EXCLUDED.sku, EXCLUDED.product_name,
                             EXCLUDED.current_price, EXCLUDED.is_active)
    """),
    ("dim_date", """
        INSERT INTO warehouse.dim_date
          (date_key, full_date, calendar_year, calendar_month,
           calendar_day, calendar_quarter, iso_week)
        SELECT DISTINCT
          TO_CHAR(order_date AT TIME ZONE 'UTC', 'YYYYMMDD')::integer,
          (order_date AT TIME ZONE 'UTC')::date,
          EXTRACT(YEAR FROM order_date AT TIME ZONE 'UTC')::integer,
          EXTRACT(MONTH FROM order_date AT TIME ZONE 'UTC')::integer,
          EXTRACT(DAY FROM order_date AT TIME ZONE 'UTC')::integer,
          EXTRACT(QUARTER FROM order_date AT TIME ZONE 'UTC')::integer,
          EXTRACT(WEEK FROM order_date AT TIME ZONE 'UTC')::integer
        FROM staging.orders
        ON CONFLICT (date_key) DO NOTHING
    """),
    ("fact_order_items", """
        INSERT INTO warehouse.fact_order_items
          (order_item_id, order_id, customer_key, product_key,
           order_date_key, order_status, has_successful_payment,
           quantity, unit_price)
        SELECT oi.order_item_id, oi.order_id, dc.customer_key, dp.product_key,
          TO_CHAR(s.order_date AT TIME ZONE 'UTC', 'YYYYMMDD')::integer,
          s.order_status, s.has_successful_payment, oi.quantity, oi.unit_price
        FROM public.order_items AS oi
        JOIN staging.orders AS s ON s.order_id = oi.order_id
        JOIN warehouse.dim_customer AS dc ON dc.customer_id = s.customer_id
        JOIN warehouse.dim_product AS dp ON dp.product_id = oi.product_id
        ON CONFLICT (order_item_id) DO UPDATE SET
          order_id = EXCLUDED.order_id,
          customer_key = EXCLUDED.customer_key,
          product_key = EXCLUDED.product_key,
          order_date_key = EXCLUDED.order_date_key,
          order_status = EXCLUDED.order_status,
          has_successful_payment = EXCLUDED.has_successful_payment,
          quantity = EXCLUDED.quantity,
          unit_price = EXCLUDED.unit_price,
          refreshed_at = NOW()
        WHERE (warehouse.fact_order_items.order_id,
               warehouse.fact_order_items.customer_key,
               warehouse.fact_order_items.product_key,
               warehouse.fact_order_items.order_date_key,
               warehouse.fact_order_items.order_status,
               warehouse.fact_order_items.has_successful_payment,
               warehouse.fact_order_items.quantity,
               warehouse.fact_order_items.unit_price)
           IS DISTINCT FROM
              (EXCLUDED.order_id, EXCLUDED.customer_key, EXCLUDED.product_key,
               EXCLUDED.order_date_key, EXCLUDED.order_status,
               EXCLUDED.has_successful_payment, EXCLUDED.quantity,
               EXCLUDED.unit_price)
    """),
)


def run() -> None:
    started = time.perf_counter()
    # Snapshot consistency across all SQL statements during this build.
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (20261009,))
            cur.execute("SELECT COUNT(*) FROM public.orders")
            source_orders = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.orders")
            stage_orders = cur.fetchone()[0]
            if source_orders != stage_orders:
                raise RuntimeError(
                    f"Incomplete staging: source={source_orders}, staging={stage_orders}. "
                    "Run Day 06 CDC sync + Day 07 validation first."
                )
            # This training load checks counts as a gate. The Day 07 validator
            # must also be run to establish status/value consistency.
            for label, sql in SQL_STEPS:
                cur.execute(sql)
                LOG.info("Loaded %s: inserted_or_updated=%s", label, cur.rowcount)
            cur.execute("SELECT COUNT(*) FROM warehouse.fact_order_items")
            LOG.info("Fact rows total=%s", cur.fetchone()[0])
    LOG.info("Warehouse build committed; elapsed=%.2fs", time.perf_counter()-started)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
