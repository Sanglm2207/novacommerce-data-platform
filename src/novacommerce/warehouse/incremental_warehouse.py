"""Day 10 incremental warehouse loader: additive to Day 09, not a replacement.

Run Day 06 CDC sync first. Warehouse product SCD2 stores the history of
*observed* changes; Day 09 SCD1 dim_product and fact FK are preserved.
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import timedelta
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[3] / '.env')
LOG = logging.getLogger('warehouse.incremental')
LOCK_KEY = 20261010


def connect():
    if os.getenv('POSTGRES_DB') != 'novacommerce_oltp':
        raise RuntimeError('Use POSTGRES_DB=novacommerce_oltp training database')
    return psycopg.connect(
        host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )


def process_batch(conn, size: int, fail_after: bool = False) -> tuple[int, int]:
    """One atomic batch: source -> dimensions/facts -> processed markers."""
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK_KEY,))
            # Don't warehouse stale staging rows while Day 06 is still behind.
            cur.execute('SELECT EXISTS(SELECT 1 FROM etl.order_changes WHERE processed_at IS NULL)')
            if cur.fetchone()[0]:
                raise RuntimeError('Day 06 CDC has pending events; sync staging first')

            cur.execute('''SELECT event_id, product_id, sku, product_name, price,
                                  is_active, captured_at
                           FROM warehouse.product_events
                           WHERE processed_at IS NULL ORDER BY event_id
                           LIMIT %s FOR UPDATE SKIP LOCKED''', (size,))
            product_events = cur.fetchall()
            for event_id, pid, sku, name, price, active, captured_at in product_events:
                cur.execute('''SELECT product_version_key,sku,product_name,price,
                                      is_active,valid_from
                               FROM warehouse.dim_product_history
                               WHERE product_id=%s AND is_current FOR UPDATE''', (pid,))
                prior = cur.fetchone()
                changed = prior is None or prior[1:5] != (sku, name, price, active)
                if changed:
                    start = captured_at
                    if prior:
                        # Avoid invalid/zero time windows if events have equal timestamps.
                        start = max(start, prior[5] + timedelta(microseconds=1))
                        cur.execute('''UPDATE warehouse.dim_product_history
                                       SET valid_to=%s, is_current=FALSE
                                       WHERE product_version_key=%s''', (start, prior[0]))
                    else:
                        # Initial history is only a baseline, not original creation time.
                        start = '-infinity'
                    cur.execute('''INSERT INTO warehouse.dim_product_history
                        (product_id,sku,product_name,price,is_active,valid_from,is_current)
                        VALUES (%s,%s,%s,%s,%s,%s,TRUE)''',
                        (pid, sku, name, price, active, start))
            if product_events:
                cur.execute('''UPDATE warehouse.product_events SET processed_at=NOW()
                               WHERE event_id = ANY(%s)''',
                            ([x[0] for x in product_events],))

            cur.execute('''SELECT event_id,entity_type,entity_id
                           FROM warehouse.refresh_queue
                           WHERE processed_at IS NULL ORDER BY event_id
                           LIMIT %s FOR UPDATE SKIP LOCKED''', (size,))
            jobs = cur.fetchall()
            customer_ids = sorted({eid for _, kind, eid in jobs if kind == 'CUSTOMER'})
            product_ids = sorted({eid for _, kind, eid in jobs if kind == 'PRODUCT'})
            order_ids = sorted({eid for _, kind, eid in jobs if kind == 'ORDER'})

            if customer_ids:
                cur.execute('''INSERT INTO warehouse.dim_customer
                   (customer_id,full_name,email,source_created_at)
                   SELECT customer_id,full_name,email,created_at FROM public.customers
                   WHERE customer_id=ANY(%s)
                   ON CONFLICT(customer_id) DO UPDATE SET
                     full_name=EXCLUDED.full_name,email=EXCLUDED.email,
                     source_created_at=EXCLUDED.source_created_at,refreshed_at=NOW()
                   WHERE (warehouse.dim_customer.full_name,warehouse.dim_customer.email,
                          warehouse.dim_customer.source_created_at)
                     IS DISTINCT FROM (EXCLUDED.full_name,EXCLUDED.email,
                                       EXCLUDED.source_created_at)''', (customer_ids,))
            if product_ids:
                cur.execute('''INSERT INTO warehouse.dim_product
                    (product_id,sku,product_name,current_price,is_active)
                    SELECT product_id,sku,product_name,price,is_active
                    FROM public.products WHERE product_id=ANY(%s)
                    ON CONFLICT(product_id) DO UPDATE SET
                      sku=EXCLUDED.sku,product_name=EXCLUDED.product_name,
                      current_price=EXCLUDED.current_price,is_active=EXCLUDED.is_active,
                      refreshed_at=NOW()
                    WHERE (warehouse.dim_product.sku,warehouse.dim_product.product_name,
                           warehouse.dim_product.current_price,warehouse.dim_product.is_active)
                      IS DISTINCT FROM (EXCLUDED.sku,EXCLUDED.product_name,
                                        EXCLUDED.current_price,EXCLUDED.is_active)''', (product_ids,))

            if order_ids:
                # Remove existing order lines for affected orders; reload current state.
                # Handles source deleted items and (if allowed) deleted orders.
                cur.execute('DELETE FROM warehouse.fact_order_items WHERE order_id=ANY(%s)', (order_ids,))
                cur.execute('''INSERT INTO warehouse.dim_customer
                    (customer_id,full_name,email,source_created_at)
                    SELECT DISTINCT c.customer_id,c.full_name,c.email,c.created_at
                    FROM public.customers c
                    JOIN staging.orders s ON s.customer_id=c.customer_id
                    WHERE s.order_id=ANY(%s)
                    ON CONFLICT(customer_id) DO NOTHING''', (order_ids,))
                cur.execute('''INSERT INTO warehouse.dim_product
                    (product_id,sku,product_name,current_price,is_active)
                    SELECT DISTINCT p.product_id,p.sku,p.product_name,p.price,p.is_active
                    FROM public.products p
                    JOIN public.order_items oi ON oi.product_id=p.product_id
                    WHERE oi.order_id=ANY(%s)
                    ON CONFLICT(product_id) DO NOTHING''', (order_ids,))
                cur.execute('''INSERT INTO warehouse.dim_date
                    (date_key,full_date,calendar_year,calendar_month,
                     calendar_day,calendar_quarter,iso_week)
                    SELECT DISTINCT
                      TO_CHAR(s.order_date AT TIME ZONE 'UTC','YYYYMMDD')::integer,
                      (s.order_date AT TIME ZONE 'UTC')::date,
                      EXTRACT(YEAR FROM s.order_date AT TIME ZONE 'UTC')::integer,
                      EXTRACT(MONTH FROM s.order_date AT TIME ZONE 'UTC')::integer,
                      EXTRACT(DAY FROM s.order_date AT TIME ZONE 'UTC')::integer,
                      EXTRACT(QUARTER FROM s.order_date AT TIME ZONE 'UTC')::integer,
                      EXTRACT(WEEK FROM s.order_date AT TIME ZONE 'UTC')::integer
                    FROM staging.orders s WHERE s.order_id=ANY(%s)
                    ON CONFLICT(date_key) DO NOTHING''', (order_ids,))
                cur.execute('''INSERT INTO warehouse.fact_order_items
                    (order_item_id,order_id,customer_key,product_key,order_date_key,
                     order_status,has_successful_payment,quantity,unit_price)
                    SELECT oi.order_item_id,oi.order_id,dc.customer_key,dp.product_key,
                       TO_CHAR(s.order_date AT TIME ZONE 'UTC','YYYYMMDD')::integer,
                       s.order_status,s.has_successful_payment,oi.quantity,oi.unit_price
                    FROM public.order_items oi
                    JOIN staging.orders s ON s.order_id=oi.order_id
                    JOIN warehouse.dim_customer dc ON dc.customer_id=s.customer_id
                    JOIN warehouse.dim_product dp ON dp.product_id=oi.product_id
                    WHERE oi.order_id=ANY(%s)''', (order_ids,))

            if fail_after:
                raise RuntimeError('Injected failure: rollback batch expected')
            if jobs:
                cur.execute('''UPDATE warehouse.refresh_queue SET processed_at=NOW()
                               WHERE event_id=ANY(%s)''', ([x[0] for x in jobs],))
    return len(product_events), len(jobs)


def run(size: int, fail: bool):
    if size <= 0:
        raise ValueError('--batch-size must be positive')
    start = time.perf_counter()
    totals = [0, 0]
    with connect() as conn:
        while True:
            products, events = process_batch(conn, size, fail)
            totals[0] += products
            totals[1] += events
            LOG.info('Committed product_events=%s refresh_events=%s', products, events)
            if not products and not events:
                break
    LOG.info('DONE product_events=%s refresh_events=%s elapsed=%.2fs',
             *totals, time.perf_counter() - start)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--fail-after-batch', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    run(args.batch_size, args.fail_after_batch)
