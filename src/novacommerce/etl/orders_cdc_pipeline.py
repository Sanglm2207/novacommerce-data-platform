"""Trigger-journal incremental synchronization for NovaCommerce (training only)."""
from __future__ import annotations

import argparse
import logging
import os
import random
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.errors import DeadlockDetected, SerializationFailure, LockNotAvailable

load_dotenv(Path(__file__).resolve().parents[3] / '.env')
LOG = logging.getLogger('novacommerce.cdc')
RETRYABLE = (psycopg.OperationalError, DeadlockDetected, SerializationFailure, LockNotAvailable)


def connect():
    db = os.environ.get('POSTGRES_DB')
    if db != 'novacommerce_oltp':
        raise RuntimeError('Refusing to run outside novacommerce_oltp')
    return psycopg.connect(host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        dbname=db, user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'])


SELECT_CHANGES = '''
SELECT change_id, order_id
FROM etl.order_changes
WHERE processed_at IS NULL
ORDER BY change_id
LIMIT %s
FOR UPDATE SKIP LOCKED
'''

SELECT_ORDER = '''
SELECT o.order_id, o.customer_id, o.status, o.created_at,
       COALESCE(i.total_items, 0), COALESCE(i.total_amount, 0),
       EXISTS (SELECT 1 FROM public.payments p
               WHERE p.order_id = o.order_id AND p.status = 'SUCCESS')
FROM public.orders o
LEFT JOIN LATERAL (
    SELECT SUM(quantity) AS total_items, SUM(quantity * unit_price) AS total_amount
    FROM public.order_items oi WHERE oi.order_id = o.order_id
) i ON TRUE
WHERE o.order_id = %s
'''

UPSERT = '''
INSERT INTO staging.orders
(order_id, customer_id, order_status, order_date,
 total_items, total_amount, has_successful_payment)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (order_id) DO UPDATE SET
 customer_id=EXCLUDED.customer_id,
 order_status=EXCLUDED.order_status,
 order_date=EXCLUDED.order_date,
 total_items=EXCLUDED.total_items,
 total_amount=EXCLUDED.total_amount,
 has_successful_payment=EXCLUDED.has_successful_payment,
 loaded_at=NOW()
'''


def process_batch(batch_size: int, fail_after: int = 0) -> tuple[int, int]:
    # A short transaction: source recomputation, staging writes, and event acknowledgments.
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_CHANGES, (batch_size,))
            changes = cur.fetchall()
            if not changes:
                return 0, 0
            ids = list(dict.fromkeys(order_id for _, order_id in changes))
            for index, order_id in enumerate(ids, 1):
                cur.execute(SELECT_ORDER, (order_id,))
                record = cur.fetchone()
                if record is None:
                    cur.execute('DELETE FROM staging.orders WHERE order_id = %s', (order_id,))
                else:
                    cur.execute(UPSERT, record)
                if fail_after and index >= fail_after:
                    raise RuntimeError(f'Injected failure after {index} orders')

            change_ids = [change_id for change_id, _ in changes]
            cur.execute('UPDATE etl.order_changes SET processed_at = NOW() '
                        'WHERE change_id = ANY(%s)', (change_ids,))
            return len(changes), len(ids)


def run(batch_size: int, max_retries: int, fail_after: int):
    processed_events = processed_orders = 0
    while True:
        for attempt in range(1, max_retries + 1):
            try:
                events, orders = process_batch(batch_size, fail_after)
                break
            except RETRYABLE:
                if attempt == max_retries:
                    raise
                delay = min(2 ** (attempt - 1), 30) + random.uniform(0, 0.3)
                LOG.warning('Transient error; retry %d/%d in %.2fs', attempt, max_retries, delay)
                time.sleep(delay)
        if events == 0:
            break
        processed_events += events
        processed_orders += orders
        LOG.info('Committed batch: events=%d distinct_orders=%d', events, orders)
    LOG.info('Sync complete: events=%d distinct_orders_processed=%d', processed_events, processed_orders)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--max-retries', type=int, default=3)
    parser.add_argument('--fail-after', type=int, default=0)
    args = parser.parse_args()
    if args.batch_size < 1 or args.max_retries < 1 or args.fail_after < 0:
        parser.error('Invalid numeric parameters')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    run(args.batch_size, args.max_retries, args.fail_after)


if __name__ == '__main__':
    main()
