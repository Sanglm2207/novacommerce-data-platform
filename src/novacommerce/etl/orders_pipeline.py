"""NovaCommerce DATA-002 Day 05: checkpointed orders ETL.

Run from repository root:
  python src/novacommerce/etl/orders_pipeline.py run
  python src/novacommerce/etl/orders_pipeline.py reconcile --order-id 100

Required tables: public.orders, public.order_items, public.payments,
staging.orders, etl.checkpoints (see V002 migration).
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.errors import DeadlockDetected, SerializationFailure

ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env")

PIPELINE_NAME = "orders_pipeline"
LOCK_KEY = 20261002
DEFAULT_BATCH_SIZE = 5_000
MAX_RETRIES = 3
RETRYABLE_ERRORS = (
    psycopg.OperationalError,
    DeadlockDetected,
    SerializationFailure,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("novacommerce.etl.orders")

EXTRACT_SQL = """
WITH batch AS (
    SELECT o.order_id, o.customer_id, o.status, o.created_at
    FROM public.orders o
    WHERE (o.created_at, o.order_id) > (%s::timestamptz, %s::bigint)
    ORDER BY o.created_at, o.order_id
    LIMIT %s
)
SELECT
    b.order_id, b.customer_id, b.status, b.created_at,
    COALESCE(i.total_items, 0),
    COALESCE(i.total_amount, 0::numeric),
    EXISTS (
        SELECT 1 FROM public.payments p
        WHERE p.order_id = b.order_id AND p.status = 'SUCCESS'
    ) AS has_successful_payment
FROM batch b
LEFT JOIN LATERAL (
    SELECT SUM(oi.quantity) AS total_items,
           SUM(oi.quantity * oi.unit_price) AS total_amount
    FROM public.order_items oi
    WHERE oi.order_id = b.order_id
) i ON TRUE
ORDER BY b.created_at, b.order_id
"""

RECONCILE_SQL = """
SELECT
    o.order_id, o.customer_id, o.status, o.created_at,
    COALESCE(i.total_items, 0),
    COALESCE(i.total_amount, 0::numeric),
    EXISTS (
        SELECT 1 FROM public.payments p
        WHERE p.order_id = o.order_id AND p.status = 'SUCCESS'
    ) AS has_successful_payment
FROM public.orders o
LEFT JOIN LATERAL (
    SELECT SUM(oi.quantity) AS total_items,
           SUM(oi.quantity * oi.unit_price) AS total_amount
    FROM public.order_items oi
    WHERE oi.order_id = o.order_id
) i ON TRUE
WHERE o.order_id = %s
"""

UPSERT_SQL = """
INSERT INTO staging.orders (
    order_id, customer_id, order_status, order_date,
    total_items, total_amount, has_successful_payment
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    order_status = EXCLUDED.order_status,
    order_date = EXCLUDED.order_date,
    total_items = EXCLUDED.total_items,
    total_amount = EXCLUDED.total_amount,
    has_successful_payment = EXCLUDED.has_successful_payment,
    loaded_at = NOW()
"""


def get_connection() -> psycopg.Connection:
    required = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing .env configuration: {', '.join(missing)}")
    if os.getenv("POSTGRES_DB") != "novacommerce_oltp":
        raise ValueError("Refusing to run outside novacommerce_oltp training database")
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
        application_name="novacommerce_orders_etl",
    )


def run_once(batch_size: int, fail_after_rows: int) -> int:
    processed = 0
    # REPEATABLE READ gives a stable source snapshot across batches;
    # the entire run, including checkpoint, is committed atomically.
    with get_connection() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
            cur.execute(
                "SELECT last_created_at, last_order_id "
                "FROM etl.checkpoints WHERE pipeline_name = %s FOR UPDATE",
                (PIPELINE_NAME,),
            )
            checkpoint = cur.fetchone()
            if checkpoint is None:
                raise RuntimeError("Checkpoint missing: execute V002 migration first")
            last_created_at, last_order_id = checkpoint

            while True:
                cur.execute(EXTRACT_SQL, (last_created_at, last_order_id, batch_size))
                rows = cur.fetchall()
                if not rows:
                    break
                cur.executemany(UPSERT_SQL, rows)
                last_order_id, last_created_at = rows[-1][0], rows[-1][3]
                cur.execute(
                    "UPDATE etl.checkpoints "
                    "SET last_created_at = %s, last_order_id = %s, updated_at = NOW() "
                    "WHERE pipeline_name = %s",
                    (last_created_at, last_order_id, PIPELINE_NAME),
                )
                processed += len(rows)
                logger.info("batch=%d total=%d", len(rows), processed)
                if fail_after_rows and processed >= fail_after_rows:
                    raise RuntimeError(
                        f"Injected failure after {processed} rows (rollback expected)"
                    )
    return processed


def reconcile_once(order_id: int) -> None:
    if order_id <= 0:
        raise ValueError("order-id must be positive")
    with get_connection() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
            cur.execute(RECONCILE_SQL, (order_id,))
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Source order {order_id} does not exist")
            cur.execute(UPSERT_SQL, row)


def with_retry(action, description: str) -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        started = time.perf_counter()
        try:
            logger.info("%s | attempt=%d/%d", description, attempt, MAX_RETRIES)
            action()
            logger.info("%s | success | duration=%.2fs", description,
                        time.perf_counter() - started)
            return
        except RETRYABLE_ERRORS as exc:
            if attempt == MAX_RETRIES:
                logger.exception("%s | retry budget exhausted", description)
                raise
            delay = min(2 ** (attempt - 1), 30) + random.uniform(0, 0.5)
            logger.warning("Transient error: %s; retrying in %.2fs", exc, delay)
            time.sleep(delay)


def main() -> None:
    parser = argparse.ArgumentParser(description="NovaCommerce orders ETL")
    sub = parser.add_subparsers(dest="command")
    run = sub.add_parser("run", help="Load new orders incrementally")
    run.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    run.add_argument("--fail-after-rows", type=int, default=0,
                     help="Inject failure for rollback testing (training only)")
    rec = sub.add_parser("reconcile", help="Refresh one existing order")
    rec.add_argument("--order-id", type=int, required=True)
    args = parser.parse_args()

    if args.command in (None, "run"):
        batch_size = getattr(args, "batch_size", DEFAULT_BATCH_SIZE)
        fail_after = getattr(args, "fail_after_rows", 0)
        if batch_size <= 0 or fail_after < 0:
            parser.error("batch-size must be positive and fail-after-rows >= 0")

        def job() -> None:
            count = run_once(batch_size, fail_after)
            logger.info("COMMITTED rows=%d", count)

        with_retry(job, "orders incremental load")
    else:
        with_retry(lambda: reconcile_once(args.order_id),
                   f"reconcile order_id={args.order_id}")


if __name__ == "__main__":
    main()
