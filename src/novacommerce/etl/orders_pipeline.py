from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env")

BATCH_SIZE = 5_000
PIPELINE_NAME = "orders_pipeline"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


EXTRACT_SQL = """
    WITH batch AS (
        SELECT
            order_id,
            customer_id,
            status,
            created_at
        FROM public.orders
        WHERE (created_at, order_id) > (%s::timestamptz, %s)
        ORDER BY created_at, order_id
        LIMIT %s
    )
    SELECT
        b.order_id,
        b.customer_id,
        b.status,
        b.created_at,
        COALESCE(items.total_items, 0),
        COALESCE(items.total_amount, 0),
        EXISTS (
            SELECT 1
            FROM public.payments p
            WHERE p.order_id = b.order_id
              AND p.status = 'SUCCESS'
        ) AS has_successful_payment
    FROM batch b
    LEFT JOIN LATERAL (
        SELECT
            SUM(oi.quantity) AS total_items,
            SUM(oi.quantity * oi.unit_price) AS total_amount
        FROM public.order_items oi
        WHERE oi.order_id = b.order_id
    ) items ON TRUE
    ORDER BY b.created_at, b.order_id
"""


UPSERT_SQL = """
    INSERT INTO staging.orders (
        order_id,
        customer_id,
        order_status,
        order_date,
        total_items,
        total_amount,
        has_successful_payment
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id)
    DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        order_status = EXCLUDED.order_status,
        order_date = EXCLUDED.order_date,
        total_items = EXCLUDED.total_items,
        total_amount = EXCLUDED.total_amount,
        has_successful_payment =
            EXCLUDED.has_successful_payment,
        loaded_at = NOW()
"""


def get_connection() -> psycopg.Connection:
    required = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")

    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing environment variables: {missing}")

    if os.environ["POSTGRES_DB"] != "novacommerce_oltp":
        raise ValueError("Expected training database: novacommerce_oltp")

    return psycopg.connect(
        host="127.0.0.1",
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def run_pipeline() -> None:
    started = time.perf_counter()
    total_processed = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Prevent concurrent runs of this pipeline.
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (20261002,),
            )

            cur.execute(
                """
                -- Python datetime cannot represent PostgreSQL +/-infinity.
                -- Keep the initial watermark as text and cast it in EXTRACT_SQL.
                SELECT last_created_at::text, last_order_id
                FROM etl.checkpoints
                WHERE pipeline_name = %s
                FOR UPDATE
                """,
                (PIPELINE_NAME,),
            )
            checkpoint = cur.fetchone()

            if checkpoint is None:
                raise RuntimeError("ETL checkpoint not initialized")

            last_created_at, last_order_id = checkpoint

            while True:
                cur.execute(
                    EXTRACT_SQL,
                    (last_created_at, last_order_id, BATCH_SIZE),
                )
                records = cur.fetchall()

                if not records:
                    break

                cur.executemany(UPSERT_SQL, records)

                last_order = records[-1]
                last_order_id = last_order[0]
                last_created_at = last_order[3]

                cur.execute(
                    """
                    UPDATE etl.checkpoints
                    SET last_created_at = %s,
                        last_order_id = %s,
                        updated_at = NOW()
                    WHERE pipeline_name = %s
                    """,
                    (
                        last_created_at,
                        last_order_id,
                        PIPELINE_NAME,
                    ),
                )

                total_processed += len(records)

                logger.info(
                    "Processed batch=%d total=%d",
                    len(records),
                    total_processed,
                )

        # One transaction: staging and checkpoint commit together.

    elapsed = time.perf_counter() - started

    logger.info(
        "Pipeline completed | rows=%d | duration=%.2fs",
        total_processed,
        elapsed,
    )


if __name__ == "__main__":
    run_pipeline()
