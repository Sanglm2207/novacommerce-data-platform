"""NovaCommerce Day 07 — read-only data quality audit and persisted run metrics.

Run after syncing the CDC journal. Use --max-pending-age-seconds to set the SLA.
This validator does not alter OLTP or staging business data.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[3] / '.env')
LOG = logging.getLogger('novacommerce.quality')
PIPELINE = 'orders_cdc_quality'


@dataclass(frozen=True)
class Check:
    name: str
    observed: int
    threshold: int
    severity: str = 'ERROR'

    @property
    def passed(self) -> bool:
        return self.observed <= self.threshold


def connect() -> psycopg.Connection:
    if os.environ.get('POSTGRES_DB') != 'novacommerce_oltp':
        raise RuntimeError('Refusing to run outside the novacommerce_oltp training database')
    return psycopg.connect(
        host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        connect_timeout=10,
    )


def record_start() -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute('INSERT INTO etl.pipeline_runs (pipeline_name) VALUES (%s) RETURNING run_id', (PIPELINE,))
        return cur.fetchone()[0]


def collect(max_pending_age_seconds: int) -> list[Check]:
    # Consistent snapshot of source, staging and event queue.
    with connect() as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        with conn.cursor() as cur:
            queries = [
                ('missing_staging_orders', '''
                    SELECT count(*) FROM public.orders o
                    WHERE NOT EXISTS (SELECT 1 FROM staging.orders s WHERE s.order_id=o.order_id)
                ''', 0, 'ERROR'),
                ('orphan_staging_orders', '''
                    SELECT count(*) FROM staging.orders s
                    WHERE NOT EXISTS (SELECT 1 FROM public.orders o WHERE o.order_id=s.order_id)
                ''', 0, 'ERROR'),
                ('invalid_staging_values', '''
                    SELECT count(*) FROM staging.orders
                    WHERE total_items < 0 OR total_amount < 0
                ''', 0, 'ERROR'),
                ('mismatched_order_status', '''
                    SELECT count(*) FROM staging.orders s
                    JOIN public.orders o USING (order_id)
                    WHERE s.order_status IS DISTINCT FROM o.status
                ''', 0, 'ERROR'),
                ('mismatched_item_aggregates', '''
                    SELECT count(*) FROM staging.orders s
                    JOIN public.orders o USING (order_id)
                    LEFT JOIN (
                        SELECT order_id, SUM(quantity)::bigint AS qty,
                               SUM(quantity * unit_price)::numeric AS amount
                        FROM public.order_items GROUP BY order_id
                    ) a ON a.order_id = o.order_id
                    WHERE s.total_items IS DISTINCT FROM COALESCE(a.qty,0)
                       OR s.total_amount IS DISTINCT FROM COALESCE(a.amount,0)
                ''', 0, 'ERROR'),
                ('mismatched_payment_flags', '''
                    SELECT count(*) FROM staging.orders s
                    JOIN public.orders o USING (order_id)
                    WHERE s.has_successful_payment IS DISTINCT FROM EXISTS (
                      SELECT 1 FROM public.payments p
                      WHERE p.order_id=o.order_id AND p.status='SUCCESS'
                    )
                ''', 0, 'ERROR'),
                ('unresolved_aged_events', '''
                    SELECT count(*) FROM etl.order_changes
                    WHERE processed_at IS NULL
                      AND recorded_at < clock_timestamp() - (%s * INTERVAL '1 second')
                ''', 0, 'ERROR'),
                ('multiple_successful_payments', '''
                    SELECT count(*) FROM (
                       SELECT order_id FROM public.payments
                       WHERE status='SUCCESS'
                       GROUP BY order_id HAVING count(*) > 1
                    ) flagged
                ''', 0, 'WARNING'),
            ]
            results = []
            for name, sql, threshold, severity in queries:
                params = (max_pending_age_seconds,) if name == 'unresolved_aged_events' else None
                cur.execute(sql, params)
                results.append(Check(name, int(cur.fetchone()[0]), threshold, severity))
            return results


def record_finish(run_id: int, checks: list[Check], error: str | None) -> None:
    with connect() as conn, conn.cursor() as cur:
        if checks:
            cur.executemany('''
                INSERT INTO etl.data_quality_results
                  (run_id, check_name, observed_value, threshold, passed, severity)
                VALUES (%s,%s,%s,%s,%s,%s)
            ''', [(run_id,c.name,c.observed,c.threshold,c.passed,c.severity) for c in checks])
        failures = sum(not c.passed and c.severity == 'ERROR' for c in checks)
        cur.execute('''
            UPDATE etl.pipeline_runs
            SET finished_at=clock_timestamp(), status=%s,
                checks_total=%s, checks_failed=%s, error_message=%s
            WHERE run_id=%s
        ''', ('FAILED' if error or failures else 'SUCCESS', len(checks), failures, error, run_id))


def run(max_age: int) -> int:
    run_id = record_start()
    checks: list[Check] = []
    try:
        checks = collect(max_age)
        record_finish(run_id, checks, None)
    except Exception as exc:
        try:
            record_finish(run_id, checks, str(exc)[:2000])
        except Exception:
            LOG.exception('Could not persist failure for run_id=%d', run_id)
        LOG.exception('Audit failed run_id=%d', run_id)
        return 2
    for check in checks:
        status = 'PASS' if check.passed else check.severity
        LOG.info('%s %-32s observed=%d threshold=%d', status, check.name, check.observed, check.threshold)
    failed = sum(not c.passed and c.severity == 'ERROR' for c in checks)
    LOG.info('run_id=%d checks=%d failed=%d', run_id, len(checks), failed)
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description='NovaCommerce Data Quality Audit')
    parser.add_argument('--max-pending-age-seconds', type=int, default=300)
    args = parser.parse_args()
    if args.max_pending_age_seconds < 0:
        parser.error('Max pending age must be >= 0')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    return run(args.max_pending_age_seconds)


if __name__ == '__main__':
    sys.exit(main())
