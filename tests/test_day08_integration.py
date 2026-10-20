"""Tests actual Day 06/07 modules unchanged against an ephemeral PostgreSQL 17."""
from decimal import Decimal
from uuid import uuid4
import pytest
from novacommerce.etl import orders_cdc_pipeline as cdc
from novacommerce.etl import validate_pipeline as quality

@pytest.fixture
def inserted_order():
    with cdc.connect() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO public.customers(full_name,email) VALUES ('CI Customer',%s) RETURNING customer_id", (f"ci-{uuid4().hex}@example.invalid",))
        customer_id = cur.fetchone()[0]
        cur.execute("INSERT INTO public.products(sku,product_name,price) VALUES (%s,'Test keyboard',1500000) RETURNING product_id", (f"CI-{uuid4().hex[:12]}",))
        product_id = cur.fetchone()[0]
        cur.execute("INSERT INTO public.orders(customer_id,status) VALUES (%s,'PENDING') RETURNING order_id", (customer_id,))
        order_id = cur.fetchone()[0]
        cur.execute("INSERT INTO public.order_items(order_id,product_id,quantity,unit_price) VALUES (%s,%s,2,1500000)", (order_id,product_id))
    return order_id

def test_cdc_updates_and_quality(inserted_order):
    order_id = inserted_order
    with cdc.connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM etl.order_changes WHERE order_id=%s AND processed_at IS NULL',(order_id,))
        assert cur.fetchone()[0] >= 2

    events, orders = cdc.process_batch(100)
    assert events >= 2 and orders == 1
    assert cdc.process_batch(100) == (0,0)

    with cdc.connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT order_status, total_items, total_amount, has_successful_payment FROM staging.orders WHERE order_id=%s',(order_id,))
        assert cur.fetchone() == ('PENDING',2,Decimal('3000000.00'),False)
        cur.execute("UPDATE public.orders SET status='COMPLETED' WHERE order_id=%s",(order_id,))
        cur.execute("INSERT INTO public.payments(order_id,amount,status) VALUES (%s,3000000,'SUCCESS')",(order_id,))

    assert cdc.process_batch(100)[0] >= 2
    with cdc.connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT order_status, has_successful_payment FROM staging.orders WHERE order_id=%s',(order_id,))
        assert cur.fetchone() == ('COMPLETED', True)

    checks = quality.collect(300)
    errors = [check for check in checks if check.severity == 'ERROR' and not check.passed]
    assert not errors, [(c.name,c.observed) for c in errors]
    assert quality.run(300) == 0

def test_failure_rollback(inserted_order):
    order_id = inserted_order

    with pytest.raises(RuntimeError, match='Injected failure'):
        cdc.process_batch(100, fail_after=1)

    with cdc.connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM staging.orders WHERE order_id=%s',(order_id,))
        assert cur.fetchone()[0] == 0
        cur.execute('SELECT count(*) FROM etl.order_changes WHERE order_id=%s AND processed_at IS NULL',(order_id,))
        assert cur.fetchone()[0] > 0

    assert cdc.process_batch(100)[0] >= 1
    assert cdc.process_batch(100) == (0,0)
