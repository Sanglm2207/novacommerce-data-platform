
from __future__ import annotations

import argparse
import os
import random
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from faker import Faker


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

SEED = 42
CUSTOMERS = 12_000
PRODUCTS = 3_000
ORDERS = 40_000

TABLES = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
)

START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)


def copy_rows(cur, table, columns, rows):
    """Stream records through PostgreSQL COPY protocol."""
    sql = f"COPY {table} ({', '.join(columns)}) FROM STDIN"
    count = 0

    with cur.copy(sql) as writer:
        for row in rows:
            writer.write_row(row)
            count += 1

    print(f"  {table:<16} {count:>8,} rows")
    return count


def assert_empty(cur):
    """Prevent accidental insertion into an existing dataset."""
    for table in TABLES:
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})")
        if cur.fetchone()[0]:
            raise RuntimeError(
                f"Table '{table}' is not empty. "
                "Use a fresh training database, or run with --reset "
                "to delete and regenerate all seed data."
            )


def generate_customers(fake):
    for i in range(1, CUSTOMERS + 1):
        yield (
            fake.name(),
            f"customer{i:06d}@example.com",
            START_DATE - timedelta(days=i % 365),
        )


def generate_products(rng):
    categories = (
        "Keyboard",
        "Mouse",
        "Monitor",
        "Headphones",
        "DJI Osmo Pocket 4",
        "GoPro Hero 13 Black",
        "Apple iPhone 17 Pro Max",
        "Samsung Galaxy S26 Ultra",
    )

    for i in range(1, PRODUCTS + 1):
        category = categories[(i - 1) % len(categories)]
        price = Decimal(rng.randrange(50_000, 8_000_001, 1_000))

        yield (
            f"SKU-{i:06d}",
            f"{category} Model {i:04d}",
            price,
            True,
            START_DATE,
        )


def generate_orders(rng, customer_ids):
    statuses = ("PENDING", "CONFIRMED", "COMPLETED", "CANCELLED")
    weights = (10, 20, 60, 10)

    for i in range(ORDERS):
        yield (
            rng.choice(customer_ids),
            rng.choices(statuses, weights=weights, k=1)[0],
            START_DATE + timedelta(minutes=i * 15),
        )


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic training data.")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete all data in the five seed tables and regenerate it.",
    )
    args = parser.parse_args()

    started = time.perf_counter()
    rng = random.Random(SEED)
    fake = Faker("en_US")
    fake.seed_instance(SEED)

    db_name = os.getenv("POSTGRES_DB")

    if db_name != "novacommerce_oltp":
        raise RuntimeError(
            "Refusing to seed: expected training DB "
            "'novacommerce_oltp'."
        )

    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is missing")

    connection = {
        "host": "127.0.0.1",
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": db_name,
        "user": os.getenv("POSTGRES_USER", "novacommerce"),
        "password": password,
    }

    print("Starting synthetic data generation...")
    total_rows = 0

    # One transaction: commit everything or roll back everything.
    with psycopg.connect(**connection) as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '180s'")
            if args.reset:
                print("Resetting seed tables...")
                cur.execute(
                    f"TRUNCATE TABLE {', '.join(TABLES)} RESTART IDENTITY"
                )
            assert_empty(cur)

            # 1. Customers
            total_rows += copy_rows(
                cur,
                "customers",
                ("full_name", "email", "created_at"),
                generate_customers(fake),
            )

            cur.execute("SELECT customer_id FROM customers")
            customer_ids = [row[0] for row in cur.fetchall()]

            # 2. Products
            total_rows += copy_rows(
                cur,
                "products",
                ("sku", "product_name", "price",
                 "is_active", "created_at"),
                generate_products(rng),
            )

            cur.execute("SELECT product_id, price FROM products")
            product_prices = dict(cur.fetchall())
            product_ids = list(product_prices)

            # 3. Orders
            total_rows += copy_rows(
                cur,
                "orders",
                ("customer_id", "status", "created_at"),
                generate_orders(rng, customer_ids),
            )

            cur.execute(
                "SELECT order_id, status, created_at "
                "FROM orders ORDER BY created_at"
            )
            order_records = cur.fetchall()

            # 4. Order items + payment records
            # Each order has 2 or 3 distinct product lines.
            payment_rows = []

            def generate_items():
                for order_id, status, created_at in order_records:
                    order_total = Decimal("0.00")
                    selected = rng.sample(
                        product_ids,
                        k=rng.choice((2, 3)),
                    )

                    for product_id in selected:
                        quantity = rng.randint(1, 4)
                        unit_price = product_prices[product_id]
                        order_total += unit_price * quantity

                        yield (
                            order_id,
                            product_id,
                            quantity,
                            unit_price,
                        )

                    payment_status = {
                        "PENDING": "PENDING",
                        "CONFIRMED": "SUCCESS",
                        "COMPLETED": "SUCCESS",
                        "CANCELLED": "FAILED",
                    }[status]

                    # Optional failed attempt before successful payment.
                    if (
                        payment_status == "SUCCESS"
                        and rng.random() < 0.05
                    ):
                        payment_rows.append((
                            order_id,
                            order_total,
                            "FAILED",
                            created_at + timedelta(minutes=1),
                        ))

                    payment_rows.append((
                        order_id,
                        order_total,
                        payment_status,
                        created_at + timedelta(minutes=2),
                    ))

            total_rows += copy_rows(
                cur,
                "order_items",
                ("order_id", "product_id",
                 "quantity", "unit_price"),
                generate_items(),
            )

            # 5. Payments
            total_rows += copy_rows(
                cur,
                "payments",
                ("order_id", "amount", "status", "created_at"),
                payment_rows,
            )

            # Update statistics for query planning.
            for table in TABLES:
                cur.execute(f"ANALYZE {table}")

        # Automatic COMMIT on successful context exit.

    elapsed = time.perf_counter() - started

    print()
    print("Seed completed successfully!")
    print(f"Total records : {total_rows:,}")
    print(f"Duration      : {elapsed:.2f}s")
    print(f"Throughput    : {total_rows / elapsed:,.0f} rows/sec")


if __name__ == "__main__":
    main()
