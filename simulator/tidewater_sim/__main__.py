"""Transaction simulator: drives realistic OLTP traffic into the source Postgres.

Usage (inside compose):  docker compose run --rm simulator --rate 50 --duration 600
Connection settings come from the standard PG* environment variables.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import time
from collections import Counter
from decimal import Decimal

import psycopg
from faker import Faker

from tidewater_sim.events import (
    REFUNDABLE_STATUSES,
    EventKind,
    Pacer,
    next_status,
    pick_event,
)

log = logging.getLogger("tidewater_sim")

CATEGORIES = ["electronics", "home", "books", "toys", "apparel", "grocery", "sports"]
PAYMENT_METHODS = ["card", "card", "card", "paypal", "gift_card", "bank_transfer"]


class Simulator:
    def __init__(self, conn: psycopg.Connection, rng: random.Random, fake: Faker):
        self.conn = conn
        self.rng = rng
        self.fake = fake

    # ------------------------------------------------------------------ helpers
    def _random_id(self, table: str, pk: str, where: str = "true") -> int | None:
        # TABLESAMPLE keeps this cheap on large tables; fall back to a plain scan when small.
        row = (
            self.conn.execute(
                f"select {pk} from {table} tablesample system (5) where {where} limit 1"
            ).fetchone()
            or self.conn.execute(
                f"select {pk} from {table} where {where} order by random() limit 1"
            ).fetchone()
        )
        return row[0] if row else None

    def _customer_row(self) -> tuple:
        f = self.fake
        return (
            f.unique.email(),
            f.name(),
            f.phone_number()[:30],
            f.street_address(),
            f.city(),
            f.state_abbr(),
            f.postcode(),
        )

    # ------------------------------------------------------------------ seeding
    def seed(self, n_customers: int, n_products: int) -> None:
        have = self.conn.execute("select count(*) from customers").fetchone()[0]
        if have:
            log.info("source already seeded (%s customers), skipping seed", have)
            return
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.executemany(
                "insert into customers (email, full_name, phone, address_line, city, state,"
                " postal_code) values (%s, %s, %s, %s, %s, %s, %s)",
                [self._customer_row() for _ in range(n_customers)],
            )
            cur.executemany(
                "insert into products (sku, name, category, unit_price) values (%s, %s, %s, %s)",
                [
                    (
                        f"SKU-{i:06d}",
                        self.fake.catch_phrase(),
                        self.rng.choice(CATEGORIES),
                        Decimal(self.rng.randint(199, 49999)) / 100,
                    )
                    for i in range(1, n_products + 1)
                ],
            )
        log.info("seeded %s customers and %s products", n_customers, n_products)

    # ------------------------------------------------------------------ events
    def new_customer(self) -> None:
        self.conn.execute(
            "insert into customers (email, full_name, phone, address_line, city, state,"
            " postal_code) values (%s, %s, %s, %s, %s, %s, %s)",
            self._customer_row(),
        )

    def new_order(self) -> None:
        customer_id = self._random_id("customers", "customer_id")
        if customer_id is None:
            return self.new_customer()
        n_items = self.rng.randint(1, 4)
        products = self.conn.execute(
            "select product_id, unit_price from products where is_active"
            " order by random() limit %s",
            (n_items,),
        ).fetchall()
        if not products:
            return
        items = [(pid, self.rng.randint(1, 3), price) for pid, price in products]
        total = sum(q * p for _, q, p in items)

        order_id = self.conn.execute(
            "insert into orders (customer_id, order_total) values (%s, %s) returning order_id",
            (customer_id, total),
        ).fetchone()[0]
        with self.conn.cursor() as cur:
            cur.executemany(
                "insert into order_items (order_id, product_id, quantity, unit_price)"
                " values (%s, %s, %s, %s)",
                [(order_id, pid, q, p) for pid, q, p in items],
            )
        self.conn.execute(
            "insert into payments (order_id, amount, method, status)"
            " values (%s, %s, %s, 'authorized')",
            (order_id, total, self.rng.choice(PAYMENT_METHODS)),
        )

    def advance_status(self) -> None:
        order_id = self._random_id("orders", "order_id", "status in ('placed','paid','shipped')")
        if order_id is None:
            return
        row = self.conn.execute(
            "select status from orders where order_id = %s for update", (order_id,)
        ).fetchone()
        if row is None:  # deleted by a concurrent simulator since we picked it
            return
        new = next_status(row[0])
        if new is None:
            return
        self.conn.execute("update orders set status = %s where order_id = %s", (new, order_id))
        if new == "paid":
            self.conn.execute(
                "update payments set status = 'captured'"
                " where order_id = %s and status = 'authorized'",
                (order_id,),
            )

    def refund(self) -> None:
        order_id = self._random_id("orders", "order_id", "status in ('shipped','delivered')")
        if order_id is None:
            return
        row = self.conn.execute(
            "select status, order_total from orders where order_id = %s for update", (order_id,)
        ).fetchone()
        if row is None or row[0] not in REFUNDABLE_STATUSES:
            return
        self.conn.execute("update orders set status = 'refunded' where order_id = %s", (order_id,))
        # Refund is a new negative payment row, so payment history is append-only.
        self.conn.execute(
            "insert into payments (order_id, amount, method, status)"
            " select order_id, -amount, method, 'refunded' from payments"
            " where order_id = %s and status = 'captured'",
            (order_id,),
        )

    def address_change(self) -> None:
        customer_id = self._random_id("customers", "customer_id")
        if customer_id is None:
            return
        f = self.fake
        self.conn.execute(
            "update customers set address_line = %s, city = %s, state = %s, postal_code = %s"
            " where customer_id = %s",
            (f.street_address(), f.city(), f.state_abbr(), f.postcode(), customer_id),
        )

    def price_change(self) -> None:
        product_id = self._random_id("products", "product_id")
        if product_id is None:
            return
        factor = Decimal(self.rng.choice(["0.90", "0.95", "1.05", "1.10"]))
        self.conn.execute(
            "update products set unit_price = round(unit_price * %s, 2) where product_id = %s",
            (factor, product_id),
        )

    def cancel_delete(self) -> None:
        # Hard-deletes an unpaid order; FK cascades emit deletes for items and payments too.
        order_id = self._random_id("orders", "order_id", "status = 'placed'")
        if order_id is not None:
            self.conn.execute("delete from orders where order_id = %s", (order_id,))

    def delete_customer(self) -> None:
        customer_id = self._random_id(
            "customers",
            "customer_id",
            "not exists (select 1 from orders o where o.customer_id = customers.customer_id)",
        )
        if customer_id is not None:
            self.conn.execute("delete from customers where customer_id = %s", (customer_id,))

    def run_one(self, kind: EventKind) -> None:
        handler = getattr(self, kind.value)
        with self.conn.transaction():
            handler()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.getenv("SIM_EVENTS_PER_SEC", "20")),
        help="business events (transactions) per second",
    )
    parser.add_argument("--duration", type=float, default=0, help="seconds to run; 0 = forever")
    parser.add_argument("--events", type=int, default=0, help="stop after N events; 0 = no limit")
    parser.add_argument(
        "--seed-customers", type=int, default=int(os.getenv("SIM_SEED_CUSTOMERS", "200"))
    )
    parser.add_argument(
        "--seed-products", type=int, default=int(os.getenv("SIM_SEED_PRODUCTS", "100"))
    )
    parser.add_argument("--random-seed", type=int, default=None, help="for reproducible runs")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rng = random.Random(args.random_seed)
    fake = Faker()
    if args.random_seed is not None:
        Faker.seed(args.random_seed)

    with psycopg.connect(autocommit=True) as conn:
        sim = Simulator(conn, rng, fake)
        sim.seed(args.seed_customers, args.seed_products)

        pacer = Pacer(args.rate)
        counts: Counter[str] = Counter()
        started = last_report = time.monotonic()
        log.info("generating %.1f events/sec (duration=%ss)", args.rate, args.duration or "inf")
        while not args.duration or time.monotonic() - started < args.duration:
            if args.events and sum(counts.values()) >= args.events:
                break
            pacer.wait()
            kind = pick_event(rng)
            try:
                sim.run_one(kind)
                counts[kind.value] += 1
            except psycopg.errors.UniqueViolation:
                counts["retry_unique"] += 1
            except (psycopg.errors.IntegrityError, psycopg.errors.TransactionRollbackError):
                # Concurrent simulators race like real clients: a row deleted between
                # pick and write (FK violation) or a deadlock. Skip the event.
                counts["retry_conflict"] += 1
            now = time.monotonic()
            if now - last_report >= 10:
                total = sum(counts.values())
                log.info(
                    "%d events, %.1f/s overall | %s",
                    total,
                    total / (now - started),
                    dict(counts.most_common()),
                )
                last_report = now
        log.info("done: %d events | %s", sum(counts.values()), dict(counts.most_common()))


if __name__ == "__main__":
    main()
