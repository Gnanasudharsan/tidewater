"""Provision the Tidewater Metabase dashboard as code. Idempotent: safe to re-run.

    python3 scripts/metabase_setup.py        (or: make dashboard)

Performs first-time setup with the local admin from .env (MB_ADMIN_EMAIL /
MB_ADMIN_PASSWORD), connects the warehouse, then creates or updates every card
and the dashboard layout. Live tiles read staging views (refreshed by the loader);
history tiles read dbt marts.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE = os.getenv("MB_URL", "http://localhost:3000")
DASHBOARD = "Tidewater: live ops"
DATABASE = "Tidewater warehouse"
UTC_NOW = "(now() at time zone 'utc')"
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

# (name, display, sql, (row, col, size_x, size_y))
CARDS = [
    (
        "GMV today (live)",
        "scalar",
        f"""select coalesce(sum(order_total), 0) as gmv_today
from staging.stg_orders
where order_status <> 'cancelled' and ordered_at >= date_trunc('day', {UTC_NOW})""",
        (0, 0, 6, 3),
    ),
    (
        "Orders in the last 5 minutes",
        "scalar",
        f"select count(*) as orders from staging.stg_orders "
        f"where ordered_at > {UTC_NOW} - interval '5 minutes'",
        (0, 6, 6, 3),
    ),
    (
        "Source to warehouse lag (seconds)",
        "scalar",
        f"select round(extract(epoch from {UTC_NOW} - max(_source_ts))) as lag_seconds "
        f"from silver.orders",
        (0, 12, 6, 3),
    ),
    (
        "DQ failures (last 24h)",
        "scalar",
        f"select count(*) as failures from silver.dq_quarantine "
        f"where _recorded_at > {UTC_NOW} - interval '24 hours'",
        (0, 18, 6, 3),
    ),
    (
        "Orders per minute (last hour)",
        "line",
        f"""select date_trunc('minute', ordered_at) as minute, count(*) as orders
from staging.stg_orders
where ordered_at > {UTC_NOW} - interval '60 minutes'
group by 1 order by 1""",
        (3, 0, 12, 6),
    ),
    (
        "DQ failures by rule (last 24h)",
        "bar",
        f"""select source_table || ': ' || reasons as rule, count(*) as failures
from silver.dq_quarantine
where _recorded_at > {UTC_NOW} - interval '24 hours'
group by 1 order by 2 desc""",
        (3, 12, 12, 6),
    ),
    (
        "GMV by day",
        "bar",
        "select order_date, gmv, orders from marts.fct_daily_sales order by order_date",
        (9, 0, 12, 6),
    ),
    (
        "Refund rate by day",
        "line",
        """select order_date, round(1.0 * refunded_orders / nullif(orders, 0), 4) as refund_rate
from marts.fct_daily_sales order by order_date""",
        (9, 12, 12, 6),
    ),
    (
        "Revenue by category",
        "bar",
        """select p.category, sum(i.line_total) as revenue
from marts.fct_order_items as i
join marts.dim_product as p on i.product_id = p.product_id
group by 1 order by 2 desc""",
        (15, 0, 12, 6),
    ),
    (
        "Late arrivals by table (superseded CDC events)",
        "bar",
        """select source_table, count(*) as late_events
from silver.late_arrivals group by 1 order by 2 desc""",
        (15, 12, 12, 6),
    ),
]


def load_env(path: Path = ENV_FILE) -> None:
    for line in path.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def api(method: str, path: str, body: dict | None = None, session: str | None = None):
    req = urllib.request.Request(
        f"{BASE}/api{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Content-Type": "application/json",
            **({"X-Metabase-Session": session} if session else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"{method} {path} failed: {exc.code} {exc.read()[:300]!r}") from exc


def database_details() -> dict:
    return {
        "host": "postgres-wh",
        "port": 5432,
        "dbname": os.environ["WH_PG_DB"],
        "user": os.environ["WH_PG_USER"],
        "password": os.environ["WH_PG_PASSWORD"],
        "ssl": False,
    }


def login() -> str:
    email, password = os.environ["MB_ADMIN_EMAIL"], os.environ["MB_ADMIN_PASSWORD"]
    token = api("GET", "/session/properties").get("setup-token")
    if token and not api("GET", "/session/properties").get("has-user-setup"):
        print("first-time setup")
        result = api(
            "POST",
            "/setup",
            {
                "token": token,
                "user": {
                    "first_name": "Tidewater",
                    "last_name": "Admin",
                    "email": email,
                    "password": password,
                    "site_name": "Tidewater",
                },
                "prefs": {"site_name": "Tidewater", "site_locale": "en", "allow_tracking": False},
            },
        )
        return result["id"]
    return api("POST", "/session", {"username": email, "password": password})["id"]


def ensure_database(session: str) -> int:
    dbs = api("GET", "/database", session=session)
    dbs = dbs["data"] if isinstance(dbs, dict) else dbs
    for db in dbs:
        if db["name"] == DATABASE:
            return db["id"]
    db = api(
        "POST",
        "/database",
        {"engine": "postgres", "name": DATABASE, "details": database_details()},
        session=session,
    )
    print(f"connected database {DATABASE} (id {db['id']})")
    return db["id"]


def upsert_cards(session: str, db_id: int) -> list[tuple[int, tuple]]:
    existing = {c["name"]: c["id"] for c in api("GET", "/card?f=all", session=session)}
    placed = []
    for name, display, sql, layout in CARDS:
        body = {
            "name": name,
            "display": display,
            "visualization_settings": {},
            "dataset_query": {"type": "native", "database": db_id, "native": {"query": sql}},
        }
        if name in existing:
            api("PUT", f"/card/{existing[name]}", body, session=session)
            card_id = existing[name]
        else:
            card_id = api("POST", "/card", body, session=session)["id"]
        placed.append((card_id, layout))
    return placed


def upsert_dashboard(session: str, placed: list[tuple[int, tuple]]) -> int:
    dash = next(
        (d for d in api("GET", "/dashboard", session=session) if d["name"] == DASHBOARD), None
    )
    if dash is None:
        dash = api(
            "POST",
            "/dashboard",
            {"name": DASHBOARD, "description": "Live CDC pipeline health and sales."},
            session=session,
        )
    dashcards = [
        {
            "id": -(i + 1),
            "card_id": card_id,
            "row": row,
            "col": col,
            "size_x": sx,
            "size_y": sy,
            "series": [],
            "visualization_settings": {},
            "parameter_mappings": [],
        }
        for i, (card_id, (row, col, sx, sy)) in enumerate(placed)
    ]
    api("PUT", f"/dashboard/{dash['id']}", {"dashcards": dashcards}, session=session)
    return dash["id"]


def main() -> int:
    load_env()
    session = login()
    db_id = ensure_database(session)
    dash_id = upsert_dashboard(session, upsert_cards(session, db_id))
    print(f"dashboard ready: {BASE}/dashboard/{dash_id}#refresh=60")
    return 0


if __name__ == "__main__":
    sys.exit(main())
