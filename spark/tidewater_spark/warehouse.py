"""SQL for loading silver into the Postgres warehouse. Pure Python, so it is unit-testable.

Each table is loaded by Delta version, not by timestamp: a bookmark of the last
loaded version lives in `silver._load_state`, and the upsert plus the bookmark
update commit in ONE Postgres transaction. A crash before commit loses nothing
(the versions are re-read); a crash after commit re-reads nothing.
"""

from __future__ import annotations

SCHEMA = "silver"
STATE_TABLE = f"{SCHEMA}._load_state"

CREATE_STATE = f"""
create table if not exists {STATE_TABLE} (
    table_name text primary key,
    delta_version bigint not null,
    rows_loaded bigint not null,
    loaded_at timestamptz not null default now()
)"""

# Bookmarks are Delta versions, which restart at 0 when a table is recreated (e.g.
# silver rebuilt from bronze). The table's unique id detects that and forces a full load.
ADD_TABLE_ID = f"alter table {STATE_TABLE} add column if not exists delta_table_id text"


def q(identifier: str) -> str:
    """Quote a Postgres identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def staging_table(table: str) -> str:
    return f"_stg_{table}"


def create_target_sql(table: str, pk: tuple[str, ...]) -> list[str]:
    target, stg = f"{SCHEMA}.{q(table)}", f"{SCHEMA}.{q(staging_table(table))}"
    return [
        f"create table if not exists {target} (like {stg})",
        f"alter table {target} add column if not exists _wh_loaded_at timestamptz",
        f"""do $$ begin
            if not exists (select 1 from pg_constraint where conrelid = '{target}'::regclass
                           and contype = 'p') then
                alter table {target} add primary key ({", ".join(q(c) for c in pk)});
            end if;
        end $$""",
    ]


def add_column_sql(table: str, staging_cols: dict[str, str], target_cols: set[str]) -> list[str]:
    """ALTERs for columns that appeared in silver (source schema evolution)."""
    return [
        f"alter table {SCHEMA}.{q(table)} add column {q(name)} {pg_type}"
        for name, pg_type in staging_cols.items()
        if name not in target_cols
    ]


def upsert_sql(table: str, columns: list[str], pk: tuple[str, ...]) -> str:
    """Insert-or-update from the staging table, guarded by the same (lsn, offset) order
    as silver so the warehouse can never move a row backwards."""
    target, stg = f"{SCHEMA}.{q(table)}", f"{SCHEMA}.{q(staging_table(table))}"
    cols = ", ".join(q(c) for c in columns)
    updates = ", ".join(f"{q(c)} = excluded.{q(c)}" for c in columns if c not in pk)
    return (
        f"insert into {target} ({cols}, _wh_loaded_at)\n"
        f"select {cols}, now() from {stg}\n"
        f"on conflict ({', '.join(q(c) for c in pk)}) do update set {updates}, "
        f"_wh_loaded_at = excluded._wh_loaded_at\n"
        f"where ({target}._lsn, {target}._source_offset) "
        f"< (excluded._lsn, excluded._source_offset)"
    )


def save_state_sql() -> str:
    return (
        f"insert into {STATE_TABLE} (table_name, delta_version, rows_loaded, delta_table_id) "
        f"values (%s, %s, %s, %s) "
        f"on conflict (table_name) do update set delta_version = excluded.delta_version, "
        f"rows_loaded = excluded.rows_loaded, delta_table_id = excluded.delta_table_id, "
        f"loaded_at = now()"
    )
