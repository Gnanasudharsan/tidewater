"""SQL for loading silver into Snowflake (schema TIDEWATER.SILVER). Pure Python, unit-tested.

Same contract as the Postgres loader (ADR 0008): bookmark by Delta version and table
id, stage the changed rows, then MERGE with the (lsn, offset) guard and move the
bookmark in one transaction. Snowflake has no row-value comparison, so the guard is
spelled out. Identifiers are unquoted (Snowflake upper-cases them), matching dbt.
"""

from __future__ import annotations

STATE = "SILVER._LOAD_STATE"
LOG = "SILVER._LOAD_LOG"

CREATE_STATE = f"""create table if not exists {STATE} (
    table_name string primary key, delta_version number, delta_table_id string,
    rows_loaded number, loaded_at timestamp_ntz)"""

CREATE_LOG = f"""create table if not exists {LOG} (
    table_name string, rows_loaded number, delta_version number, loaded_at timestamp_ntz)"""

NEWER = "s._LSN > t._LSN or (s._LSN = t._LSN and s._SOURCE_OFFSET > t._SOURCE_OFFSET)"


def staging(table: str) -> str:
    return f"_STG_{table.upper()}"


def create_target(table: str) -> str:
    return f"create table if not exists SILVER.{table.upper()} like SILVER.{staging(table)}"


def add_columns(table: str, staging_cols: dict[str, str], target_cols: set[str]) -> list[str]:
    return [
        f"alter table SILVER.{table.upper()} add column {name} {sf_type}"
        for name, sf_type in staging_cols.items()
        if name.upper() not in {c.upper() for c in target_cols}
    ]


def merge(table: str, columns: list[str], pk: tuple[str, ...]) -> str:
    cols = [c.upper() for c in columns]
    keys = [k.upper() for k in pk]
    on = " and ".join(f"t.{k} = s.{k}" for k in keys)
    updates = ", ".join(f"{c} = s.{c}" for c in cols if c not in keys)
    return (
        f"merge into SILVER.{table.upper()} t using SILVER.{staging(table)} s on {on}\n"
        f"when matched and ({NEWER}) then update set {updates}\n"
        f"when not matched then insert ({', '.join(cols)}) "
        f"values ({', '.join('s.' + c for c in cols)})"
    )


def save_state() -> str:
    return (
        f"merge into {STATE} t using (select %s table_name, %s delta_version, %s delta_table_id, "
        f"%s rows_loaded) s on t.table_name = s.table_name "
        f"when matched then update set delta_version = s.delta_version, "
        f"delta_table_id = s.delta_table_id, rows_loaded = s.rows_loaded, "
        f"loaded_at = current_timestamp() "
        f"when not matched then insert (table_name, delta_version, delta_table_id, rows_loaded, "
        f"loaded_at) values (s.table_name, s.delta_version, s.delta_table_id, s.rows_loaded, "
        f"current_timestamp())"
    )


def log_load() -> str:
    return (
        f"insert into {LOG} (table_name, rows_loaded, delta_version, loaded_at) "
        f"values (%s, %s, %s, current_timestamp())"
    )
