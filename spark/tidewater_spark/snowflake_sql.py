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


def create_target(table: str) -> list[str]:
    """Target table plus the _WH_LOADED_AT load stamp the dbt incremental facts key on
    (the Postgres loader stamps it too). Rows loaded before the column existed get now()."""
    t = f"SILVER.{table.upper()}"
    return [
        f"create table if not exists {t} like SILVER.{staging(table)}",
        f"alter table {t} add column if not exists _WH_LOADED_AT timestamp_ntz",
        f"update {t} set _WH_LOADED_AT = current_timestamp() where _WH_LOADED_AT is null",
    ]


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
        f"when matched and ({NEWER}) then update set {updates}, "
        f"_WH_LOADED_AT = current_timestamp()\n"
        f"when not matched then insert ({', '.join(cols)}, _WH_LOADED_AT) "
        f"values ({', '.join('s.' + c for c in cols)}, current_timestamp())"
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


STAGE = "SILVER.LOAD_STAGE"
CREATE_STAGE = f"create stage if not exists {STAGE} file_format = (type = parquet)"

# Spark SQL type (DataType.simpleString()) -> Snowflake column type.
_SIMPLE = {
    "bigint": "NUMBER(38,0)",
    "int": "NUMBER(38,0)",
    "smallint": "NUMBER(38,0)",
    "string": "VARCHAR",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP_NTZ",
    "timestamp_ntz": "TIMESTAMP_NTZ",
    "date": "DATE",
    "double": "FLOAT",
    "float": "FLOAT",
}


def sf_type(spark_type: str) -> str:
    if spark_type.startswith("decimal("):
        return "NUMBER" + spark_type[len("decimal") :]
    if spark_type not in _SIMPLE:
        raise ValueError(f"no Snowflake mapping for Spark type {spark_type}")
    return _SIMPLE[spark_type]


def create_staging(table: str, fields: list[tuple[str, str]]) -> str:
    """Staging table from the Spark schema: (column name, Spark simpleString) pairs."""
    cols = ", ".join(f"{name.upper()} {sf_type(t)}" for name, t in fields)
    return f"create or replace table SILVER.{staging(table)} ({cols})"


def put(local_dir: str, stage_path: str) -> str:
    return (
        f"put file://{local_dir}/*.parquet @{STAGE}/{stage_path} auto_compress = false parallel = 8"
    )


def copy_into(table: str, stage_path: str) -> str:
    return (
        f"copy into SILVER.{staging(table)} from @{STAGE}/{stage_path} "
        f"file_format = (type = parquet use_logical_type = true) "
        f"match_by_column_name = case_insensitive purge = true"
    )
