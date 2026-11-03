# Snowflake first run (2026-11-02, X-Small TIDEWATER_WH)

Silver Delta -> TIDEWATER.SILVER via `make load-sf` (Parquet PUT/COPY, LSN-guarded MERGE):

| table | Delta rows | live | Snowflake rows | live | |
|---|---|---|---|---|---|
| customers | 204,467 | 152,478 | 204,467 | 152,478 | MATCH |
| order_items | 2,619,552 | 2,358,652 | 2,619,552 | 2,358,652 | MATCH |
| orders | 1,047,404 | 943,208 | 1,047,404 | 943,208 | MATCH |
| payments | 1,178,746 | 1,074,550 | 1,178,746 | 1,074,550 | MATCH |
| products | 100 | 100 | 100 | 100 | MATCH |

- Full initial load of ~5.05M rows: about 4 minutes from the laptop (upload-bound).
- Incremental load after a 30 s burst: 3,900 rows across 5 tables, each reading
  only its new Delta versions.
- `dbt build --target snowflake`: **PASS=81, 33.5 s** (5 views, 6 tables, 4
  incremental, 1 snapshot, 65 data tests). The same project passes 80/80 on Postgres
  (`snowflake_cost` is Snowflake-only).

Not measured yet: full-refresh timing on both warehouses, and credits per million
rows (`marts.snowflake_cost`; ACCOUNT_USAGE lags about 3 h). Two attempts failed on
the laptop's side (network drops while the Mac slept; Docker VM clock skew
invalidating the key-pair JWT), not on Snowflake's.
