# ADR 0005: Postgres warehouse for development, Snowflake for benchmarks

Status: Accepted · Date: 2026-10-31

## Context
The project has a $0 budget. Snowflake offers a 30-day trial with credits.
Developing against Snowflake for 10 weeks would exhaust the trial before
benchmarking starts.

## Decision
- Develop and run CI against a **Postgres 16 warehouse** (`postgres-wh`).
- Run the **same dbt project** with `--target snowflake` only in week 9, for
  benchmarks and cost numbers.

## Rationale
- Postgres is free, runs in CI as a service container, and is close enough in
  SQL semantics for staging, dims, and facts.
- Starting the trial late means its whole window goes to benchmarking.

## Consequences
- dbt SQL must stay adapter-agnostic: use `dbt.dateadd`, `dbt.datediff`,
  `dbt.type_*`, `dbt.safe_cast`, and `dbt_utils` macros instead of
  dialect-specific functions. Any unavoidable difference goes in a dispatched macro.
- CI never touches Snowflake, so Snowflake-specific breakage is only caught in week 9.
  Mitigation: keep Snowflake-only code paths to a minimum.
- All benchmark outputs are exported to the repo before the trial ends.
