# Snowflake benchmark setup (week 9)

Everything is prepared in code. Snowflake is only needed for the cloud half of the
benchmark: the same dbt project on a real warehouse, and credits per million rows.
Start the 30-day trial **only when you are ready to run it**, so the trial window
covers the benchmark.

## 1. Sign up (you)
Create a trial at snowflake.com (standard edition, any cloud/region). Check the
current trial terms (credits, duration) on the signup page. Note your
**organization name** and **account name** (Admin → Accounts).

## 2. Local credentials (you)
Put these in `.env` (gitignored; never commit them, never paste them in chat):

```bash
SNOWFLAKE_ORGANIZATION_NAME=<org>
SNOWFLAKE_ACCOUNT_NAME=<account>
SNOWFLAKE_USER=<your admin login>        # used only by Terraform
SNOWFLAKE_PASSWORD=<your admin password> # used only by Terraform
SNOWFLAKE_ACCOUNT=<org>-<account>        # used by the loader and dbt
```

## 3. Provision (one command each)

```bash
make sf-keypair   # RSA key pair for the service user -> .secrets/ (gitignored)
make sf-plan      # review: warehouse, database, schemas, 2 roles, service user, monitor
make sf-apply
```

What gets created (`terraform/snowflake/`):

| Object | Setting | Why |
|---|---|---|
| `TIDEWATER_WH` | X-Small, auto-suspend 60 s, starts suspended | credits only while working |
| `TIDEWATER_MONITOR` | 20 credits/month, notify 50/80%, **suspend at 100%** | the trial can't run away |
| `TIDEWATER` db | `SILVER`, `STAGING`, `MARTS`, `SNAPSHOTS` | same layout as Postgres |
| `TIDEWATER_LOADER` / `TIDEWATER_TRANSFORMER` | least privilege | loader writes silver; dbt reads silver, writes the rest |
| `SVC_TIDEWATER` | key-pair auth only | no password in the pipeline |

After `sf-apply`, remove `SNOWFLAKE_PASSWORD` from `.env`: nothing else needs it.

## 4. Run the benchmark

```bash
make load-sf      # silver Delta -> TIDEWATER.SILVER (Change Data Feed, LSN-guarded MERGE)
make dbt-sf       # the same dbt project with --target snowflake
make bench        # load at 100 / 500 / 1K / 2K events/s; run load-sf + dbt-sf after each
```

`marts.snowflake_cost` (Snowflake only) joins `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`
to the rows the loader delivered: **credits per million rows**. ACCOUNT_USAGE lags
by up to ~3 hours, so build it again the next day for final numbers.

## 5. Before the trial ends
Export everything into the repo: `bench/results/*.csv`, the `snowflake_cost` table
(`dbt show -s snowflake_cost --limit 1000 --target snowflake > bench/results/snowflake_cost.txt`),
query history for dbt runs, and screenshots. Then `terraform destroy` in
`terraform/snowflake` (via `make sf-plan`-style docker run) or let the trial lapse.

## Status
The Terraform validates against provider 1.0.5, the dbt project parses with
`dbt-snowflake` 1.8.4, and the loader's SQL is unit-tested. **None of it has run
against a live account yet**; expect small fixes on first contact.
