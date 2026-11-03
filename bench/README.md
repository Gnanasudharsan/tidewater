# Benchmarks

All numbers were measured on a laptop: a 10-core Mac running **every** service
(Postgres, Kafka, Connect, registry, SeaweedFS, two Spark streams, Prometheus,
Grafana and the load generators) in one 8 GB Docker VM. Treat them as relative,
not as capacity planning.

**How to run:** `make bench` (sweep: `RATES`, `STEP`, `WINDOW`) and
`scripts/bench_ab.sh` (A/B/A comparison). Each step runs N parallel simulators
(one tops out near 400 tx/s), measures the last `WINDOW` seconds of the load from
Prometheus, then stops the load and times the **drain**: until silver has read
every row bronze ingested during the step. Latency is Tidewater's own
source-commit → silver-commit measurement (per-batch p50/p95, averaged over the window).

## 1. Throughput sweep (baseline, sequential MERGEs, `local[2]`)

[`baseline_20261102T182901.csv`](results/baseline_20261102T182901.csv), 120 s steps:

| target ev/s | achieved ev/s | CDC records/s | p50 s | p95 s | silver batch s | drain s |
|---|---|---|---|---|---|---|
| 100 | 100 | 269 | 107 | 122 | 51 | 15 |
| 500 | 500 | 1,332 | 70 | 92 | 47 | 57 |
| 1,000 | 989 | 2,629 | 70 | 80 | 52 | 82 |
| 2,000 | 1,997 | 5,232 | 83 | 96 | 53 | 77 |

- **Sources keep up at 2,000 business events/s** (5,232 CDC records/s): Debezium
  and bronze show 0 offsets of lag at every rate.
- **Silver is the bottleneck, and it is bounded by MERGE cost, not volume.** Batch
  time is roughly flat at ~50 s from 100 to 2,000 events/s; silver absorbs load by
  making each batch bigger. So latency is about "one to two batch durations"
  regardless of rate.
- **The SLO (p95 < 60 s) is not met at these table sizes.** Early in the project
  (tables around 50k rows) the same 100 ev/s step measured **p95 59.7 s**; by this
  sweep (around 250k rows per table) it was 122 s. A 193-row batch now takes 44 s,
  27 s of it MERGE.

## 2. Tuning experiment: concurrent per-table MERGEs (A/B/A)

Hypothesis: five sequential MERGEs (one per table) dominate batch time (~60–66%,
measured), and they are independent, so running them concurrently should cut it.
One variable: `SILVER_MERGE_PARALLELISM` 1 vs 5; everything else equal (`local[4]`).
A/B/A ordering exposes drift, since tables grow with every run.

| run | target ev/s | achieved | p50 s | p95 s | silver batch s | drain s |
|---|---|---|---|---|---|---|
| A1 sequential | 500 | 497 | 263 | 287 | 60 | 15 |
| A1 sequential | 1,000 | 661 | 179 | 224 | 129 | 196 |
| **B concurrent** | 500 | 498 | 131 | 146 | 106 | 117 |
| **B concurrent** | 1,000 | 877 | 111 | 123 | 88 | 88 |
| A2 sequential | 500 | 500 | 93 | 113 | 64 | 67 |
| A2 sequential | 1,000 | 999 | 102 | 111 | 78 | >900 (never drained) |

**Result: inconclusive. The default stays sequential.**
- The two *identical* baseline runs differ 2.5× at 500 ev/s (287 s vs 113 s). On
  this machine, run-to-run noise is larger than the effect being measured.
- At 500 ev/s, B was **worse** than A2: five concurrent MERGEs compete for the same
  cores as eight simulators, Kafka, Postgres and bronze.
- At 1,000 ev/s, B drained its backlog in 88 s; A2 never did (silver fell to about
  143 rows/s). That is a possible catch-up benefit under heavy load, but it is one sample.

The honest conclusion is to keep the change behind a flag
(`SILVER_MERGE_PARALLELISM`) and not claim a win. A clean answer needs the
streams on dedicated cores, separate from the load generators.

## 3. What to tune next (the evidence points here)

Latency grows with **table size**, not load: every MERGE into an unpartitioned
Delta table scans and rewrites large files. That is also why concurrent writers
conflict on every commit (chaos 06). The next experiment, same A/B/A method:
1. **Deletion vectors** (`delta.enableDeletionVectors`): MERGE marks rows deleted
   instead of rewriting whole files.
2. **Partition silver by key range** (or Liquid Clustering when available), so a
   batch touches a few small files and the backfill and stream stop conflicting.

## 4. Postgres vs Snowflake (same dbt project, same data)

Both warehouses held identical live data (verified per table: 943,775 orders,
2,360,035 order lines, 1,075,192 payments, 152,543 customers). `dbt build
--full-refresh`, 8 threads, run from the laptop
([csv](results/warehouse_comparison_20261103.csv)):

| | Postgres 16 (local container) | Snowflake X-Small (AWS us-east-2) |
|---|---|---|
| nodes | 80 | 81 (+ `snowflake_cost`) |
| **full rebuild, wall** | **16.3 s** | **31.7 s** |
| `fct_orders` (944k rows) | 5.77 s | **3.07 s** |
| `fct_order_items` (2.36M rows) | 3.84 s | 3.89 s |
| 65 data tests, average | **0.54 s** | 1.28 s |

**Reading it:** on the heavy models the two are level, and Snowflake is faster on
the largest join. Snowflake loses overall on **per-statement overhead**: each of the
65 small test queries pays a laptop-to-AWS round trip plus Snowflake's fixed query
cost. At about 5M rows the workload is too small for Snowflake's strengths (scale-out,
concurrency); the crossover point would need a larger dataset to find.

## 5. Snowflake cost

Measured with `INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY` (near real time):

| what | value |
|---|---|
| rows delivered by the loader | 5,054,321 (11 loads) |
| credits used by `TIDEWATER_WH` | ~0.297 (all loads, 3 dbt builds incl. a full rebuild, tests, failed attempts) |
| **credits per million rows delivered and transformed** | **~0.059** |

All-in and conservative: it includes every query on the warehouse, and the most
recent hour may not be fully metered yet. The resource monitor caps spend at 20
credits/month. For the final hourly breakdown, build `marts.snowflake_cost` once
`ACCOUNT_USAGE` has caught up (up to ~3 h lag).
