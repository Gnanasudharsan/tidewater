# Demo video script (3–5 minutes)

Record with `DEMO_PAUSE=1 make demo` so each step waits for Enter. Screen layout:
terminal on the left, Grafana (http://localhost:3001) on the right.

**Before recording:** `make up-core`, wait ~2 minutes, keep the Mac awake and plugged
in, close other heavy apps.

| Time | Screen | Say |
|---|---|---|
| 0:00 | README with the architecture diagram | "Tidewater is a CDC lakehouse. Kafka delivers at least once; this pipeline makes the results exactly once, and I'll prove it by crashing it." |
| 0:20 | diagram, pointing at bronze and silver | "Every change carries its Postgres WAL position. Silver only applies a change if it's newer, so duplicates and late events can't change the result." |
| 0:45 | terminal, step 1, then 2 | "Here's live e-commerce traffic: 50 orders, payments and refunds a second." Switch to Grafana: produce rate rising. |
| 1:15 | terminal, step 3 | "Now I SIGKILL the Spark stream *while a batch is running*." Grafana: gap in the lag line. "It restarts from its checkpoint, and Delta skips any batch it already wrote." |
| 2:00 | terminal, step 4 | "Load finishes, the pipeline drains." |
| 2:20 | terminal, step 5 | "Two proofs. Bronze: every Kafka offset exactly once, no gaps. Silver against the source: row counts and a SHA-256 of every row. 100%." |
| 3:00 | terminal, step 6 | "And one order traced from the warehouse back to the exact Kafka offsets and WAL positions it came from." |
| 3:30 | README results table | "Six chaos tests like this one, including a Debezium outage and rebuilding silver from scratch. The honest part: the latency SLO holds on small tables and misses at scale; the README explains why and what's next." |
| 4:00 | postmortem page | "And the time Kafka actually lost all its data, and how it was recovered to 100%." |
| 4:20 | end | "Code, design docs and runbooks are on GitHub." |

Upload to YouTube (unlisted) or Loom, then link it from the README results section.
