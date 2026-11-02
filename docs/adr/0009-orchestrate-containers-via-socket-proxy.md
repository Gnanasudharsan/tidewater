# ADR 0009: Airflow runs pipeline jobs as containers, through a Docker socket proxy

Status: Accepted · Date: 2026-11-02

## Context
Airflow must run Spark jobs (load, backfill, maintenance) and dbt. Installing
Spark, Java, Delta jars and dbt into the Airflow image would create a second,
drifting copy of every runtime. Mounting the Docker socket into Airflow so it can
run the existing images is common, but it hands Airflow root-equivalent control
of the host.

## Decision
- DAG tasks are `DockerOperator`s that run the **same images** the pipeline uses
  locally and in CI (`tidewater-spark`, `tidewater-dbt`) on the Compose network.
- Airflow reaches Docker only through **tecnativa/docker-socket-proxy**, which
  allows the container and image APIs and refuses the rest (volumes, networks,
  swarm, exec, system).
- The dbt project is baked into its image, so a task needs no bind mounts.

## Rationale
- One runtime per job: what passes in CI is exactly what Airflow runs.
- The orchestrator stays thin (scheduling, retries, params, alerting) and holds no
  pipeline dependencies, which mirrors KubernetesPodOperator in production.
- The proxy shrinks the blast radius of a compromised Airflow from "the host" to
  "can start containers".

## Consequences
- Starting containers is still powerful (one could mount host paths). The proxy
  is a reduction in risk, not isolation; production would use Kubernetes with
  RBAC and pod security instead.
- Job images must be built before DAGs run (`make up-orch` builds them).
- Task logs arrive through the Docker API; very chatty jobs make large task logs.
