# Tidewater — developer entry points. Run `make help` for the list.
SHELL := /bin/bash
COMPOSE := docker compose
comma := ,
RATE ?= 20
CHAOS_RATE ?= 100
KILLS ?= 3
DURATION ?= 0

.DEFAULT_GOAL := help
.PHONY: help init up-core up-wh up-all down nuke ps logs register sim sim-burst \
        psql-src psql-wh topics test test-spark verify-bronze chaos-bronze backfill reconcile soak load-wh dbt dbt-build dbt-freshness metrics dashboard maintenance up-orch airflow-password test-airflow tf-plan tf-apply integration up-obs chaos up-lineage audit-trace demo bench sf-keypair sf-plan sf-apply load-sf dbt-sf lint fmt pre-commit

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

init: ## Create .env from the example and install dev tooling
	@test -f .env || cp .env.example .env
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements-dev.txt
	.venv/bin/pre-commit install

.env:
	cp .env.example .env

up-core: .env ## Start source DB, Kafka, Debezium, registry, S3, Kafka UI, bronze + silver streams
	$(COMPOSE) --profile core up -d --wait
	@echo "Kafka UI: http://localhost:8080  S3: http://localhost:8333 (filer UI :8888)  Connect: http://localhost:8083"

up-wh: .env ## Start the Postgres warehouse and Metabase (~2 GB)
	$(COMPOSE) --profile wh up -d --wait

up-obs: .env ## Start Prometheus, Grafana (http://localhost:3001), Kafka exporter, alert sink
	$(COMPOSE) --profile core --profile obs up -d prometheus kafka-exporter grafana alert-sink

up-lineage: .env ## Start Marquez (API :5005, UI http://localhost:3002); set OPENLINEAGE_URL to emit
	$(COMPOSE) --profile wh up -d --wait postgres-wh
	$(COMPOSE) exec -T postgres-wh sh -c 'psql -q -U $$POSTGRES_USER -d $$POSTGRES_DB' < infra/postgres-wh/init/03_marquez.sql
	$(COMPOSE) --profile wh --profile lineage up -d marquez marquez-web

audit-trace: ## Trace one order from the mart back to Kafka offsets and WAL LSNs (ID=1234)
	$(SPARK_RUN) $(SUBMIT) jobs/audit_trace.py --order-id $(ID)

demo: ## 5-minute demo: live load, crash mid-batch, recovery, proof (DEMO_PAUSE=1 for recording)
	./scripts/demo.sh

bench: ## Throughput/latency sweep (RATES="100 500 1000 2000" STEP=180), results in bench/results/
	./scripts/bench.sh $(or $(LABEL),baseline)

# ---- Snowflake (week 9). Credentials: SNOWFLAKE_* in .env, private key in .secrets/ ----
SF_TF := docker run --rm -v $(PWD)/terraform/snowflake:/tf -w /tf --env-file .env \
	-v $(PWD)/.secrets:/secrets:ro -e TF_VAR_private_key_path=/secrets/terraform_key.p8 \
	-e TF_VAR_service_user_rsa_public_key hashicorp/terraform:1.9.8

sf-keypair: ## Generate the service user's RSA key pair into .secrets/
	./scripts/snowflake_keypair.sh

sf-plan: ## Terraform plan for Snowflake objects (warehouse, DB, roles, service user, monitor)
	export TF_VAR_service_user_rsa_public_key="$$(grep -v -- '-----' .secrets/snowflake_key.pub | tr -d '\n')"; \
	$(SF_TF) init -input=false -no-color >/dev/null && $(SF_TF) plan -input=false -no-color

sf-apply: ## Terraform apply for Snowflake objects
	export TF_VAR_service_user_rsa_public_key="$$(grep -v -- '-----' .secrets/snowflake_key.pub | tr -d '\n')"; \
	$(SF_TF) init -input=false -no-color >/dev/null && $(SF_TF) apply -input=false -auto-approve -no-color

load-sf: ## Incrementally load silver into Snowflake (TIDEWATER.SILVER)
	$(SPARK_RUN) $(SUBMIT) jobs/load_snowflake.py $(ARGS)

dbt-sf: ## Build and test every dbt model on Snowflake (same project, --target snowflake)
	$(COMPOSE) --profile wh run --rm -e DBT_TARGET=snowflake dbt deps
	$(COMPOSE) --profile wh run --rm -e DBT_TARGET=snowflake dbt build

up-orch: .env ## Start Airflow (UI http://localhost:8085) and its Docker socket proxy
	$(COMPOSE) --profile wh up -d --wait postgres-wh
	$(COMPOSE) exec -T postgres-wh sh -c 'psql -q -U $$POSTGRES_USER -d $$POSTGRES_DB' < infra/postgres-wh/init/02_airflow.sql
	$(COMPOSE) build spark-bronze && $(COMPOSE) --profile wh build dbt
	$(COMPOSE) --profile wh --profile orch up -d airflow

airflow-password: ## Print the generated Airflow admin password (user: admin)
	@$(COMPOSE) --profile wh --profile orch exec airflow cat /opt/airflow/standalone_admin_password.txt; echo

test-airflow: ## DAG integrity tests inside the Airflow image
	$(COMPOSE) --profile wh --profile orch run --rm --no-deps -v $(PWD)/tests/airflow:/opt/airflow/tests airflow \
	  bash -c "pip install -q pytest==8.3.2 && python -m pytest -q -p no:cacheprovider /opt/airflow/tests"

TF := docker run --rm --network $${COMPOSE_PROJECT_NAME:-tidewater}_default -v $(PWD)/terraform/local:/tf -w /tf \
	-e TF_VAR_s3_access_key -e TF_VAR_s3_secret_key hashicorp/terraform:1.9.8
TF_ENV := set -a; . ./.env; set +a; export TF_VAR_s3_access_key=$$S3_ACCESS_KEY TF_VAR_s3_secret_key=$$S3_SECRET_KEY;

tf-plan: ## Terraform plan for local S3 buckets (runs in a container on the Compose network)
	$(TF_ENV) $(TF) init -input=false -no-color >/dev/null && $(TF_ENV) $(TF) plan -input=false -no-color

tf-apply: ## Terraform apply for local S3 buckets
	$(TF_ENV) $(TF) init -input=false -no-color >/dev/null && $(TF_ENV) $(TF) apply -input=false -auto-approve -no-color

integration: ## End-to-end test: 1,000 events through the pipeline into dbt marts
	./tests/integration/run.sh

up-all: .env ## Start everything (16 GB machines only)
	$(COMPOSE) --profile all up -d --wait

down: ## Stop all containers (keeps data volumes)
	$(COMPOSE) --profile all --profile sim down

nuke: ## Stop everything AND delete all data volumes
	$(COMPOSE) --profile all --profile sim down -v

ps: ## Show running services
	$(COMPOSE) --profile all ps

logs: ## Tail logs, e.g. make logs S=connect
	$(COMPOSE) --profile all logs -f $(S)

register: ## Set registry to BACKWARD compatibility and (re)register the Debezium connector
	./scripts/register_connector.sh

sim: ## Run the transaction simulator, e.g. make sim RATE=100 DURATION=600
	$(COMPOSE) --profile sim run --rm --build simulator --rate $(RATE) --duration $(DURATION)

topics: ## List CDC topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

psql-src: ## Open psql on the source database
	$(COMPOSE) exec postgres-src sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB'

psql-wh: ## Open psql on the warehouse
	$(COMPOSE) exec postgres-wh sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB'

test: ## Run unit tests (no Spark needed)
	.venv/bin/pytest -q

SPARK_RUN := $(COMPOSE) run --rm --no-deps --build spark-bronze

test-spark: ## Run Spark unit tests inside the Spark image
	$(SPARK_RUN) python3 -m pytest -q -p no:cacheprovider tests

verify-bronze: ## Check bronze has every Kafka offset exactly once (add ARGS=--require-caught-up)
	$(SPARK_RUN) $(SUBMIT) jobs/verify_bronze.py $(ARGS)

SUBMIT := /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g

backfill: ## Backfill silver from source while the stream runs (ARGS="--table orders --detect-deletes")
	$(SPARK_RUN) $(SUBMIT) jobs/backfill.py $(ARGS)

reconcile: ## Compare silver to the source DB: counts + per-row hashes (run when source is quiet)
	$(SPARK_RUN) $(SUBMIT) jobs/reconcile.py $(ARGS)

load-wh: ## Incrementally load silver into the Postgres warehouse (ARGS="--interval 60" to loop)
	$(SPARK_RUN) $(SUBMIT) jobs/load_warehouse.py $(ARGS)

DBT := $(COMPOSE) --profile wh run --rm --build dbt

dbt: ## Run any dbt command, e.g. make dbt CMD="run -s stg_orders"
	$(DBT) $(CMD)

dbt-build: ## Install dbt packages, then snapshot, build and test every model
	$(DBT) deps
	$(DBT) build

metrics: ## Query the semantic layer, e.g. make metrics Q="--metrics gmv,aov --group-by metric_time__day"
	$(COMPOSE) --profile wh run --rm --build --entrypoint mf dbt query $(or $(Q),--metrics gmv$(comma)orders$(comma)aov$(comma)refund_rate --group-by metric_time__day --order -metric_time__day --limit 7 --decimals 2)

dbt-freshness: ## Check silver source freshness thresholds
	$(DBT) source freshness

dashboard: ## Provision the Metabase dashboard as code (idempotent)
	$(COMPOSE) exec -T postgres-wh sh -c 'psql -q -U $$POSTGRES_USER -d $$POSTGRES_DB' < infra/postgres-wh/init/01_schemas.sql
	python3 scripts/metabase_setup.py

maintenance: ## Compact small files and vacuum bronze/silver (ARGS="--skip-vacuum")
	$(SPARK_RUN) $(SUBMIT) jobs/maintenance.py $(ARGS)

soak: ## Load test: simulator + mid-run concurrent backfill, then verify bronze and reconcile silver
	./scripts/soak.sh $(or $(DURATION),600) $(or $(SOAK_RATE),50)

chaos: ## Run one chaos test, e.g. make chaos N=03 (see tests/chaos/)
	./tests/chaos/$(N)_*.sh $(ARGS)

chaos-bronze: ## Chaos test 01: SIGKILL bronze mid-batch N times, then verify (KILLS=3 CHAOS_RATE=100)
	./tests/chaos/01_kill_bronze_mid_batch.sh $(KILLS) $(CHAOS_RATE)

lint: ## Lint Python and SQL
	.venv/bin/ruff check .
	.venv/bin/black --check .
	.venv/bin/sqlfluff lint infra

fmt: ## Auto-format Python
	.venv/bin/ruff check --fix .
	.venv/bin/black .

pre-commit: ## Run every pre-commit hook on all files
	.venv/bin/pre-commit run --all-files
