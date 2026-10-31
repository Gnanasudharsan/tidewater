# Tidewater — developer entry points. Run `make help` for the list.
SHELL := /bin/bash
COMPOSE := docker compose
RATE ?= 20
DURATION ?= 0

.DEFAULT_GOAL := help
.PHONY: help init up-core up-wh up-all down nuke ps logs register sim sim-burst \
        psql-src psql-wh topics test lint fmt pre-commit

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

init: ## Create .env from the example and install dev tooling
	@test -f .env || cp .env.example .env
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements-dev.txt
	.venv/bin/pre-commit install

.env:
	cp .env.example .env

up-core: .env ## Start source DB, Kafka, Debezium, schema registry, S3 store, Kafka UI (~4 GB)
	$(COMPOSE) --profile core up -d --wait
	@echo "Kafka UI: http://localhost:8080  S3: http://localhost:8333 (filer UI :8888)  Connect: http://localhost:8083"

up-wh: .env ## Start the Postgres warehouse and Metabase (~2 GB)
	$(COMPOSE) --profile wh up -d --wait

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

test: ## Run unit tests
	.venv/bin/pytest -q

lint: ## Lint Python and SQL
	.venv/bin/ruff check .
	.venv/bin/black --check .
	.venv/bin/sqlfluff lint infra

fmt: ## Auto-format Python
	.venv/bin/ruff check --fix .
	.venv/bin/black .

pre-commit: ## Run every pre-commit hook on all files
	.venv/bin/pre-commit run --all-files
