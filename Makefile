.PHONY: help venv seed test lint clean up down reset ps logs bootstrap load-oltp \
        ingest ingest-incremental silver load-warehouse pipeline dq \
        airflow-build airflow-up airflow-down airflow-logs airflow-test \
        rag-index rag-reindex rag-eval ask search verify

# Prefer the project venv when one exists. The RAG layer needs psycopg,
# fastembed and pgvector; the seed generator is stdlib-only and runs anywhere.
PY := $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
RUN := PYTHONPATH=src $(PY)

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

venv:  ## Create .venv and install the project with its rag and dev extras
	python3 -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/python -m pip install -e ".[rag,dev]"

seed:  ## Generate all source data into seeds/
	$(RUN) -m meridian.seed --out seeds/

up:  ## Start the core profile (Postgres + MinIO) and wait for health
	@test -f .env || { echo "no .env — copy .env.example and fill it in"; exit 1; }
	docker compose --profile core up -d
# --wait is scoped to the two long-running services deliberately. minio-init is
# a one-shot that creates the lake bucket and exits 0, and `--wait` treats a
# zero-exit container as a failure — waiting on the whole profile therefore
# fails on a perfectly healthy stack.
	docker compose --profile core up -d --wait postgres minio
	@echo "core profile ready: postgres healthy, pgvector present, lake bucket created"

down:  ## Stop the stack, keeping volumes
	docker compose --profile core down

reset:  ## Stop and DESTROY volumes, so db/init/*.sql run again on next `make up`
	docker compose --profile core down -v

ps:  ## Show container status (minio-init exiting 0 is success, not failure)
	docker compose --profile core ps -a

logs:  ## Tail Postgres logs — where init script failures surface
	docker compose --profile core logs -f postgres

bootstrap:  ## Create the warehouse tables the pipeline owns (meta, silver, secure)
	$(RUN) -m meridian.warehouse.bootstrap

load-oltp:  ## Populate the OLTP source database from seeds/
	$(RUN) -m meridian.seed.load_oltp

ingest:  ## Full capture of all four batch sources into Bronze
	$(RUN) -m meridian.ingest.oltp    --mode full
	$(RUN) -m meridian.ingest.files   --mode full
	$(RUN) -m meridian.ingest.restapi --mode full
	$(RUN) -m meridian.ingest.vendor  --mode full

ingest-incremental:  ## Watermarked capture — the second and every later run
	$(RUN) -m meridian.ingest.oltp    --mode incremental
	$(RUN) -m meridian.ingest.files   --mode incremental --entity web_events
	$(RUN) -m meridian.ingest.restapi --mode incremental
	$(RUN) -m meridian.ingest.vendor  --mode incremental

silver:  ## Bronze -> Silver: dedup, type, validate, quarantine
	$(RUN) -m meridian.lake.build_silver

load-warehouse:  ## Silver Parquet -> warehouse.silver, via Arrow and COPY
	$(RUN) -m meridian.lake.load_warehouse

pipeline:  ## The whole batch chain, from source system to warehouse
	$(MAKE) bootstrap
	$(MAKE) load-oltp
	$(MAKE) ingest
	$(MAKE) silver
	$(MAKE) load-warehouse
	$(MAKE) dq

dq:  ## Run every data quality suite; exits 2 on a BLOCK failure
	$(RUN) -m meridian.dq.run --suite all

airflow-build:  ## Build the orchestration image (three isolated interpreters)
# Stage the local CA if this machine sits behind a TLS-inspecting proxy. Absent
# on an ordinary network, in which case the Dockerfile skips the step.
	@mkdir -p airflow/certs
	@cp /root/.ccr/ca-bundle.crt airflow/certs/proxy-ca.crt 2>/dev/null && \
	  echo "staged a local CA for the build" || true
# --network host so the build can reach the package index through whatever
# egress this machine has; container builds cannot see a loopback proxy.
	docker build --network host -f airflow/Dockerfile -t meridian-airflow:3.1.3 .

airflow-up:  ## Start Airflow (needs `make up` first)
	docker compose --profile full up -d airflow-init
	docker compose --profile full up -d airflow-apiserver airflow-scheduler airflow-dag-processor
	@echo "Airflow at http://localhost:$${AIRFLOW_PORT:-8080}"

airflow-down:  ## Stop Airflow, leaving Postgres and MinIO up
	docker compose --profile full rm -sf airflow-apiserver airflow-scheduler airflow-dag-processor airflow-init

airflow-logs:  ## Tail the scheduler
	docker compose --profile full logs -f airflow-scheduler

airflow-test:  ## Parse the DAGs and report import errors
	docker compose --profile full run --rm airflow-init bash -c \
	  "airflow dags list && airflow dags list-import-errors"

rag-index:  ## Chunk, mask, embed and upsert the ticket corpus
	$(RUN) -m meridian.rag.index

rag-reindex:  ## Same, but discard the store first and rebuild from scratch
	$(RUN) -m meridian.rag.index --rebuild

rag-eval:  ## Measure recall@5 for hybrid, vector-only and lexical-only
	$(RUN) -m meridian.rag.evaluate --min-recall 0.80

search:  ## Hybrid search. Usage: make search Q="tracking has not updated"
	@$(RUN) -m meridian.rag.retrieve "$(Q)"

ask:  ## Answer a question. Usage: make ask Q="why do customers ask for refunds?"
	@$(RUN) -m meridian.rag.generate "$(Q)"

test:  ## Run the test suite. DB-backed tests skip when the stack is down.
	$(PY) -m pytest

lint:  ## Lint and format check
	$(PY) -m ruff check src tests
	$(PY) -m ruff format --check src tests

verify:  ## Everything the README claims, from a cold start
	$(MAKE) lint
	$(MAKE) seed
	$(MAKE) up
	$(MAKE) pipeline
	$(MAKE) rag-index
	$(MAKE) test
	$(MAKE) rag-eval

clean:  ## Remove generated data
	rm -rf seeds/ .pytest_cache/ .ruff_cache/
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
