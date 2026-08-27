"""The batch pipeline: source systems to warehouse, gated by data quality.

Every task is a subprocess. Not one of them imports anything from `meridian`,
and that is the design rather than an accident — CONTRACTS.md §5 makes each
pipeline step a module entrypoint runnable without Airflow, and this DAG calls
those same commands. Two things follow:

- `make pipeline` proves the platform works with the orchestrator switched off,
  because the orchestrator is not in the code path.
- A dependency change in the pipeline cannot break the scheduler. The pipeline
  lives in `/opt/meridian-venv` and Airflow cannot see into it (airflow/Dockerfile
  explains why that separation is forced rather than chosen).

The exit codes are load-bearing here. `meridian.dq.run` returns 2 when a
BLOCK-severity check fails, which fails the task and stops everything downstream
— which is the entire point of having a severity column.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset

PY = "/opt/meridian-venv/bin/python"

# Consumed by meridian_rag. Data-aware scheduling rather than a cron guess:
# the RAG index runs when the tickets it indexes have actually landed, not at a
# time somebody hoped the batch would be finished by.
SILVER_READY = Asset("meridian://silver/support_tickets")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=2),
    # Ingestion is idempotent — Bronze is append-only and Silver dedups on
    # _record_hash — so a retry re-reads rather than double-counting.
    "depends_on_past": False,
}


def step(task_id: str, module: str, *args: str, **kwargs) -> BashOperator:
    command = " ".join([PY, "-m", module, *args])
    return BashOperator(task_id=task_id, bash_command=command, **kwargs)


with DAG(
    dag_id="meridian_batch",
    description="Source systems -> Bronze -> Silver -> warehouse, gated by data quality",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["meridian", "batch"],
    doc_md=__doc__,
) as dag:
    bootstrap = step(
        "warehouse_bootstrap",
        "meridian.warehouse.bootstrap",
        doc_md="Create meta, silver and secure if they are not there. Idempotent, "
        "so it runs every time rather than being a step someone must remember.",
    )

    # The four sources are independent and each owns a disjoint set of entities
    # (CONTRACTS.md §12), so they fan out. If two of them wrote the same entity
    # this parallelism would be a race; the ownership rule is what makes it safe.
    ingest = [
        step("ingest_oltp", "meridian.ingest.oltp", "--mode", "incremental"),
        step("ingest_files", "meridian.ingest.files", "--mode", "incremental",
             "--entity", "web_events"),
        step("ingest_files_full", "meridian.ingest.files", "--mode", "full",
             "--entity", "products",
             doc_md="products has no event timestamp, so it is full-refresh only. "
                    "Saying so here beats a --mode flag that is silently ignored."),
        step("ingest_restapi", "meridian.ingest.restapi", "--mode", "incremental"),
        step("ingest_vendor", "meridian.ingest.vendor", "--mode", "incremental"),
    ]

    build_silver = step(
        "build_silver",
        "meridian.lake.build_silver",
        doc_md="Dedup, type, validate, quarantine. Exits 2 if an entity exceeds "
        "the quarantine ceiling, which stops the load rather than publishing a "
        "table missing most of its rows.",
    )

    load_warehouse = step(
        "load_warehouse",
        "meridian.lake.load_warehouse",
        outlets=[SILVER_READY],
        doc_md="Silver Parquet into warehouse.silver via Arrow and binary COPY. "
        "Truncate-and-load inside one transaction, so a failure leaves the "
        "previous contents intact.",
    )

    data_quality = step(
        "data_quality",
        "meridian.dq.run",
        "--suite", "all",
        doc_md="Referential integrity, business invariants, volume, freshness and "
        "the Pandera distribution schemas. Exit 2 on any BLOCK failure.",
    )

    bootstrap >> ingest >> build_silver >> load_warehouse >> data_quality
