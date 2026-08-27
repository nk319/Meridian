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

# dbt lives in its own interpreter and is invoked by absolute path, never
# imported. CONTRACTS.md §6 explains why this is forced rather than tidy:
# Airflow 3.1.3's constraints pin protobuf 4.25.8 and dbt-core needs >=6, so the
# image provably cannot build with them co-installed.
DBT = "/opt/dbt-venv/bin/dbt"
DBT_DIR = "/opt/meridian/dbt"

# Consumed by meridian_rag. Data-aware scheduling rather than a cron guess:
# the RAG index runs when the tickets it indexes have actually landed, not at a
# time somebody hoped the batch would be finished by.
SILVER_READY = Asset("meridian://silver/support_tickets")

# Consumed by nothing yet — the dashboard is Phase 7. Declared now because the
# outlet is what makes "gold is rebuilt" an event other DAGs can subscribe to,
# and adding it later means editing this DAG to add a consumer elsewhere.
GOLD_READY = Asset("meridian://gold/marts")

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

    # ------------------------------------------------------------------
    # dbt. Runs after data_quality, so a BLOCK failure in silver stops the
    # transform rather than propagating bad numbers into gold with a green DAG
    # above them.
    # ------------------------------------------------------------------
    def dbt_step(task_id: str, *args: str, **kwargs) -> BashOperator:
        # `cd` into the project rather than passing --project-dir: dbt writes
        # target/ relative to the project directory either way, and the parse
        # step below reads it from a fixed path.
        command = " ".join([f"cd {DBT_DIR} &&", DBT, *args, "--profiles-dir", DBT_DIR])
        return BashOperator(task_id=task_id, bash_command=command, **kwargs)

    dbt_snapshot = dbt_step(
        "dbt_snapshot",
        "snapshot",
        doc_md=(
            "SCD2 history for customers. Runs *before* the models, not after: "
            "`dim_customer` reads the snapshot table, so a run in the other "
            "order builds today's dimension from yesterday's history and the "
            "dashboard is a day stale in a way nothing reports.\n\n"
            "Idempotent — a second run on unchanged data adds no versions. The "
            "one-time backfill that reconstructs history from the change log is "
            "`scripts/dbt_snapshot_backfill.sh` and deliberately is not a task "
            "here; it is not idempotent and must not run on a schedule."
        ),
    )

    dbt_run = dbt_step(
        "dbt_run",
        "run",
        doc_md=(
            "gold_stg -> gold_int -> gold. Views for staging and intermediate, "
            "tables for the dims, facts and marts the dashboard queries."
        ),
    )

    dbt_test = dbt_step(
        "dbt_test",
        "test",
        # The outlet hangs off the *test*, not off the parse step below it.
        # `dbt_results` runs on all_done — it has to, or a failing test is the
        # one result never recorded — so putting GOLD_READY there would fire
        # the asset after a red test suite and wake every consumer to read a
        # warehouse that just failed its own assertions.
        outlets=[GOLD_READY],
        doc_md=(
            "87 assertions: grain, referential integrity, enum vocabularies, "
            "and the four singular SCD2 tests CONTRACTS.md §8 requires. A "
            "non-zero exit fails this task and stops the gold outlet from "
            "firing, so a consumer scheduled on it does not read a warehouse "
            "that just failed its own tests."
        ),
    )

    dbt_results = step(
        "dbt_results",
        "meridian.dbt.results",
        # The one task in the chain that must run even when the one before it
        # failed: its entire job is to record what dbt found, and a failing
        # `dbt test` is exactly when that record matters. Without this trigger
        # rule the failures dbt caught would be the ones never written down.
        trigger_rule="all_done",
        doc_md=(
            "Parse dbt's run_results.json into meta.dq_check_results with "
            "source='dbt', so every assertion in the platform — Pandera, "
            "custom SQL and dbt alike — is queryable from one table "
            "(CONTRACTS.md §7)."
        ),
    )

    enrich = step(
        "rag_enrich",
        "meridian.rag.enrich",
        "--limit", "200",
        doc_md=(
            "Classify tickets from masked text and write rag.ticket_enrichment, "
            "which fact_support_tickets joins and mart_support_health scores. "
            "Exits 0 doing nothing when no ANTHROPIC_API_KEY is set — the "
            "platform is specified to run end to end without one.\n\n"
            "Before dbt, so a run's predictions reach the same run's marts."
        ),
    )

    bootstrap >> ingest >> build_silver >> load_warehouse >> data_quality
    data_quality >> enrich >> dbt_snapshot >> dbt_run >> dbt_test >> dbt_results
