"""Re-index the ticket corpus and re-measure retrieval, when Silver changes.

Scheduled on an Asset rather than a cron expression. The batch DAG declares
`meridian://silver/support_tickets` as an outlet of its warehouse load, and this
DAG consumes it — so indexing runs when the tickets it indexes have actually
landed, not at a time somebody guessed the batch would be finished by. A cron
here would either fire too early on a slow day or waste an hour every fast one.

Indexing is cheap to repeat by design: the content hash means an unchanged
corpus re-embeds nothing, so a spurious trigger costs about five seconds rather
than three minutes.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset

PY = "/opt/meridian-venv/bin/python"

SILVER_READY = Asset("meridian://silver/support_tickets")
RAG_INDEXED = Asset("meridian://rag/chunks")

with DAG(
    dag_id="meridian_rag",
    description="Mask, embed and index support tickets, then measure retrieval quality",
    schedule=[SILVER_READY],
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
    },
    tags=["meridian", "ai"],
    doc_md=__doc__,
) as dag:
    index = BashOperator(
        task_id="rag_index",
        bash_command=f"{PY} -m meridian.rag.index --source silver",
        outlets=[RAG_INDEXED],
        doc_md=(
            "Reads silver.support_tickets as `rag_indexer`, masks by dictionary "
            "before embedding, and skips chunks whose content hash is unchanged. "
            "Refuses to embed a chunk that still contains a manifest term."
        ),
    )

    evaluate = BashOperator(
        task_id="rag_eval",
        bash_command=f"{PY} -m meridian.rag.evaluate --min-recall 0.80",
        doc_md=(
            "Scores the golden set and exits 2 below the acceptance bar. This is "
            "the task that turns 'retrieval still works' from an assumption into "
            "something the scheduler finds out about."
        ),
    )

    index >> evaluate
