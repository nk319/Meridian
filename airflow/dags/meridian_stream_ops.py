"""Watch the streaming path. It runs continuously; this notices when it stops.

The consumers are long-running processes, not tasks — Airflow schedules work
that starts and finishes, and a consumer that finishes is a consumer that has
stopped consuming. So this DAG does not run them. It records what they are
doing, which is a different job and the one a scheduler is good at.

Lag is the metric, and the reason it needs a *schedule* rather than an alert
threshold is that a single reading cannot tell the two failure modes apart. A
consumer that has died and one that is merely slower than the producer both show
lag; the dead one's climbs without bound and the slow one's plateaus. Only a
series distinguishes them, which is why `meta.kafka_consumer_offsets` has
`observed_at` in its primary key and this runs every fifteen minutes.

Runs hourly-ish rather than daily because a broker's interesting timescale is
minutes. It is also the one DAG here that is expected to be a no-op most of the
time — which is the point of it.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PY = "/opt/meridian-venv/bin/python"


def step(task_id: str, module: str, *args: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=" ".join([PY, "-m", module, *args]),
        **kwargs,
    )


with DAG(
    dag_id="meridian_stream_ops",
    description="Record Kafka consumer lag and keep the declared topics in existence",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        # One retry, short. A broker that is briefly unreachable is worth
        # retrying; one that is down for five minutes should show as a failed
        # run rather than a task that eventually succeeded and hid the outage.
        "retries": 1,
        "retry_delay": dt.timedelta(seconds=30),
    },
    tags=["meridian", "streaming"],
    doc_md=__doc__,
) as dag:
    ensure_topics = step(
        "ensure_topics",
        "meridian.stream.admin",
        "--create",
        doc_md=(
            "Idempotent. Runs on a schedule rather than once at setup because a "
            "topic can be deleted, and a producer that finds no topic will "
            "happily auto-create one with the broker's default partition count "
            "— which is right for nothing and wrong silently."
        ),
    )

    check_drift = step(
        "check_topic_drift",
        "meridian.stream.admin",
        "--describe",
        doc_md=(
            "Fails if the broker's partition counts disagree with "
            "contracts/topics.yml. That is the shape an auto-created topic "
            "takes, and the failure mode is a loss of parallelism nothing else "
            "reports."
        ),
    )

    record_lag = step(
        "record_lag",
        "meridian.stream.lag",
        "--max-lag",
        "50000",
        doc_md=(
            "Appends one row per (group, topic, partition) to "
            "meta.kafka_consumer_offsets and exits 2 above the ceiling.\n\n"
            "50,000 is deliberately generous: this is a demo whose producer is "
            "run by hand, so a burst of a few thousand behind a stopped "
            "consumer is normal and not worth paging for. The number that "
            "matters is the trend in the table, not this threshold."
        ),
    )

    ensure_topics >> check_drift >> record_lag
