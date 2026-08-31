"""Record consumer lag into `meta.kafka_consumer_offsets`, and print it.

    python -m meridian.stream.lag
    python -m meridian.stream.lag --max-lag 10000    # exit 2 above the ceiling

Lag is the one streaming metric that matters operationally, and the reason is
that it is the only one that distinguishes the two failure modes that look
identical from outside: a consumer that has stopped, and a consumer that is
merely slower than the producer. A stopped consumer's lag climbs linearly
forever; a slow one's climbs and then plateaus. You cannot tell them apart from
a single reading, which is why this appends a time series rather than
overwriting a gauge — `meta.kafka_consumer_offsets` has `observed_at` in its
primary key precisely so history accumulates.

Read via the admin API rather than by subscribing. A tool that joined the group
to read its position would trigger a rebalance every time it ran, which would
make the act of measuring lag a cause of lag.
"""

from __future__ import annotations

import argparse
import sys
import uuid

from ..db import UpstreamUnavailable, connect
from ..runlog import (
    EXIT_DQ_BLOCK,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_UPSTREAM_UNAVAILABLE,
    RunLogger,
)
from . import topics
from .client import admin_client, base_config

INSERT_SQL = """
INSERT INTO meta.kafka_consumer_offsets
    (consumer_group, topic, partition, current_offset, log_end_offset, observed_at)
VALUES (%s, %s, %s, %s, %s, now())
"""


def collect(timeout: float = 30.0) -> list[dict]:
    """Committed offset and log end offset, per (group, topic, partition).

    Both numbers are needed and neither is enough. The committed offset alone
    says where a group is but not whether that is behind; the log end offset
    alone says how much exists but not how much was read. Lag is the difference,
    and a table storing only one of them cannot recompute it later.
    """
    from confluent_kafka import Consumer, TopicPartition

    admin = admin_client()
    metadata = admin.list_topics(timeout=timeout)
    rows: list[dict] = []

    for topic in topics.load().values():
        found = metadata.topics.get(topic.name)
        if found is None or found.error is not None:
            continue

        for group in topic.consumer_groups:
            # A consumer instance is created to read committed offsets and to
            # query watermarks, but deliberately never subscribed — an
            # unsubscribed consumer does not join the group, so this does not
            # cause the rebalance it is measuring.
            probe = Consumer(
                base_config()
                | {
                    "group.id": group,
                    "enable.auto.commit": False,
                    # A distinct client id so this shows up as a tool rather than
                    # as a mysterious extra member in the broker's logs.
                    "client.id": f"meridian-lag-{uuid.uuid4().hex[:8]}",
                }
            )
            try:
                partitions = [TopicPartition(topic.name, p) for p in sorted(found.partitions)]
                committed = probe.committed(partitions, timeout=timeout)
                for tp in committed:
                    low, high = probe.get_watermark_offsets(tp, timeout=timeout, cached=False)
                    # -1001 is librdkafka's OFFSET_INVALID: the group exists but
                    # has never committed this partition. Recorded as the low
                    # watermark, because "has read nothing" is the truth and a
                    # null would make the lag arithmetic undefined.
                    current = tp.offset if tp.offset is not None and tp.offset >= 0 else low
                    rows.append(
                        {
                            "consumer_group": group,
                            "topic": topic.name,
                            "partition": tp.partition,
                            "current_offset": current,
                            "log_end_offset": high,
                            "lag": max(0, high - current),
                            "committed": tp.offset is not None and tp.offset >= 0,
                        }
                    )
            finally:
                probe.close()

    return rows


def persist(rows: list[dict]) -> None:
    with connect("meridian_etl", vectors=False) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                INSERT_SQL,
                [
                    (
                        r["consumer_group"],
                        r["topic"],
                        r["partition"],
                        r["current_offset"],
                        r["log_end_offset"],
                    )
                    for r in rows
                ],
            )
        conn.commit()


def render(rows: list[dict]) -> None:
    print(f"{'group':18} {'topic':24} {'part':>5} {'committed':>11} {'end':>9} {'lag':>8}")
    print("-" * 80)
    for row in sorted(rows, key=lambda r: (r["consumer_group"], r["topic"], r["partition"])):
        marker = "" if row["committed"] else "  (never committed)"
        print(
            f"{row['consumer_group']:18} {row['topic']:24} {row['partition']:>5} "
            f"{row['current_offset']:>11} {row['log_end_offset']:>9} {row['lag']:>8}{marker}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-lag",
        type=int,
        default=None,
        help=(
            "Exit 2 if any partition's lag exceeds this. For an Airflow task "
            "that should page rather than merely record."
        ),
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Print only. For a quick look without a row in the time series.",
    )
    args = parser.parse_args(argv)

    log = RunLogger("stream.lag")

    try:
        rows = collect()
    except Exception as exc:  # noqa: BLE001
        log.emit("failed", error=f"broker unreachable: {type(exc).__name__}: {exc}")
        return EXIT_UPSTREAM_UNAVAILABLE

    if not rows:
        log.emit("no_groups", reason="no declared topic has a consumer group with offsets")
        return EXIT_OK

    render(rows)

    if not args.no_persist:
        try:
            persist(rows)
        except UpstreamUnavailable as exc:
            log.emit("failed", error=str(exc))
            return EXIT_UPSTREAM_UNAVAILABLE
        except Exception as exc:  # noqa: BLE001
            log.emit("failed", error=f"{type(exc).__name__}: {exc}")
            return EXIT_ERROR

    total_lag = sum(r["lag"] for r in rows)
    worst = max(rows, key=lambda r: r["lag"])
    log.emit(
        "done",
        partitions=len(rows),
        total_lag=total_lag,
        max_lag=worst["lag"],
        max_lag_group=worst["consumer_group"],
        status="SUCCESS",
    )

    if args.max_lag is not None and worst["lag"] > args.max_lag:
        print(
            f"  LAG CEILING EXCEEDED: {worst['consumer_group']} is "
            f"{worst['lag']} behind on {worst['topic']}/{worst['partition']} "
            f"(ceiling {args.max_lag})",
            file=sys.stderr,
        )
        return EXIT_DQ_BLOCK
    return EXIT_OK


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
