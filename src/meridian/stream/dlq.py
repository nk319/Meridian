"""Read the dead letter queue and say what is in it.

    python -m meridian.stream.dlq
    python -m meridian.stream.dlq --show-payload --limit 5

A DLQ nobody reads is a topic that grows. The point of routing a poison message
aside is that somebody can then look at it, decide whether it is a producer bug
or a genuinely corrupt record, and either fix the producer or replay the message
— and none of that is possible without a tool that reads the queue.

This reads with **no consumer group**: it assigns partitions directly and never
commits. Two consequences, both wanted. Looking at the DLQ does not consume it,
so running this twice shows the same messages rather than emptying the queue for
whoever looks next. And it does not appear in the group listing, so `make
stream-lag` is not cluttered by a diagnostic tool that has read to the end.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
from collections import Counter

from ..runlog import EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from . import topics
from .client import base_config, decode


def read_all(limit: int, timeout: float = 5.0) -> list[dict]:
    """Every envelope currently on the DLQ, oldest first.

    Assign-and-seek rather than subscribe: a subscription needs a group id, a
    group id joins a consumer group, and a diagnostic tool that joins a group
    changes what it is measuring.
    """
    from confluent_kafka import Consumer, TopicPartition

    dlq = topics.get(topics.DLQ)
    consumer = Consumer(
        base_config()
        | {
            # Required by librdkafka even for assign-only use. Distinct so it
            # cannot be mistaken for one of the two real groups.
            "group.id": "meridian-dlq-reader",
            "enable.auto.commit": False,
        }
    )

    envelopes: list[dict] = []
    try:
        metadata = consumer.list_topics(dlq.name, timeout=timeout)
        found = metadata.topics.get(dlq.name)
        if found is None or found.error is not None:
            return []

        assignments = []
        remaining = 0
        for partition in sorted(found.partitions):
            tp = TopicPartition(dlq.name, partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=timeout, cached=False)
            if high <= low:
                continue
            # Read the tail rather than the head when the queue is long: the
            # recent failures are the ones somebody is investigating.
            start = max(low, high - limit)
            assignments.append(TopicPartition(dlq.name, partition, start))
            remaining += high - start

        if not assignments:
            return []

        consumer.assign(assignments)
        schema = dlq.schema
        while remaining > 0:
            msg = consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                continue
            remaining -= 1
            try:
                envelopes.append(decode(schema, msg.value()))
            except Exception as exc:  # noqa: BLE001
                # A message on the DLQ that the DLQ's own schema cannot decode.
                # Reported rather than raised: this tool exists to look at
                # broken things, so falling over on one would be poor form.
                envelopes.append(
                    {
                        "failed_at": 0,
                        "consumer_group": "?",
                        "source_topic": "?",
                        "source_partition": -1,
                        "source_offset": -1,
                        "error_type": "UndecodableEnvelope",
                        "error_detail": f"{type(exc).__name__}: {exc}",
                        "key": None,
                        "payload": msg.value(),
                    }
                )
    finally:
        consumer.close()

    return sorted(envelopes, key=_sort_key)


def _when(value) -> str:
    """Format `failed_at`, which comes back as either an int or a datetime.

    fastavro's logical-type handling is asymmetric: `timestamp-millis` accepts
    an integer number of milliseconds on the way in and returns a timezone-aware
    `datetime` on the way out. Both are correct and the round trip is lossless;
    the trap is assuming the type you wrote is the type you read.
    """
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if value:
        return dt.datetime.fromtimestamp(value / 1000, tz=dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
    return "-"


def _sort_key(envelope: dict) -> float:
    value = envelope.get("failed_at")
    if isinstance(value, dt.datetime):
        return value.timestamp()
    return float(value or 0)


def render(envelopes: list[dict], *, show_payload: bool) -> None:
    if not envelopes:
        print("dead letter queue is empty")
        return

    by_error = Counter(e["error_type"] for e in envelopes)
    by_group = Counter(e["consumer_group"] for e in envelopes)

    print(f"{len(envelopes)} dead letters\n")
    print("by error type:")
    for name, count in by_error.most_common():
        print(f"  {count:>5}  {name}")
    print("\nby consumer group:")
    for name, count in by_group.most_common():
        # The same bad message is dead-lettered once by each group that reads
        # the topic. That is correct — each group failed independently — and it
        # is worth seeing, because a message in one group's column and not the
        # other's means the two disagree about what is valid.
        print(f"  {count:>5}  {name}")

    print(f"\n{'when':20} {'group':18} {'source':34} {'error':22} detail")
    print("-" * 120)
    for envelope in envelopes:
        when = _when(envelope.get("failed_at"))
        source = (
            f"{envelope['source_topic']}/"
            f"p{envelope['source_partition']}@{envelope['source_offset']}"
        )
        print(
            f"{when:20} {envelope['consumer_group']:18} {source:34} "
            f"{envelope['error_type']:22} {envelope['error_detail'][:60]}"
        )
        if show_payload and envelope.get("payload"):
            raw = envelope["payload"]
            try:
                shown = raw.decode("utf-8")
            except UnicodeDecodeError:
                # Binary Avro. Base64 rather than a repr full of escapes,
                # because base64 can be pasted back into a replay tool.
                shown = base64.b64encode(raw).decode("ascii")
            print(f"{'':20} payload: {shown[:200]}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=50, help="Most recent N per partition")
    parser.add_argument("--show-payload", action="store_true", help="Print the original bytes")
    args = parser.parse_args(argv)

    log = RunLogger("stream.dlq")
    try:
        envelopes = read_all(args.limit)
    except Exception as exc:  # noqa: BLE001
        log.emit("failed", error=f"broker unreachable: {type(exc).__name__}: {exc}")
        return EXIT_UPSTREAM_UNAVAILABLE

    render(envelopes, show_payload=args.show_payload)
    log.emit(
        "done",
        dead_letters=len(envelopes),
        by_error=dict(Counter(e["error_type"] for e in envelopes)) or None,
        status="SUCCESS",
    )
    return EXIT_OK


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
