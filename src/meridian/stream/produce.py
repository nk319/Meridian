"""Replay seed data onto the topics, as a live stream.

    python -m meridian.stream.produce --topic ecom.web.events.v1 --count 5000
    python -m meridian.stream.produce --topic ecom.web.events.v1 --rate 200 --duration 30
    python -m meridian.stream.produce --defects 0.02        # feed the DLQ

Replay rather than a synthetic generator: the same `seeds/` rows the batch path
ingests are the ones that go on the wire, so the streaming and batch views of
the world are of the *same events*. A separate stream generator would make
"batch and stream agree" untestable, which is the one thing about a Lambda-shaped
architecture worth demonstrating.

`--defects` is not a curiosity. A DLQ that has never received a message proves
nothing, and the interesting property of a dead letter queue is not that it
exists but that the consumer keeps its offset moving past a poison message
instead of stalling the partition forever. The malformed records this injects
are what make that observable.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import decimal
import random
import sys
import time

from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from ..settings import settings
from . import topics
from .client import encode, producer_config


def _ts_millis(value: str) -> int:
    """ISO-8601 to epoch milliseconds, which is what the Avro logical type wants."""
    return int(dt.datetime.fromisoformat(value).timestamp() * 1000)


def read_rows(path, limit: int | None) -> list[dict]:
    """Raw CSV rows, unconverted.

    Reading and converting are separate steps on purpose. `seeds/` carries
    deliberately malformed rows — `seed/defects.py` injects unparseable
    timestamps, out-of-vocabulary enums and empty required fields — and a reader
    that converted as it read would raise on the first one and take the whole
    run down. Those rows are not a problem to route around: they are the most
    realistic messages this producer can send, and what the DLQ exists for.
    """
    rows = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(row)
            if limit and len(rows) >= limit:
                break
    return rows


def build_web_event(row: dict) -> dict:
    return {
        "event_id": row["event_id"],
        "session_id": row["session_id"],
        # Anonymous browsing is real, and the CSV writes it as an empty string.
        "customer_id": row["customer_id"] or None,
        "event_ts": _ts_millis(row["event_ts"]),
        "event_type": row["event_type"],
        "product_id": row["product_id"] or None,
        "order_id": row["order_id"] or None,
        "channel": row["channel"],
        "device_type": row["device_type"],
    }


def build_order_placed(row: dict) -> dict:
    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "order_ts": _ts_millis(row["order_ts"]),
        "channel": row["channel"],
        "device_type": row["device_type"],
        # Decimal, not float. The Avro logical type is backed by bytes and
        # fastavro does the scaling; a float here would reintroduce exactly the
        # rounding the type exists to avoid.
        "gross_amount": decimal.Decimal(row["gross_amount"]),
        "total_amount": decimal.Decimal(row["total_amount"]),
        "line_count": 0,
    }


SOURCES = {
    topics.WEB_EVENTS: (("files", "web_events.csv"), build_web_event),
    topics.ORDERS_PLACED: (("oltp", "orders.csv"), build_order_placed),
}


def corrupt(record: dict, rng: random.Random) -> tuple[bytes, str]:
    """Produce something the consumer cannot decode, and say which kind.

    Three kinds, because they fail at three different places and a DLQ that only
    ever sees one of them has not been tested:

      `not_avro`      plain JSON on an Avro topic. Fails on the magic bytes,
                      before any schema is consulted.
      `truncated`     a valid header and a datum cut in half. Fails inside the
                      Avro reader, mid-record.
      `wrong_schema`  a valid message under a different schema. Decodes
                      *successfully* under the wrong schema in a naive consumer
                      and produces plausible nonsense, which is why the
                      fingerprint check in client.py exists.
    """
    kind = rng.choice(["not_avro", "truncated", "wrong_schema"])
    if kind == "not_avro":
        import json

        return json.dumps(record, default=str).encode("utf-8"), kind
    if kind == "truncated":
        payload = encode(topics.get(topics.WEB_EVENTS).schema, record)
        return payload[: len(payload) // 2], kind
    return (
        encode(
            topics.get(topics.DLQ).schema,
            {
                "failed_at": 0,
                "consumer_group": "n/a",
                "source_topic": "n/a",
                "source_partition": 0,
                "source_offset": 0,
                "error_type": "synthetic",
                "error_detail": "a valid message under the wrong schema",
                "key": None,
                "payload": None,
            },
        ),
        kind,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default=topics.WEB_EVENTS, choices=sorted(SOURCES))
    parser.add_argument("--count", type=int, default=5000, help="Messages to send")
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Messages per second. 0 (default) sends as fast as the broker accepts.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Stop after this many seconds. Requires --rate; overrides --count.",
    )
    parser.add_argument(
        "--defects",
        type=float,
        default=0.0,
        help="Fraction of messages to corrupt, for the DLQ path. 0.02 is a good demo.",
    )
    parser.add_argument("--seed", type=int, default=17, help="RNG seed for defect placement")
    args = parser.parse_args(argv)

    log = RunLogger("stream.produce")
    rng = random.Random(args.seed)
    topic = topics.get(args.topic)

    limit = None if args.duration else args.count
    (directory, filename), build = SOURCES[args.topic]
    rows = read_rows(settings().seeds_dir / directory / filename, limit)
    if not rows:
        log.emit("failed", error="no seed data — run `make seed`")
        return EXIT_ERROR

    from confluent_kafka import KafkaException, Producer

    producer = Producer(producer_config())

    delivered = {"ok": 0, "failed": 0}

    def on_delivery(err, msg):
        # Delivery is asynchronous, and this is the only place a failure
        # surfaces. Without a callback, `produce()` returning is not evidence of
        # anything and a broker rejecting every message looks like success.
        if err is None:
            delivered["ok"] += 1
        else:
            delivered["failed"] += 1
            log.emit("delivery_failed", error=str(err))

    schema = topic.schema
    interval = 1.0 / args.rate if args.rate else 0.0
    deadline = time.monotonic() + args.duration if args.duration else None

    sent = corrupted = source_defects = 0
    started = time.perf_counter()
    index = 0

    try:
        while True:
            if deadline is not None:
                if time.monotonic() >= deadline:
                    break
            elif sent >= args.count:
                break

            # Cycle rather than stop, so `--duration` can outlast the seed file.
            row = rows[index % len(rows)]
            index += 1

            try:
                record = build(row)
                key = topic.key_for(record)
            except (ValueError, KeyError, decimal.InvalidOperation) as exc:
                # A row the source system emitted that this producer cannot turn
                # into a valid message. Sent anyway, as raw JSON, so the
                # consumer's DLQ path handles it — which is the honest
                # simulation. Dropping it here would mean the DLQ only ever
                # receives synthetic corruption, and "the consumer survives bad
                # input" would be a claim about test data rather than about real
                # data.
                import json as _json

                payload = _json.dumps(row).encode("utf-8")
                key = (row.get(topic.key_field) or "").encode() or None
                source_defects += 1
                log.emit(
                    "source_defect",
                    error=f"{type(exc).__name__}: {exc}",
                    key=row.get("event_id") or row.get("order_id"),
                )
                producer.produce(topic.name, key=key, value=payload, on_delivery=on_delivery)
                sent += 1
                producer.poll(0)
                continue

            if args.defects and rng.random() < args.defects:
                payload, kind = corrupt(record, rng)
                corrupted += 1
                log.emit("corrupted", kind=kind, key=key.decode() if key else None)
            else:
                try:
                    payload = encode(schema, record)
                except Exception as exc:  # noqa: BLE001 — fastavro raises several types
                    # An enum value outside CONTRACTS.md §9, most likely. The
                    # schema catches it here, at the producer, where the message
                    # names the field — which is the argument for enums in the
                    # Avro schema rather than plain strings.
                    import json as _json

                    payload = _json.dumps(row).encode("utf-8")
                    source_defects += 1
                    log.emit("source_defect", error=f"{type(exc).__name__}: {exc}")

            while True:
                try:
                    producer.produce(topic.name, key=key, value=payload, on_delivery=on_delivery)
                    break
                except BufferError:
                    # The local queue is full: the broker is slower than this
                    # loop. Serving delivery callbacks drains it. Dropping the
                    # message instead would be silent data loss in the one
                    # component whose job is not to lose messages.
                    producer.poll(0.1)

            sent += 1
            producer.poll(0)
            if interval:
                time.sleep(interval)

        remaining = producer.flush(timeout=30)
        if remaining:
            log.emit("flush_incomplete", still_queued=remaining)
    except KafkaException as exc:
        log.emit("failed", error=f"{type(exc).__name__}: {exc}")
        return EXIT_UPSTREAM_UNAVAILABLE
    except KeyboardInterrupt:
        producer.flush(timeout=10)
        log.emit("interrupted", sent=sent)
        return EXIT_OK

    elapsed = time.perf_counter() - started
    log.emit(
        "done",
        topic=topic.name,
        rows_out=sent,
        delivered=delivered["ok"],
        delivery_failures=delivered["failed"],
        corrupted=corrupted,
        source_defects=source_defects,
        duration_ms=round(elapsed * 1000, 1),
        msgs_per_sec=round(sent / elapsed, 1) if elapsed else None,
        status="FAILED" if delivered["failed"] else "SUCCESS",
    )

    if delivered["failed"]:
        print(f"  {delivered['failed']} messages were not delivered", file=sys.stderr)
        return EXIT_ERROR
    return EXIT_OK


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
