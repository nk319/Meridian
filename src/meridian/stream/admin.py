"""Create the declared topics, and report what is actually there.

    python -m meridian.stream.admin --create
    python -m meridian.stream.admin --describe

This is the "broker init script" CONTRACTS.md §4 asks for, written in Python
against the same loader the producer and consumers use rather than as a shell
script full of `rpk topic create`. The point is not that Python is nicer: it is
that a shell script is a *second* place the partition counts live, and §4's
whole reason for a manifest is that there should not be a second place.

`--describe` exists because "the topics were created" and "the topics have the
partition count the manifest declares" are different claims. A topic
auto-created by a producer gets the broker's default partition count, and
`ecom.web.events.v1` with one partition instead of three still works — it just
silently loses the parallelism the design is about.
"""

from __future__ import annotations

import argparse
import sys

from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from . import topics
from .client import admin_client, describe_topics


def create(log: RunLogger, timeout: float = 30.0) -> int:
    """Create every declared topic. Existing topics are left alone.

    Idempotent by design — this runs on every `make stream-up`, so "already
    exists" is the expected outcome rather than an error to report.
    """
    from confluent_kafka.admin import NewTopic

    declared = topics.load()
    admin = admin_client()

    existing = set(admin.list_topics(timeout=timeout).topics)
    wanted = [
        NewTopic(
            topic=t.name,
            num_partitions=t.partitions,
            replication_factor=t.replication_factor,
            config={"retention.ms": str(t.retention_ms)},
        )
        for t in declared.values()
        if t.name not in existing
    ]

    if not wanted:
        log.emit("topics_ready", created=0, existing=len(declared))
        return EXIT_OK

    created, failed = 0, 0
    for name, future in admin.create_topics(wanted, request_timeout=timeout).items():
        try:
            future.result()
            created += 1
            log.emit("topic_created", topic=name, partitions=declared[name].partitions)
        except Exception as exc:  # noqa: BLE001 — the client raises several unrelated types
            # A race with another process creating the same topic is fine and
            # common: `make stream-up` and an Airflow task can both run this.
            if "already exists" in str(exc).lower():
                continue
            failed += 1
            log.emit("topic_failed", topic=name, error=f"{type(exc).__name__}: {exc}")

    log.emit("topics_ready", created=created, failed=failed, existing=len(existing & set(declared)))
    return EXIT_ERROR if failed else EXIT_OK


def describe(log: RunLogger, timeout: float = 30.0) -> int:
    """Compare what the broker has against what the manifest declares."""
    declared = topics.load()
    actual = describe_topics(timeout=timeout)

    drift = []
    print(f"{'topic':26} {'partitions':>12} {'declared':>10}  {'groups':<32}")
    print("-" * 84)
    for name, topic in sorted(declared.items()):
        found = actual.get(name)
        partitions = len(found.partitions) if found else 0
        marker = "" if partitions == topic.partitions else "  <-- DRIFT"
        if marker:
            drift.append((name, partitions, topic.partitions))
        print(
            f"{name:26} {partitions:>12} {topic.partitions:>10}  "
            f"{','.join(topic.consumer_groups) or '-':<32}{marker}"
        )

    if drift:
        print(file=sys.stderr)
        for name, found, want in drift:
            print(
                f"  {name}: broker has {found} partitions, manifest declares {want}. "
                f"A topic auto-created by a producer takes the broker default; "
                f"delete it and re-run --create.",
                file=sys.stderr,
            )
    log.emit("described", topics=len(declared), drifted=len(drift))
    return EXIT_ERROR if drift else EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--create", action="store_true", help="Create the declared topics")
    action.add_argument(
        "--describe", action="store_true", help="Show broker state vs. the manifest"
    )
    args = parser.parse_args(argv)

    log = RunLogger("stream.admin")
    try:
        return create(log) if args.create else describe(log)
    except Exception as exc:  # noqa: BLE001
        # A broker that is not up is exit 3, not 1 — CONTRACTS.md §5, so Airflow
        # can retry a step that failed on a dependency and page for one that
        # failed on its own logic.
        if "resolve" in str(exc).lower() or "transport" in str(exc).lower():
            log.emit("failed", error=f"broker unreachable: {exc}")
            return EXIT_UPSTREAM_UNAVAILABLE
        log.emit("failed", error=f"{type(exc).__name__}: {exc}")
        return EXIT_ERROR


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
