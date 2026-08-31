"""The topic manifest, loaded once and shared by everything that touches Kafka.

`contracts/topics.yml` declares each topic's name, partition count, key field
and Avro schema. This module turns that into objects. Nothing anywhere else in
the codebase is allowed to write a topic name as a string literal — the tests
enforce it — because the drift CONTRACTS.md §4 describes is not hypothetical:
an init script, a producer, a consumer and a dashboard each holding their own
copy of `ecom.web.events.v1` is four chances to typo a version suffix, and the
symptom is a producer writing happily to a topic nobody reads.

The Avro schemas are parsed here too, for the same reason. A schema file
referenced by the manifest but missing from disk should fail when the manifest
loads, not when the first message is produced twenty minutes into a run.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import yaml

from ..settings import project_root


@dataclass(frozen=True)
class Topic:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    # None for the DLQ: dead letters have no meaningful ordering to preserve, so
    # keying them would concentrate a burst of failures onto one partition for
    # no benefit.
    key_field: str | None
    schema_path: Path
    consumer_groups: tuple[str, ...]

    @property
    def schema(self) -> dict:
        return load_schema(self.schema_path)

    def key_for(self, record: dict) -> bytes | None:
        """The partition key for a record, as bytes.

        Kafka partitions by `hash(key) % partitions`, so this is the function
        that decides ordering guarantees. A null key round-robins, which is
        right for the DLQ and wrong for everything else here.
        """
        if self.key_field is None:
            return None
        value = record.get(self.key_field)
        if value is None:
            # A null key on a topic that declares one is a bug in the producer,
            # not a routing decision. Round-robining it would silently drop the
            # per-session ordering the key exists to provide.
            raise ValueError(
                f"{self.name} is keyed by {self.key_field!r} and this record has "
                f"none. Producing it would lose the ordering guarantee the key "
                f"is there for."
            )
        return str(value).encode("utf-8")


@cache
def manifest_path() -> Path:
    return project_root() / "contracts" / "topics.yml"


@cache
def load_schema(path: Path) -> dict:
    from fastavro import parse_schema

    raw = json.loads(path.read_text(encoding="utf-8"))
    # Parsed rather than returned verbatim: fastavro resolves named types and
    # validates the schema itself, so a malformed .avsc fails here with a
    # message about the schema instead of at encode time with one about a field.
    return parse_schema(raw)


@cache
def load() -> dict[str, Topic]:
    """Every declared topic, keyed by name."""
    doc = yaml.safe_load(manifest_path().read_text(encoding="utf-8"))
    defaults = doc.get("defaults") or {}
    root = manifest_path().parent

    topics: dict[str, Topic] = {}
    for entry in doc["topics"]:
        schema_path = root / entry["value_schema"]
        if not schema_path.is_file():
            raise FileNotFoundError(
                f"{entry['name']} declares {entry['value_schema']}, which does not "
                f"exist. The manifest is the source of truth, so a missing schema "
                f"is a broken contract rather than a missing optional file."
            )
        topics[entry["name"]] = Topic(
            name=entry["name"],
            partitions=int(entry["partitions"]),
            replication_factor=int(
                entry.get("replication_factor", defaults.get("replication_factor", 1))
            ),
            retention_ms=int(entry.get("retention_ms", defaults.get("retention_ms", 604800000))),
            key_field=entry.get("key_field"),
            schema_path=schema_path,
            consumer_groups=tuple(entry.get("consumer_groups") or ()),
        )
    return topics


def get(name: str) -> Topic:
    topics = load()
    if name not in topics:
        raise KeyError(f"{name!r} is not in contracts/topics.yml. Declared: {sorted(topics)}")
    return topics[name]


def groups_for(name: str) -> tuple[str, ...]:
    return get(name).consumer_groups


# Named constants, so a caller writes `topics.WEB_EVENTS` and a typo is an
# AttributeError at import rather than a topic that quietly does not exist.
# Resolved through `get()` so they are still checked against the manifest.
WEB_EVENTS = "ecom.web.events.v1"
ORDERS_PLACED = "ecom.orders.placed.v1"
DLQ = "ecom.dlq.v1"

BRONZE_SINK = "bronze-sink"
REALTIME_METRICS = "realtime-metrics"
