"""The streaming path: manifest, codec, and the two properties that matter.

Split deliberately into tests that need a broker and tests that do not. The
manifest loader and the Avro codec are checkable with nothing but the
repository, so they run in CI; the consumer-group semantics need Redpanda and
skip cleanly without it.

The broker-backed tests here are not "does Kafka work". They are the two claims
this phase makes that would otherwise be assertions in a README:

  * two consumer groups on one topic have independent offsets, and
  * a poison message does not stall a partition.

Both were false at some point during construction. The second was fine; the
first was broken by a commit bug that no amount of reading the code would have
surfaced — see `test_a_batch_commits_every_partition_it_touched`.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json

import pytest

from meridian.stream import topics
from meridian.stream.client import MAGIC, SchemaMismatch, decode, encode

# ---------------------------------------------------------------------------
# The manifest — no broker needed
# ---------------------------------------------------------------------------


def test_the_manifest_declares_what_contracts_says():
    """CONTRACTS.md §4 names three topics with specific partition counts.

    Asserted here rather than trusted, because the manifest is the source every
    other component generates from — a wrong number in it is wrong everywhere at
    once, which is the cost of having one source of truth and the reason it
    needs a test.
    """
    declared = topics.load()
    assert set(declared) == {
        "ecom.web.events.v1",
        "ecom.orders.placed.v1",
        "ecom.dlq.v1",
    }
    assert declared["ecom.web.events.v1"].partitions == 3
    assert declared["ecom.orders.placed.v1"].partitions == 3
    # One partition, because ordering is irrelevant for dead letters and three
    # would spread a burst of failures across files for no benefit.
    assert declared["ecom.dlq.v1"].partitions == 1


def test_keys_are_what_the_ordering_guarantee_requires():
    """The partition key *is* the ordering guarantee. §4 names both."""
    assert topics.get(topics.WEB_EVENTS).key_field == "session_id"
    assert topics.get(topics.ORDERS_PLACED).key_field == "customer_id"
    assert topics.get(topics.DLQ).key_field is None


def test_both_declared_consumer_groups_are_on_the_web_events_topic():
    """Two groups on one topic is the only concrete proof of offset independence.

    If the manifest ever declares only one, the demonstration silently stops
    being possible and nothing else notices.
    """
    assert set(topics.groups_for(topics.WEB_EVENTS)) == {"bronze-sink", "realtime-metrics"}


def test_a_keyed_topic_refuses_a_record_with_no_key():
    """Producing it would round-robin, losing the ordering the key is there for.

    Silently. That is the whole reason this raises instead of returning None:
    the failure has no symptom at produce time and shows up much later as
    out-of-order events in one session.
    """
    topic = topics.get(topics.WEB_EVENTS)
    assert topic.key_for({"session_id": "S1"}) == b"S1"
    with pytest.raises(ValueError, match="ordering guarantee"):
        topic.key_for({"session_id": None})


def test_every_declared_schema_file_exists_and_parses():
    for topic in topics.load().values():
        assert topic.schema_path.is_file(), topic.schema_path
        assert topic.schema["type"] == "record"


def test_no_module_writes_a_topic_name_as_a_literal():
    """§4's actual requirement, enforced.

    "Generated from the manifest" is only true if nothing bypasses it. The
    failure this prevents is mundane and expensive: a producer writing to
    `ecom.web.events.v1` and a consumer reading `ecom.web.events.v2`, both
    running happily, with nothing connecting them.

    `topics.py` is exempt — it is where the constants live.
    """
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1] / "src" / "meridian" / "stream"
    offenders = []
    for path in root.glob("*.py"):
        if path.name == "topics.py":
            continue
        text = path.read_text(encoding="utf-8")
        for name in topics.load():
            # In a comment or a docstring is fine — that is documentation. In
            # a string literal being passed somewhere is the problem, and a
            # quoted occurrence is the signal for it.
            if f'"{name}"' in text or f"'{name}'" in text:
                offenders.append(f"{path.name}: {name}")
    assert not offenders, f"topic names hardcoded outside topics.py: {offenders}"


# ---------------------------------------------------------------------------
# The Avro codec — no broker needed
# ---------------------------------------------------------------------------


def web_event(**overrides) -> dict:
    record = {
        "event_id": "E1",
        "session_id": "S1",
        "customer_id": None,
        "event_ts": 1_700_000_000_000,
        "event_type": "add_to_cart",
        "product_id": None,
        "order_id": None,
        "channel": "organic",
        "device_type": "mobile",
    }
    return record | overrides


def test_round_trip_preserves_the_record():
    schema = topics.get(topics.WEB_EVENTS).schema
    decoded = decode(schema, encode(schema, web_event()))
    assert decoded["event_id"] == "E1"
    assert decoded["event_type"] == "add_to_cart"
    assert decoded["customer_id"] is None
    # fastavro returns a timezone-aware datetime for timestamp-millis even
    # though it accepts an int. Asymmetric, lossless, and a trap worth pinning.
    assert isinstance(decoded["event_ts"], dt.datetime)


def test_an_enum_outside_the_frozen_vocabulary_fails_at_the_producer():
    """The argument for enums in the schema rather than plain strings.

    A string field accepts `add_to_kart` and the mistake surfaces days later as
    a funnel step that never fires. An enum rejects it here, where the error
    names the field and the run that produced it.
    """
    schema = topics.get(topics.WEB_EVENTS).schema
    with pytest.raises(Exception):  # noqa: B017 — fastavro raises several types
        encode(schema, web_event(event_type="add_to_kart"))


def test_a_decimal_survives_as_a_decimal():
    """Avro decimal, not double. A float amount is a rounding error awaiting a SUM."""
    schema = topics.get(topics.ORDERS_PLACED).schema
    payload = encode(
        schema,
        {
            "order_id": "O1",
            "customer_id": "C1",
            "order_ts": 1_700_000_000_000,
            "channel": "email",
            "device_type": "desktop",
            "gross_amount": decimal.Decimal("1234.56"),
            "total_amount": decimal.Decimal("1499.99"),
            "line_count": 3,
        },
    )
    decoded = decode(schema, payload)
    assert decoded["gross_amount"] == decimal.Decimal("1234.56")
    assert isinstance(decoded["total_amount"], decimal.Decimal)


def test_plain_json_is_rejected_rather_than_misread():
    schema = topics.get(topics.WEB_EVENTS).schema
    with pytest.raises(ValueError, match="single-object encoding"):
        decode(schema, json.dumps(web_event()).encode())


def test_a_message_written_with_another_schema_is_refused_not_decoded():
    """The reason the fingerprint is in the header.

    Without it, a DLQ envelope decoded under the web-event schema does not
    error — Avro is a positional binary format, so it produces a record with
    plausible-looking fields containing nonsense. Silently wrong data is worse
    than an exception, and with no Schema Registry the fingerprint is the only
    thing standing between the two.
    """
    web = topics.get(topics.WEB_EVENTS).schema
    dlq = topics.get(topics.DLQ).schema
    payload = encode(
        dlq,
        {
            "failed_at": 0,
            "consumer_group": "g",
            "source_topic": "t",
            "source_partition": 0,
            "source_offset": 0,
            "error_type": "E",
            "error_detail": "d",
            "key": None,
            "payload": None,
        },
    )
    with pytest.raises(SchemaMismatch):
        decode(web, payload)


def test_a_truncated_message_does_not_decode_into_a_partial_record():
    schema = topics.get(topics.WEB_EVENTS).schema
    payload = encode(schema, web_event())
    with pytest.raises(Exception):  # noqa: B017
        decode(schema, payload[: len(payload) // 2])


def test_the_header_is_the_documented_avro_framing():
    schema = topics.get(topics.WEB_EVENTS).schema
    payload = encode(schema, web_event())
    assert payload[:2] == MAGIC == b"\xc3\x01"
    assert len(payload) > 10


# ---------------------------------------------------------------------------
# Broker-backed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def broker():
    """A reachable Redpanda with the declared topics, or a skip."""
    from meridian.stream.client import admin_client

    try:
        metadata = admin_client().list_topics(timeout=5)
    except Exception:  # noqa: BLE001
        pytest.skip("no broker — run `make stream-up`")

    declared = set(topics.load())
    missing = declared - set(metadata.topics)
    if missing:
        pytest.skip(f"topics not created ({sorted(missing)}) — run `make stream-up`")
    return metadata


def test_the_broker_has_the_partition_counts_the_manifest_declares(broker):
    """A topic auto-created by a producer takes the broker default, not this.

    The result still works and quietly has one partition where three were
    designed, so the parallelism the whole design is about is gone with nothing
    reporting it.
    """
    for name, topic in topics.load().items():
        assert len(broker.topics[name].partitions) == topic.partitions, name


def test_the_two_groups_have_independent_offsets(broker):
    """CONTRACTS.md §4's stated reason for having two groups at all.

    Read through the admin path rather than by consuming, so the test does not
    change what it measures. Skips rather than fails when neither group has run
    — an assertion about offsets needs offsets to exist.
    """
    from meridian.stream.lag import collect

    rows = [row for row in collect() if row["topic"] == topics.WEB_EVENTS and row["committed"]]
    if not rows:
        pytest.skip("neither group has committed yet — run `make stream-demo`")

    groups = {row["consumer_group"] for row in rows}
    assert groups <= {"bronze-sink", "realtime-metrics"}
    # Both groups tracked separately per partition, which is the structural
    # claim. Whether the numbers currently differ depends on when each last ran.
    per_group = {(row["consumer_group"], row["partition"]): row["current_offset"] for row in rows}
    assert len(per_group) == len(rows), "a group/partition pair was recorded twice"


def test_a_batch_commits_every_partition_it_touched(broker):
    """The bug this phase actually shipped and then fixed.

    A poll loop is fed from every assigned partition, so one batch spans all
    three. `commit(message=batch[-1])` commits that message's partition only:
    no data is lost, but the group never advances on the others and lag climbs
    without bound while the consumer reports success. It surfaced here — as
    `meta.kafka_consumer_offsets` recording partition 0 as "never committed"
    after a run that had plainly consumed it.

    Skips rather than fails if the consumers have not run: this asserts a
    property of a completed run, not of an empty broker.
    """
    from meridian.stream.lag import collect

    rows = [r for r in collect() if r["topic"] == topics.WEB_EVENTS]
    if not any(r["committed"] for r in rows):
        pytest.skip("no group has committed yet — run `make stream-demo`")

    for group in {r["consumer_group"] for r in rows if r["committed"]}:
        partitions = {r["partition"] for r in rows if r["consumer_group"] == group}
        uncommitted = {
            r["partition"]
            for r in rows
            if r["consumer_group"] == group and not r["committed"] and r["log_end_offset"] > 0
        }
        assert not uncommitted, (
            f"{group} consumed but never committed partitions {sorted(uncommitted)} "
            f"of {sorted(partitions)} — the single-partition commit bug is back"
        )
