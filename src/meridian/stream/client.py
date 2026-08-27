"""Kafka clients and Avro codecs, configured in one place.

Two things live here that are easy to scatter and expensive to get
inconsistently wrong.

**Client configuration.** `enable.auto.commit` is the setting that decides
whether a consumer can lose messages, and it defaults to *true*. A consumer
that auto-commits has told the broker it processed a batch five seconds after
receiving it, whether or not it did — so a crash between the commit and the
write loses everything in flight, silently, and the group resumes past it. Both
consumer groups here disable it and commit explicitly, and the setting is set
here rather than in each consumer so it cannot be omitted from the next one.

**Avro framing.** There is no Schema Registry in this stack, so a message
carries no schema id and a consumer must know which schema to decode with. That
is a real limitation and it is stated rather than hidden: the topic *is* the
schema binding, via `contracts/topics.yml`. Messages are written as Avro single-
object encoding — the two-byte marker `C3 01`, an 8-byte CRC-64-AVRO fingerprint
of the writer's schema, then the datum — so a consumer can at least *detect* a
schema it was not expecting instead of decoding garbage into plausible fields.
"""

from __future__ import annotations

import io
import struct
from functools import cache

from ..settings import settings

# Avro single-object encoding, from the specification: every message starts with
# these two bytes, then an 8-byte little-endian CRC-64-AVRO fingerprint of the
# writer's schema.
MAGIC = b"\xc3\x01"
HEADER_LEN = len(MAGIC) + 8


class SchemaMismatch(ValueError):
    """The message was written with a different schema than we are reading with.

    A distinct type because it is the one deserialisation failure that is not
    the message's fault. A corrupt payload is a bad record to dead-letter; this
    is a deployment where the producer and consumer disagree, and dead-lettering
    the whole topic one message at a time is the wrong response to it.
    """


def bootstrap_servers() -> str:
    return settings().kafka_bootstrap_servers


def base_config() -> dict:
    return {
        "bootstrap.servers": bootstrap_servers(),
        # Fail fast rather than blocking a CLI for two minutes when the broker
        # is down. The default is 300s, which reads as a hang.
        "socket.timeout.ms": 10000,
    }


def producer_config(**overrides) -> dict:
    config = base_config() | {
        # `all` — the leader waits for every in-sync replica before
        # acknowledging. On a single-node broker that is one replica and costs
        # nothing; the reason to set it explicitly is that the alternative
        # (`acks=1`) is a silent data-loss window on any real cluster and
        # nobody revisits a default that was fine in development.
        "acks": "all",
        "enable.idempotence": True,
        # With idempotence on, the client can retry a produce without creating
        # a duplicate, so retries are safe to make generous.
        "retries": 5,
        "linger.ms": 20,
        "compression.type": "snappy",
    }
    return config | overrides


def consumer_config(group_id: str, **overrides) -> dict:
    config = base_config() | {
        "group.id": group_id,
        # The setting this module exists for. See the header.
        "enable.auto.commit": False,
        # `earliest`, so a new group reads the topic from the beginning. That is
        # what makes "two groups have independent offsets" demonstrable: the
        # second group starts at 0 while the first sits at the end.
        "auto.offset.reset": "earliest",
        # Long enough that a slow batch does not trigger a rebalance, short
        # enough that a dead consumer is noticed.
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    }
    return config | overrides


@cache
def admin_client():
    from confluent_kafka.admin import AdminClient

    return AdminClient(base_config())


def describe_topics(timeout: float = 30.0):
    return admin_client().list_topics(timeout=timeout).topics


# ---------------------------------------------------------------------------
# Avro
# ---------------------------------------------------------------------------


@cache
def _fingerprint(canonical: str) -> bytes:
    """CRC-64-AVRO of a schema's parsing canonical form, little-endian.

    Cached on the canonical string rather than the schema dict, which is
    unhashable — and the canonical form is the right key anyway: two schemas
    differing only in docstrings or field order produce the same one and are,
    for compatibility purposes, the same schema.
    """
    from fastavro.schema import fingerprint

    return struct.pack("<Q", int(fingerprint(canonical, "CRC-64-AVRO"), 16))


def _schema_fingerprint(schema: dict) -> bytes:
    from fastavro.schema import to_parsing_canonical_form

    return _fingerprint(to_parsing_canonical_form(schema))


def encode(schema: dict, record: dict) -> bytes:
    """Avro single-object encoding: marker, fingerprint, datum.

    Validation is the writer's job here and is not skipped. fastavro's
    `schemaless_writer` raises on a value outside an enum's symbols or a null in
    a non-nullable field, which means a producer that has drifted from
    `contracts/topics.yml` fails at the producer — where the stack trace names
    the bad field — rather than at the consumer, where it names a byte offset.
    """
    from fastavro import schemaless_writer

    buffer = io.BytesIO()
    buffer.write(MAGIC)
    buffer.write(_schema_fingerprint(schema))
    schemaless_writer(buffer, schema, record)
    return buffer.getvalue()


def decode(schema: dict, payload: bytes) -> dict:
    """Decode, refusing to guess when the header says the schema differs."""
    from fastavro import schemaless_reader

    if len(payload) < HEADER_LEN or payload[:2] != MAGIC:
        raise ValueError(
            "not Avro single-object encoding (missing the C3 01 marker) — "
            "this looks like a raw JSON or plain-text message"
        )

    if payload[2:HEADER_LEN] != _schema_fingerprint(schema):
        raise SchemaMismatch(
            "message was written with a different schema than this consumer "
            "reads with. Without a Schema Registry there is no way to fetch the "
            "writer's schema, so this is refused rather than decoded into "
            "plausible-looking wrong values."
        )

    return schemaless_reader(io.BytesIO(payload[HEADER_LEN:]), schema)
