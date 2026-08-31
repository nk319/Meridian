"""The consumer loop both groups share, and the dead letter path.

What is shared is the part that is easy to get subtly wrong, and the part where
getting it wrong is invisible until it matters:

**Commit after the work, never before.** `enable.auto.commit` is disabled in
`client.py`, so a subclass decides when an offset is durable. The rule here is
that `commit()` runs only after `handle_batch` has returned — meaning the rows
are in Bronze, or the metrics are in Postgres. A consumer that commits first has
told the broker it processed messages it may still drop, and a crash resumes
past them with nothing anywhere saying so.

**A poison message must not stall a partition.** The naive failure mode is a
consumer that raises on a bad message, restarts, reads the same message from the
uncommitted offset, and raises again — forever, with the partition frozen behind
it and lag climbing. Routing the message to `ecom.dlq.v1` and *continuing* is
what breaks that loop. It is also why the DLQ envelope carries topic, partition
and offset: the coordinate is how a human finds the original.

**Committing every partition in the batch, at the right offset.** Two traps
here, and the first one cost this consumer a real bug.

A `poll()` loop is fed from every partition the consumer is assigned, so one
batch of 500 messages routinely spans all three. `commit(message=batch[-1])`
commits *that message's partition only* — the other two advance not at all. No
data is lost (the uncommitted messages are simply re-delivered) but the group
never moves forward on them, so lag climbs without bound while the consumer
reports success. It showed up as `meta.kafka_consumer_offsets` recording
partition 0 as "never committed" after a clean run that had plainly consumed it.
So the flush below groups the batch by partition and commits the maximum offset
seen in each.

The second is the classic off-by-one: Kafka commits the offset of the *next*
message to read, not the last one read. `commit(message=...)` adds the 1 for
you; a hand-built `TopicPartition` does not, and committing `msg.offset()`
replays the last message of every batch on every restart forever.
"""

from __future__ import annotations

import datetime as dt
import signal
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from ..runlog import RunLogger
from . import topics
from .client import SchemaMismatch, consumer_config, decode, encode, producer_config


@dataclass
class BatchStats:
    consumed: int = 0
    handled: int = 0
    dead_lettered: int = 0
    commits: int = 0
    by_error: dict[str, int] = field(default_factory=dict)
    # The last offset committed per partition. Reported at the end so a run
    # says which partitions it actually advanced — the number that made the
    # single-partition commit bug visible.
    committed_partitions: dict[int, int] = field(default_factory=dict)


class StreamConsumer(ABC):
    """One consumer group, reading one topic.

    Subclasses implement `handle_batch`. They are handed decoded records and
    must not commit — the loop does that, after they return, which is the whole
    point of the arrangement.
    """

    group_id: str
    topic_name: str

    def __init__(self, *, batch_size: int = 500, poll_timeout: float = 1.0) -> None:
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout
        self.topic = topics.get(self.topic_name)
        self.log = RunLogger(f"stream.{self.group_id}")
        self.stats = BatchStats()
        self._running = True

        if self.group_id not in self.topic.consumer_groups:
            # The manifest declares which groups read which topic. A consumer
            # that is not on that list is either a typo or an undeclared
            # dependency, and both are worth failing on rather than silently
            # joining a group nobody knows about.
            raise ValueError(
                f"{self.group_id!r} is not declared as a consumer group of "
                f"{self.topic_name} in contracts/topics.yml "
                f"(declared: {list(self.topic.consumer_groups)})"
            )

    # -- to implement ------------------------------------------------------

    @abstractmethod
    def handle_batch(self, records: list[dict], raw) -> int:
        """Persist a batch. Return how many records were handled.

        Raising from here is a real error and stops the consumer *without*
        committing, so the batch is re-delivered. That is the correct response
        to a database being down and the wrong one to a bad message, which is
        why bad messages never reach this method.
        """

    def on_shutdown(self) -> None:  # noqa: B027 — optional hook, not a contract
        """Flush anything held back. Called once, after the loop exits.

        Deliberately concrete and empty rather than abstract. A consumer that
        holds nothing back — the Bronze sink writes every batch as it goes —
        has nothing to do here, and forcing it to write `pass` would make the
        base class demand a method that says nothing.
        """

    # -- the loop ----------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        def stop(signum, _frame):
            # Set a flag rather than exiting: the loop finishes its batch,
            # commits it, and closes the consumer so the group rebalances
            # immediately instead of waiting out session.timeout.ms.
            self.log.emit("shutdown_requested", signal=signal.Signals(signum).name)
            self._running = False

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, stop)

    def dead_letter(self, producer, msg, error: Exception) -> None:
        envelope = {
            "failed_at": int(time.time() * 1000),
            "consumer_group": self.group_id,
            "source_topic": msg.topic(),
            "source_partition": msg.partition(),
            "source_offset": msg.offset(),
            "error_type": type(error).__name__,
            "error_detail": str(error)[:1000],
            "key": msg.key(),
            # The original bytes, untouched. A DLQ that stores a parsed
            # representation cannot hold the messages that failed *because* they
            # would not parse, which is most of them.
            "payload": msg.value(),
        }
        dlq = topics.get(topics.DLQ)
        producer.produce(dlq.name, key=None, value=encode(dlq.schema, envelope))
        self.stats.dead_lettered += 1
        self.stats.by_error[type(error).__name__] = (
            self.stats.by_error.get(type(error).__name__, 0) + 1
        )

    def run(self, *, max_messages: int | None = None, idle_timeout: float | None = None) -> int:
        """Consume until stopped, or until the topic goes quiet.

        `idle_timeout` exists so this is runnable as a finite job — a test, or a
        `make` target that should terminate. Without it the loop is correct and
        never returns, which is right for a service and useless for a demo.
        """
        from confluent_kafka import Consumer, KafkaError, Producer

        self._install_signal_handlers()

        consumer = Consumer(consumer_config(self.group_id))
        producer = Producer(producer_config())
        consumer.subscribe([self.topic.name], on_assign=self._on_assign, on_revoke=self._on_revoke)

        schema = self.topic.schema
        batch: list[dict] = []
        raw: list = []
        last_message_at = time.monotonic()
        started = time.perf_counter()

        try:
            while self._running:
                if max_messages is not None and self.stats.consumed >= max_messages:
                    break

                msg = consumer.poll(self.poll_timeout)

                if msg is None:
                    # A quiet poll is the natural moment to flush a partial
                    # batch: waiting for `batch_size` messages that may never
                    # arrive would hold the last few rows hostage indefinitely.
                    if batch:
                        self._flush(consumer, batch, raw)
                        batch, raw = [], []
                    if idle_timeout and time.monotonic() - last_message_at > idle_timeout:
                        self.log.emit("idle", seconds=round(idle_timeout, 1))
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.log.emit("consume_error", error=str(msg.error()))
                    continue

                last_message_at = time.monotonic()
                self.stats.consumed += 1

                try:
                    record = decode(schema, msg.value())
                except (SchemaMismatch, ValueError, Exception) as exc:  # noqa: BLE001
                    # Everything that can go wrong decoding one message is the
                    # message's problem, not the consumer's. Route it and keep
                    # the offset moving — this is the branch that stops one bad
                    # record freezing a partition forever.
                    self.dead_letter(producer, msg, exc)
                    raw.append(msg)
                    if len(raw) + len(batch) >= self.batch_size:
                        self._flush(consumer, batch, raw)
                        batch, raw = [], []
                    continue

                batch.append(record)
                raw.append(msg)

                if len(batch) >= self.batch_size:
                    self._flush(consumer, batch, raw)
                    batch, raw = [], []

            if batch or raw:
                self._flush(consumer, batch, raw)

            producer.flush(timeout=15)
            self.on_shutdown()
        finally:
            # Closing leaves the group cleanly, which triggers an immediate
            # rebalance instead of the other members waiting out the session
            # timeout with the partitions unassigned.
            consumer.close()

        elapsed = time.perf_counter() - started
        self.log.emit(
            "done",
            group=self.group_id,
            topic=self.topic.name,
            rows_in=self.stats.consumed,
            rows_out=self.stats.handled,
            dead_lettered=self.stats.dead_lettered,
            commits=self.stats.commits,
            committed_partitions=self.stats.committed_partitions or None,
            errors=self.stats.by_error or None,
            duration_ms=round(elapsed * 1000, 1),
            status="SUCCESS",
        )
        return 0

    def _flush(self, consumer, batch: list[dict], raw: list) -> None:
        from confluent_kafka import TopicPartition

        if batch:
            self.stats.handled += self.handle_batch(batch, raw)
        if not raw:
            return

        # After the work, never before. See the module header.
        #
        # The highest offset seen per partition, plus one — a commit names the
        # next message to read. Every partition in the batch, not just the last
        # message's, which is the bug this replaced.
        highest: dict[int, int] = {}
        for msg in raw:
            partition = msg.partition()
            if msg.offset() > highest.get(partition, -1):
                highest[partition] = msg.offset()

        consumer.commit(
            offsets=[
                TopicPartition(self.topic.name, partition, offset + 1)
                for partition, offset in sorted(highest.items())
            ],
            asynchronous=False,
        )
        self.stats.commits += 1
        self.stats.committed_partitions.update(highest)

    def _on_assign(self, consumer, partitions) -> None:
        self.log.emit(
            "partitions_assigned",
            group=self.group_id,
            partitions=[p.partition for p in partitions],
        )

    def _on_revoke(self, consumer, partitions) -> None:
        self.log.emit(
            "partitions_revoked",
            group=self.group_id,
            partitions=[p.partition for p in partitions],
        )


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
