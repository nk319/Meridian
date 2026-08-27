"""The `bronze-sink` consumer group: stream to Bronze Parquet, committing after.

    python -m meridian.stream.sink_bronze --idle-timeout 10

Writes into the same Bronze layout the batch ingestors use, under
`source='stream'` — which `lake/layout.py` has always listed among the five
source systems. That is what makes the streaming path a *source*, not a parallel
architecture: the same Silver builder reads it, the same `_record_hash` dedups
it against the batch capture of the same events, and the same DQ suite checks
the result.

**The commit order is the entire point of this consumer.** Kafka's default is to
commit offsets on a timer, which means a consumer can acknowledge messages it
has not written. Here the sequence is: decode a batch, write one Parquet part,
*then* commit. A crash before the write re-delivers the batch — at-least-once,
which the record hash makes idempotent downstream. A crash after the write and
before the commit does the same, harmlessly. There is no ordering of those two
steps that loses data, and reversing them creates a window that does.
"""

from __future__ import annotations

import argparse
import datetime as dt

from ..lake import bronze
from ..lake.duck import connect as duck_connect
from ..runlog import EXIT_ERROR, RunLogger
from ..settings import settings
from . import topics
from .consume import StreamConsumer


def business_columns(topic_name: str) -> tuple[str, ...]:
    """The business columns, in the order Bronze stores them.

    Derived from the Avro schema's field order rather than written out again.
    One more hand-maintained column list is one more thing to drift from
    `contracts/topics.yml` — and a Bronze part whose column order disagrees with
    the batch capture of the same entity makes the two unreadable by one Silver
    builder, which is the property this whole arrangement is for.
    """
    schema = topics.get(topic_name).schema
    return tuple(field["name"] for field in schema["fields"])


class BronzeSink(StreamConsumer):
    group_id = topics.BRONZE_SINK
    topic_name = topics.WEB_EVENTS

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.cfg = settings()
        self.con = duck_connect()
        self.columns = business_columns(self.topic_name)
        self.seq = 0
        self.parts: list[str] = []

    def handle_batch(self, records: list[dict], raw) -> int:
        import pyarrow as pa

        # Avro gives back an enum as a plain string and a timestamp-millis as a
        # datetime, which is what Bronze wants — except that the batch path
        # captures web_events as *text* on purpose, because the CSV drop carries
        # defects that must survive to Silver rather than being coerced away at
        # capture. Matching that here keeps one Silver builder able to read both.
        table = pa.table(
            {
                name: pa.array(
                    [_stringify(record.get(name)) for record in records],
                    type=pa.string(),
                )
                for name in self.columns
            }
        )

        self.con.register("_stream_batch", table)
        try:
            written = bronze.write(
                self.con,
                source="stream",
                entity="web_events",
                select_sql="SELECT * FROM _stream_batch",
                business_columns=self.columns,
                run_id=self.log.run_id,
                # The Kafka coordinate of the batch's last message, which is
                # what a physical file name means for a stream. `_source_file`
                # is provenance, and "which offset did this come from" is the
                # only provenance a streamed record has.
                source_file=(
                    f"kafka://{self.topic.name}/p{raw[-1].partition()}@{raw[-1].offset()}"
                ),
                seq=self.seq,
                batch_offset=self.seq * self.batch_size,
                order_by="event_id",
                cfg=self.cfg,
            )
        finally:
            self.con.unregister("_stream_batch")

        self.seq += 1
        self.parts.append(written.path)
        self.log.emit(
            "part_written",
            path=written.path,
            rows=written.rows,
            partition=raw[-1].partition(),
            offset=raw[-1].offset(),
        )
        return written.rows

    def on_shutdown(self) -> None:
        self.con.close()


def _stringify(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.isoformat()
    return str(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=0.0,
        help=(
            "Stop after this many seconds with no messages. 0 runs forever, "
            "which is right for a service and wrong for `make demo`."
        ),
    )
    parser.add_argument("--max-messages", type=int, default=None)
    args = parser.parse_args(argv)

    log = RunLogger("stream.sink_bronze")
    try:
        sink = BronzeSink(batch_size=args.batch_size)
        return sink.run(
            max_messages=args.max_messages,
            idle_timeout=args.idle_timeout or None,
        )
    except Exception as exc:  # noqa: BLE001
        log.emit("failed", error=f"{type(exc).__name__}: {exc}")
        return EXIT_ERROR


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
