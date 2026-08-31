"""The `realtime-metrics` consumer group: windowed counts, independent offsets.

    python -m meridian.stream.metrics --window 60 --idle-timeout 10

Reads the *same topic* as `bronze-sink`, in its own group. That is the only
concrete proof CONTRACTS.md §4 asks for — two groups on one topic have separate
offsets, so this one can be at the beginning of the log while the sink is at the
end, and neither affects the other. `make stream-lag` shows both side by side.

What it computes is genuinely different work from the sink's, not a copy with a
different name. The sink cares about durability and writes every row; this cares
about latency and writes one aggregate per window. That difference is why the
two want separate offsets in the first place: replaying this group to recompute
a metric must not re-write the lake.

**These numbers will disagree with `gold.mart_web_funnel`, and that is the
honest part.** This is what the stream believed at a point in time, from an
at-least-once feed with no late-arrival handling and no deduplication. The mart
is what the batch path concluded after both. A demo that made them agree would
have hidden the tradeoff the Lambda shape exists to make.

The specific cost of at-least-once here: a crash between writing a window's
delta and committing the offset re-delivers those messages, and this table
counts them twice. The Bronze sink has the identical exposure and Silver's
record hash removes it downstream; a counter has no such recourse. Exactly-once
would need the offset committed in the same Postgres transaction as the metric —
which is a real design (a transactional outbox keyed on the offset) and a much
larger one than a demo of consumer-group semantics warrants. Stating the gap is
better than a comment implying it is not there.
"""

from __future__ import annotations

import argparse
import datetime as dt

from ..db import UpstreamUnavailable, connect
from ..runlog import EXIT_ERROR, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from . import topics
from .consume import StreamConsumer

# Additive on conflict, and fed with *deltas* rather than running totals.
#
# The obvious version — accumulate in memory and upsert the total with
# `value = EXCLUDED.value` — is wrong in a way that only shows up on the second
# run. Kafka offsets guarantee each message reaches this group once, so a second
# run over the same window sees only the messages the first did not: its
# in-memory counter starts at zero, and overwriting with that number discards
# everything already counted. What made it visible was `events_by_type` summing
# to 2,892 while `events` summed to 2,884 over the same messages — the
# difference being (window, dimension) pairs the first run had written and the
# second never touched, so the overwrite left them behind at a stale value while
# replacing the rest.
#
# Adding a delta is correct across batches and across runs alike, for the same
# reason: each message contributes to exactly one delta, exactly once.
UPSERT_SQL = """
INSERT INTO meta.stream_metrics
    (window_start, window_seconds, topic, consumer_group, metric, dimension,
     value, is_closed, observed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (window_start, topic, metric, dimension) DO UPDATE SET
    value       = meta.stream_metrics.value + EXCLUDED.value,
    is_closed   = EXCLUDED.is_closed,
    observed_at = now()
"""


class RealtimeMetrics(StreamConsumer):
    group_id = topics.REALTIME_METRICS
    topic_name = topics.WEB_EVENTS

    def __init__(self, *, window_seconds: int = 60, **kwargs) -> None:
        super().__init__(**kwargs)
        self.window_seconds = window_seconds
        # {window_start: {(metric, dimension): count}} — the running total for
        # this process, and what has already been written of it. The difference
        # is what gets sent, because the table accumulates rather than replaces.
        self.windows: dict[dt.datetime, dict[tuple[str, str], int]] = {}
        self.persisted: dict[dt.datetime, dict[tuple[str, str], int]] = {}
        self.conn = connect("meridian_etl", vectors=False)

    def _window_of(self, moment: dt.datetime) -> dt.datetime:
        """Floor an event timestamp to its window.

        Windowed on **event time**, not arrival time. Arrival-time windows are
        easier and wrong for anything you would then compare against a batch
        result: a replay of yesterday's messages would land every one of them in
        today's window, and the two views would disagree for a reason that has
        nothing to do with the data.
        """
        epoch = int(moment.timestamp())
        floored = epoch - (epoch % self.window_seconds)
        return dt.datetime.fromtimestamp(floored, tz=dt.UTC)

    def handle_batch(self, records: list[dict], raw) -> int:
        for record in records:
            event_ts = record["event_ts"]
            if not isinstance(event_ts, dt.datetime):
                event_ts = dt.datetime.fromtimestamp(event_ts / 1000, tz=dt.UTC)
            window = self._window_of(event_ts)
            counts = self.windows.setdefault(window, {})

            counts[("events", "_total")] = counts.get(("events", "_total"), 0) + 1
            for metric, value in (
                ("events_by_type", record.get("event_type")),
                ("events_by_channel", record.get("channel")),
                ("events_by_device", record.get("device_type")),
            ):
                if value:
                    counts[(metric, str(value))] = counts.get((metric, str(value)), 0) + 1

            if record.get("customer_id"):
                counts[("identified_events", "_total")] = (
                    counts.get(("identified_events", "_total"), 0) + 1
                )

        # Written before the commit, like the sink's Parquet part and for the
        # same reason: an offset that outruns the write is a gap nothing
        # reports. Every window is upserted on every batch, so a window stays
        # correct as more of it arrives rather than being wrong until it closes.
        self._persist(closed=False)
        return len(records)

    def _persist(self, *, closed: bool) -> None:
        """Write what has been counted since the last write, and only that."""
        rows = []
        deltas: dict[dt.datetime, dict[tuple[str, str], int]] = {}

        for window, counts in self.windows.items():
            already = self.persisted.get(window, {})
            for key, total in counts.items():
                delta = total - already.get(key, 0)
                # A zero delta on a window that is being closed still needs a
                # row, so `is_closed` gets set; on an open one it is nothing to
                # say and writing it would be a pointless round trip.
                if delta == 0 and not closed:
                    continue
                metric, dimension = key
                rows.append(
                    (
                        window,
                        self.window_seconds,
                        self.topic.name,
                        self.group_id,
                        metric,
                        dimension,
                        delta,
                        closed,
                    )
                )
                deltas.setdefault(window, {})[key] = total

        if not rows:
            return

        try:
            with self.conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, rows)
            self.conn.commit()
        except Exception:
            # Roll back before anything else touches this connection, or the
            # next statement fails with InFailedSqlTransaction and buries the
            # real error — the same trap the batch loaders hit in Phase 2.
            self.conn.rollback()
            raise

        # Only after the commit. Recording a delta as written before it is
        # durable would drop it on the next pass, which is the same
        # commit-before-the-work mistake the consumer loop exists to avoid.
        for window, written in deltas.items():
            self.persisted.setdefault(window, {}).update(written)

    def on_shutdown(self) -> None:
        # Mark everything closed on a clean exit. A window left open reads as
        # "still filling", which is true while the consumer runs and misleading
        # once it has stopped.
        self._persist(closed=True)
        self.log.emit(
            "windows_written",
            windows=len(self.windows),
            window_seconds=self.window_seconds,
        )
        self.conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window", type=int, default=60, help="Window width in seconds")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--idle-timeout", type=float, default=0.0)
    parser.add_argument("--max-messages", type=int, default=None)
    args = parser.parse_args(argv)

    log = RunLogger("stream.metrics")
    try:
        consumer = RealtimeMetrics(window_seconds=args.window, batch_size=args.batch_size)
        return consumer.run(
            max_messages=args.max_messages,
            idle_timeout=args.idle_timeout or None,
        )
    except UpstreamUnavailable as exc:
        log.emit("failed", error=str(exc))
        return EXIT_UPSTREAM_UNAVAILABLE
    except Exception as exc:  # noqa: BLE001
        log.emit("failed", error=f"{type(exc).__name__}: {exc}")
        return EXIT_ERROR


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
