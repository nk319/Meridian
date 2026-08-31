"""Structured run logging and process exit codes.

CONTRACTS.md §5 fixes both: logs are JSON lines on stdout carrying `run_id`,
`step`, `entity`, `rows_in`, `rows_out` and `duration_ms`; exit codes are 0
success, 1 unexpected error, 2 data-quality BLOCK, 3 upstream unavailable.

Airflow calls these module entrypoints as commands and reads the exit code, so
the codes are an interface, not a convention. Named constants keep them from
being retyped as bare integers at each `sys.exit`.

Not named `logging.py`: a module inside the package shadowing a stdlib name is
the same mistake as naming the package `platform`, which CONTRACTS.md §5 calls
out specifically.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
import time
import uuid
from contextlib import contextmanager
from typing import Any

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_DQ_BLOCK = 2
EXIT_UPSTREAM_UNAVAILABLE = 3


def new_run_id() -> str:
    """A UUID per pipeline run. Joins Bronze's `_ingest_run_id` and `meta`."""
    return str(uuid.uuid4())


class RunLogger:
    """Emits one JSON object per line to stdout.

    Lines go to stdout rather than through `logging` because these records are
    consumed — by Airflow's log parser and later by the ops dashboard — and a
    handler configured elsewhere in the process must not be able to reformat,
    reorder or swallow them.
    """

    def __init__(self, step: str, run_id: str | None = None, stream=None) -> None:
        self.step = step
        self.run_id = run_id or new_run_id()
        self._stream = stream if stream is not None else sys.stdout

    def emit(self, event: str, **fields: Any) -> None:
        record = {
            "ts": dt.datetime.now(dt.UTC).isoformat(),
            "run_id": self.run_id,
            "step": self.step,
            "event": event,
        }
        # Explicit None values are dropped rather than serialised: a reader
        # scanning for `rows_out` should not have to distinguish "absent"
        # from "present and null".
        record.update({k: v for k, v in fields.items() if v is not None})
        print(json.dumps(record, default=str), file=self._stream, flush=True)

    @contextmanager
    def timed(self, event: str, **fields: Any):
        """Time a stage and report `duration_ms` however it ends.

        The failure path emits too. A stage that logs its duration only on
        success makes the slow-and-then-crashed case the one with no timing.
        """
        started = time.perf_counter()
        extra: dict[str, Any] = {}
        try:
            yield extra
        except Exception as exc:
            self.emit(
                event,
                status="FAILED",
                duration_ms=round((time.perf_counter() - started) * 1000, 1),
                error=f"{type(exc).__name__}: {exc}",
                **fields,
                **extra,
            )
            raise
        self.emit(
            event,
            status="SUCCESS",
            duration_ms=round((time.perf_counter() - started) * 1000, 1),
            **fields,
            **extra,
        )
