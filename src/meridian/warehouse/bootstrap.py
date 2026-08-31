"""Create the warehouse tables the pipeline owns: meta, silver, secure.

    python -m meridian.warehouse.bootstrap

Runs as `meridian_etl`, which owns those three schemas. Idempotent, so it is
safe on every pipeline start rather than being a step someone has to remember;
the alternative is a migration ritual that works until the first person clones
the repository and skips it.

dbt owns `gold_stg`, `gold_int` and `gold` and creates everything in them, so
nothing here touches those.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from ..db import UpstreamUnavailable, connect
from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger

DDL_PATH = Path(__file__).parent / "ddl.sql"


def apply(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text(encoding="utf-8"))
    conn.commit()


def summarise(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT schemaname, count(*) FROM pg_tables "
            "WHERE schemaname IN ('meta','silver','secure') GROUP BY schemaname"
        )
        return dict(cur.fetchall())


def main(argv: list[str] | None = None) -> int:
    log = RunLogger("warehouse.bootstrap")
    log.emit("start", ddl=str(DDL_PATH))
    with connect("meridian_etl", vectors=False) as conn:
        with log.timed("apply_ddl"):
            apply(conn)
        counts = summarise(conn)
    log.emit("done", **{f"{schema}_tables": n for schema, n in counts.items()})
    return EXIT_OK


def cli() -> int:
    try:
        return main()
    except UpstreamUnavailable as exc:
        print(json.dumps({"event": "upstream_unavailable", "error": str(exc)}))
        return EXIT_UPSTREAM_UNAVAILABLE
    except Exception as exc:  # noqa: BLE001 - top-level boundary
        print(json.dumps({"event": "error", "error": f"{type(exc).__name__}: {exc}"}))
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(cli())
