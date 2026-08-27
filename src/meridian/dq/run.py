"""Run a data quality suite against the warehouse.

    python -m meridian.dq.run --suite all
    python -m meridian.dq.run --suite referential --suite business
    python -m meridian.dq.run --suite schema          # the Pandera frames

CONTRACTS.md §5 makes this a module entrypoint with the frozen exit codes, so
Airflow can call it as a command and branch on the result:

    0   every check passed, or only WARN-severity checks failed
    2   a BLOCK-severity check failed — the pipeline stops here
    3   the warehouse is unreachable

Results go to `meta.dq_check_results` whatever the outcome, passes included. A
table that only records failures cannot answer "was this ever checked?", and
"nothing failed" then looks identical to "the suite stopped running".
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import uuid

from ..db import UpstreamUnavailable, connect
from ..runlog import (
    EXIT_DQ_BLOCK,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_UPSTREAM_UNAVAILABLE,
    RunLogger,
)
from . import suites
from .checks import CheckResult, SqlCheck, persist, run_check


def run_sql_suite(conn, checks: list[SqlCheck]) -> list[CheckResult]:
    return [run_check(conn, check) for check in checks]


# ---------------------------------------------------------------------------
# the Pandera half
# ---------------------------------------------------------------------------


def run_schema_suite(conn) -> list[CheckResult]:
    """Validate each frame against its Pandera schema.

    Imported here rather than at module scope: pandera pulls in pandas, and the
    SQL suites have no use for either. Someone running `--suite referential` on
    a machine without them should not be told to install a dataframe library.
    """
    import pandas as pd
    import pandera.errors

    from .schemas import SPECS

    results: list[CheckResult] = []
    for spec in SPECS:
        with conn.cursor() as cur:
            cur.execute(spec.sql)
            columns = [d.name for d in cur.description]
            frame = pd.DataFrame(cur.fetchall(), columns=columns)

        scanned = len(frame)
        try:
            # lazy=True collects every failure instead of raising on the first,
            # which is the difference between "products failed" and knowing that
            # margin drifted AND the active share moved.
            spec.schema.validate(frame, lazy=True)
            failures: dict[tuple[str | None, str], int] = {}
        except pandera.errors.SchemaErrors as exc:
            cases = exc.failure_cases
            failures = {
                (row.column, row.check): int(count) for (row, count) in _group_failures(cases)
            }

        total_failed = sum(failures.values())
        results.append(
            CheckResult(
                check=SqlCheck(
                    name=f"pandera_schema_{spec.entity}",
                    target_table=spec.table,
                    severity="BLOCK",
                    source="pandera",
                    message=f"{spec.table} must satisfy its analytical contract",
                    count_sql="",
                    repro_sql=f"-- pandera schema {spec.schema.name}; see the detail rows",
                ),
                rows_scanned=scanned,
                rows_failed=min(total_failed, scanned),
            )
        )
        for (column, check_name), count in failures.items():
            results.append(
                CheckResult(
                    check=SqlCheck(
                        name=f"pandera_{spec.entity}_{check_name}",
                        target_table=spec.table,
                        target_column=column,
                        severity="BLOCK",
                        source="pandera",
                        message=f"pandera check {check_name!r} failed on {spec.table}",
                        count_sql="",
                        repro_sql=f"SELECT * FROM {spec.table} LIMIT 100",
                    ),
                    rows_scanned=scanned,
                    rows_failed=min(count, scanned),
                )
            )
    return results


def _group_failures(cases):
    """(column, check) -> number of failing cases.

    A dataframe-level check reports one failure case for the whole frame, while
    a column check reports one per offending row. Both are counted the same way
    on purpose: the number recorded is "how many things this check objected to",
    and the severity of a distribution check does not depend on row count.
    """
    grouped = cases.groupby(["column", "check"], dropna=False).size()
    out = []
    for (column, check), count in grouped.items():
        out.append((_Row(column if isinstance(column, str) else None, str(check)), count))
    return out


class _Row:
    __slots__ = ("column", "check")

    def __init__(self, column: str | None, check: str) -> None:
        self.column = column
        self.check = check


# ---------------------------------------------------------------------------
# entrypoint
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="meridian.dq.run",
        description="Run a data quality suite against the warehouse",
    )
    p.add_argument(
        "--suite",
        action="append",
        choices=list(suites.SUITE_NAMES),
        default=None,
        help="repeatable; defaults to `all`",
    )
    p.add_argument(
        "--max-age-hours",
        type=int,
        default=24,
        help="freshness tolerance for the last successful run of each pipeline step",
    )
    p.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="treat a failing WARN check as blocking; off by default so a known, "
        "tolerated condition does not stop the pipeline",
    )
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    chosen = args.suite or ["all"]
    registry = suites.build(args.max_age_hours)

    log = RunLogger("dq.run")
    run_id = uuid.UUID(log.run_id)
    started = dt.datetime.now(dt.UTC)
    log.emit("start", suites=chosen, max_age_hours=args.max_age_hours)

    results: list[CheckResult] = []
    with connect("meridian_etl", vectors=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO meta.pipeline_run_log (run_id, step, started_at, status) "
                "VALUES (%s, 'dq.run', %s, 'RUNNING')",
                (run_id, started),
            )
        conn.commit()

        try:
            for name in chosen:
                with log.timed("suite", suite=name) as extra:
                    if name == "schema":
                        found = run_schema_suite(conn)
                    elif name == "all":
                        found = run_sql_suite(conn, registry["all"]) + run_schema_suite(conn)
                    else:
                        found = run_sql_suite(conn, registry[name])
                    persist(conn, run_id, name, found)
                    results.extend(found)
                    extra["checks"] = len(found)
                    extra["failed"] = sum(1 for r in found if r.status == "FAIL")
        except Exception:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE meta.pipeline_run_log SET completed_at = now(), "
                    "status = 'FAILED' WHERE run_id = %s",
                    (run_id,),
                )
            conn.commit()
            raise

        failed = [r for r in results if r.status == "FAIL"]
        blocking = [r for r in failed if r.blocking or (args.fail_on_warn and r.status == "FAIL")]
        status = "FAILED" if blocking else "SUCCESS"
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE meta.pipeline_run_log SET completed_at = now(), status = %s, "
                "rows_in = %s, rows_out = %s WHERE run_id = %s",
                (status, len(results), len(failed), run_id),
            )
        conn.commit()

    if args.json:
        print(
            json.dumps(
                {
                    "run_id": str(run_id),
                    "checks": len(results),
                    "failed": len(failed),
                    "blocking": len(blocking),
                    "results": [
                        {
                            "check": r.check.name,
                            "target": r.check.target_table,
                            "severity": r.check.severity,
                            "status": r.status,
                            "rows_scanned": r.rows_scanned,
                            "rows_failed": r.rows_failed,
                            "failure_pct": round(r.failure_pct, 4),
                            "source": r.check.source,
                        }
                        for r in results
                    ],
                },
                indent=2,
            )
        )
    else:
        print(f"\ndata quality — {len(results)} checks, {len(failed)} failing\n")
        for r in sorted(results, key=lambda r: (r.status == "PASS", r.check.name)):
            print("  " + r.describe())
        print()
        if blocking:
            for r in blocking:
                print(f"  BLOCKING: {r.check.name} — {r.check.message}", file=sys.stderr)

    log.emit(
        "done",
        checks=len(results),
        failed=len(failed),
        blocking=len(blocking),
        status=status,
    )
    return EXIT_DQ_BLOCK if blocking else EXIT_OK


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
