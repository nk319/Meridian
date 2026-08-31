"""Parse dbt's `run_results.json` into `meta.dq_check_results`.

    python -m meridian.dbt.results --target-path dbt/target

CONTRACTS.md §7 puts one data quality results table in the warehouse, "fed by
Pandera, custom checks, and parsed dbt `run_results.json` alike". This is the
third of those three.

The reason it matters is not tidiness. `meridian.dq.run` and dbt assert
different things about the same warehouse — dq covers relationships and
distributions in `silver`, dbt covers grain and referential integrity in
`gold` — and the question a human asks at 3am is "what is failing", not "what
is failing in each of two systems". A dashboard that has to union a table with
a JSON file on some worker's disk will not be written, so the failures dbt
found will not be looked at.

Mapping dbt's model onto the table's takes three decisions, all of them
recorded here rather than implied:

  * **What counts as a check.** Only `test` nodes. A `model` node's status says
    a table built, which is a pipeline event and belongs in
    `meta.pipeline_run_log`; recording it here would make "how many checks ran"
    a number that grows when somebody adds a model.

  * **Severity.** dbt's own `severity` config — `error` or `warn` — maps to
    BLOCK and WARN. dbt already ran with that setting and it is the author's
    stated intent for the test; re-deriving it from the test's name would be
    inventing a second opinion.

  * **Status.** dbt's `pass`/`warn`/`fail`/`error`/`skipped` collapses to the
    table's `PASS`/`FAIL`. `error` (the test could not run) and `skipped` (an
    upstream model failed, so it never ran) both become FAIL: a check that did
    not execute has not passed, and recording it as PASS is how a broken test
    stays broken for a quarter.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from ..db import UpstreamUnavailable, connect
from ..dq.checks import OWNER_TEAM
from ..runlog import (
    EXIT_DQ_BLOCK,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_UPSTREAM_UNAVAILABLE,
    RunLogger,
)
from ..settings import project_root

# dbt status -> (our status, whether it should stop the pipeline)
#
# `warn` is a pass that wanted attention: the test found rows, and its author
# said finding rows here is not a reason to stop. Recording it as PASS would
# lose that; recording it as FAIL would stop a pipeline the author chose not to
# stop. It is FAIL at WARN severity, which is exactly what the severity column
# is for.
STATUS_MAP = {
    "success": ("PASS", False),
    "pass": ("PASS", False),
    "warn": ("FAIL", False),
    "fail": ("FAIL", True),
    "error": ("FAIL", True),
    "skipped": ("FAIL", True),
    "runtime error": ("FAIL", True),
}


@dataclass(frozen=True)
class DbtTestResult:
    name: str
    target_table: str
    target_column: str | None
    severity: str
    status: str
    rows_failed: int
    message: str
    repro_sql: str | None

    @property
    def blocking(self) -> bool:
        return self.status == "FAIL" and self.severity == "BLOCK"


def _node_target(node: dict) -> tuple[str, str | None]:
    """Which relation a test is about, and which column of it.

    dbt records this in `depends_on.nodes` and in `test_metadata.kwargs`, and
    neither is reliable alone: a singular test (a .sql file in tests/) has no
    `test_metadata` at all, and a relationship test depends on two models. The
    first dependency is the one the test is *of*; the second, when present, is
    the one it points at.
    """
    depends = node.get("depends_on", {}).get("nodes", [])
    relation = depends[0].split(".")[-1] if depends else "unknown"

    kwargs = (node.get("test_metadata") or {}).get("kwargs") or {}
    column = kwargs.get("column_name")
    # dbt renders the column as a Jinja expression for some test types.
    if isinstance(column, str) and "{{" in column:
        column = None
    return relation, column


def _node_name(unique_id: str, node: dict) -> str:
    """The test's name, as a human would write it in the project.

    The manifest's `name` is authoritative and is used whenever it is there.
    The fallback is for a run with no manifest, and it has to handle two
    different `unique_id` shapes: a generic test is
    `test.meridian.<name>.<hash>` and a singular test is `test.meridian.<name>`
    with no hash at all. Taking a fixed position from the end gets one of them
    right and silently labels every singular test `meridian` — which is what
    the first version of this did, producing a results table where the six most
    interesting assertions in the project all shared one name.
    """
    name = node.get("name")
    if name:
        return str(name)

    parts = unique_id.split(".")
    if len(parts) >= 4 and len(parts[-1]) == 32 and all(c in "0123456789abcdef" for c in parts[-1]):
        return parts[-2]
    return parts[-1] if parts else unique_id


def parse(run_results: dict, manifest: dict | None = None) -> list[DbtTestResult]:
    """Turn one run_results.json into check results.

    `manifest.json` is optional and only improves the output: it carries each
    test's compiled SQL, which becomes `repro_sql` — the column CONTRACTS.md §7
    adds so a failure links to the rows that caused it rather than to a count.
    Without the manifest every other column is still correct.
    """
    nodes = (manifest or {}).get("nodes", {})
    results: list[DbtTestResult] = []

    for entry in run_results.get("results", []):
        unique_id = entry.get("unique_id", "")
        if not unique_id.startswith("test."):
            continue

        node = nodes.get(unique_id, {})
        target_table, target_column = _node_target(node)

        raw_status = str(entry.get("status", "")).lower()
        status, _ = STATUS_MAP.get(raw_status, ("FAIL", True))

        # dbt puts the test's configured severity on the node, not on the
        # result. Absent (no manifest), assume the stricter of the two: a test
        # whose severity we cannot read is not one to quietly downgrade.
        configured = str(node.get("config", {}).get("severity", "error")).lower()
        severity = "WARN" if configured == "warn" else "BLOCK"

        failures = entry.get("failures")
        message = entry.get("message") or f"dbt test {raw_status}"

        results.append(
            DbtTestResult(
                name=_node_name(unique_id, node),
                target_table=target_table,
                target_column=target_column,
                severity=severity,
                status=status,
                rows_failed=int(failures) if failures is not None else 0,
                message=f"{message} ({raw_status})",
                repro_sql=node.get("compiled_code"),
            )
        )

    return results


def persist(conn, run_id: uuid.UUID, results: list[DbtTestResult]) -> None:
    rows = [
        (
            uuid.uuid4(),
            run_id,
            "dbt",
            "dbt",
            r.name,
            r.target_table,
            r.target_column,
            r.severity,
            r.status,
            # dbt reports how many rows failed and never how many it scanned —
            # a test is a query returning offending rows, so there is no
            # denominator to report. Left null rather than filled with the
            # failure count, which would make every failure read as 100%.
            None,
            r.rows_failed,
            None,
            OWNER_TEAM,
            r.repro_sql,
            r.message,
        )
        for r in results
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO meta.dq_check_results (check_run_id, run_id, source, suite, "
            "check_name, target_table, target_column, severity, status, rows_scanned, "
            "rows_failed, failure_pct, owner_team, repro_sql, message) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows,
        )


def _read_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-path",
        default=None,
        help="dbt's target/ directory. Defaults to <repo>/dbt/target.",
    )
    parser.add_argument(
        "--fail-on-block",
        action="store_true",
        help=(
            "Exit 2 if any BLOCK-severity dbt test failed. Off by default "
            "because `dbt test` has already returned non-zero in that case, "
            "and this step's job is to record what happened, not to re-decide it."
        ),
    )
    args = parser.parse_args(argv)

    target = Path(args.target_path) if args.target_path else project_root() / "dbt" / "target"
    log = RunLogger("dbt.results")

    run_results_path = target / "run_results.json"
    if not run_results_path.is_file():
        # Not an error worth a stack trace: the usual cause is that `dbt test`
        # never ran, which the orchestrator already knows about.
        log.emit("skipped", reason="no run_results.json", path=str(run_results_path))
        return EXIT_OK

    try:
        run_results = _read_json(run_results_path)
    except json.JSONDecodeError as exc:
        log.emit("failed", error=f"unparseable run_results.json: {exc}")
        return EXIT_ERROR

    manifest_path = target / "manifest.json"
    manifest = _read_json(manifest_path) if manifest_path.is_file() else None

    results = parse(run_results, manifest)
    blocking = [r for r in results if r.blocking]

    try:
        # As meridian_etl: `meta` is its schema, and dbt_runner has no grant on
        # it. The role that writes the results table is the role that owns it.
        with connect("meridian_etl") as conn:
            run_id = uuid.UUID(log.run_id)
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO meta.pipeline_run_log "
                    "(run_id, step, started_at, completed_at, status, rows_out) "
                    "VALUES (%s, %s, now(), now(), %s, %s)",
                    (
                        run_id,
                        "dbt.results",
                        "FAILED" if blocking else "SUCCESS",
                        len(results),
                    ),
                )
            persist(conn, run_id, results)
            conn.commit()
    except UpstreamUnavailable as exc:
        log.emit("failed", error=str(exc))
        return EXIT_UPSTREAM_UNAVAILABLE

    passed = sum(1 for r in results if r.status == "PASS")
    log.emit(
        "done",
        tests=len(results),
        passed=passed,
        failed=len(results) - passed,
        blocking=len(blocking),
        status="FAILED" if blocking else "SUCCESS",
    )

    for result in blocking:
        print(f"  BLOCKING: {result.name} — {result.message}", file=sys.stderr)

    if blocking and args.fail_on_block:
        return EXIT_DQ_BLOCK
    return EXIT_OK


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
