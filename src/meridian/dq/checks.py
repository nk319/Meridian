"""Check primitives and how a result becomes a row in meta.dq_check_results.

A check is declarative: a name, a target, a severity, and one SQL statement that
returns `(rows_scanned, rows_failed)`. Keeping it to data rather than a function
per check means a suite reads as a table of what is asserted, and adding an
assertion is adding a row rather than writing a runner.

Every check also carries `repro_sql` — a statement that returns the offending
rows. CONTRACTS.md §7 puts that column in the results table for a reason: a
data-quality finding nobody can reproduce is a number nobody acts on, and the
gap between "1.46% of order lines failed" and "here they are" is the whole
difference between a dashboard and a to-do list.

Severity decides consequence, not importance:

    BLOCK       the pipeline stops. Exit code 2 (CONTRACTS.md §5).
    QUARANTINE  the row was diverted; used by the Silver build, not here.
    WARN        recorded and visible, but the run continues.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

SEVERITIES = ("BLOCK", "QUARANTINE", "WARN")
OWNER_TEAM = "data-platform"


@dataclass(frozen=True)
class SqlCheck:
    """One assertion, expressed as SQL.

    `count_sql` must return exactly one row of two integers: how many rows were
    examined, and how many failed. Both are recorded — a check that reports 3
    failures out of 4 rows and one that reports 3 out of 4,000,000 are not the
    same finding, and a bare failure count cannot tell them apart.
    """

    name: str
    target_table: str
    count_sql: str
    repro_sql: str
    message: str
    severity: str = "BLOCK"
    target_column: str | None = None
    # A tolerance, where a non-zero one is a deliberate statement about what is
    # expected rather than a way to silence a check. Every use of it in
    # suites.py says why.
    max_failure_pct: float = 0.0
    source: str = "custom"

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(f"{self.name}: severity must be one of {SEVERITIES}")


@dataclass(frozen=True)
class CheckResult:
    check: SqlCheck
    rows_scanned: int
    rows_failed: int

    @property
    def failure_pct(self) -> float:
        return 100.0 * self.rows_failed / self.rows_scanned if self.rows_scanned else 0.0

    @property
    def status(self) -> str:
        return "FAIL" if self.failure_pct > self.check.max_failure_pct else "PASS"

    @property
    def blocking(self) -> bool:
        return self.status == "FAIL" and self.check.severity == "BLOCK"

    def describe(self) -> str:
        return (
            f"{self.status:4} {self.check.severity:10} {self.check.name:38} "
            f"{self.rows_failed:>7}/{self.rows_scanned:<9} ({self.failure_pct:6.3f}%)"
        )


def run_check(conn, check: SqlCheck) -> CheckResult:
    with conn.cursor() as cur:
        cur.execute(check.count_sql)
        row = cur.fetchone()
    if row is None or len(row) != 2:
        raise ValueError(
            f"{check.name}: count_sql must return exactly one row of "
            f"(rows_scanned, rows_failed), got {row!r}"
        )
    scanned, failed = int(row[0] or 0), int(row[1] or 0)
    if failed > scanned:
        raise ValueError(
            f"{check.name}: reported {failed} failures out of {scanned} rows "
            f"scanned, which cannot be right — the two halves of count_sql are "
            f"counting different things."
        )
    return CheckResult(check=check, rows_scanned=scanned, rows_failed=failed)


def persist(conn, run_id: uuid.UUID, suite: str, results: list[CheckResult]) -> None:
    """Append every result — passes included.

    Recording only failures would make the table unable to answer "was this ever
    checked?", and "no failing rows" and "the check stopped running" would look
    identical in the dashboard.
    """
    rows = [
        (
            uuid.uuid4(),
            run_id,
            r.check.source,
            suite,
            r.check.name,
            r.check.target_table,
            r.check.target_column,
            r.check.severity,
            r.status,
            r.rows_scanned,
            r.rows_failed,
            round(r.failure_pct, 4),
            OWNER_TEAM,
            r.check.repro_sql,
            f"{r.check.message} — {r.rows_failed}/{r.rows_scanned} "
            f"({r.failure_pct:.3f}%), tolerance {r.check.max_failure_pct}%",
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
    conn.commit()


# ---------------------------------------------------------------------------
# builders for the shapes that recur
# ---------------------------------------------------------------------------


def referential(
    child: str,
    child_column: str,
    parent: str,
    parent_column: str,
    *,
    severity: str = "BLOCK",
    max_failure_pct: float = 0.0,
    message: str | None = None,
) -> SqlCheck:
    """Every non-null child key resolves to a parent row.

    Silver deliberately has no foreign keys — a quarantined customer would
    otherwise make its orders unloadable, coupling every entity's load to every
    other entity's quarantine decisions (CONTRACTS.md §12). This is where that
    integrity is actually asserted, and being a check rather than a constraint
    means a violation is reported with a count instead of aborting a load
    halfway through.
    """
    where_not_null = f"WHERE c.{child_column} IS NOT NULL"
    return SqlCheck(
        name=f"fk_{child.split('.')[-1]}_{child_column}",
        target_table=child,
        target_column=child_column,
        severity=severity,
        max_failure_pct=max_failure_pct,
        message=message or f"{child}.{child_column} must resolve to {parent}.{parent_column}",
        count_sql=f"""
            SELECT count(*), count(*) FILTER (WHERE p.{parent_column} IS NULL)
            FROM {child} c
            LEFT JOIN {parent} p ON p.{parent_column} = c.{child_column}
            {where_not_null}
        """,
        repro_sql=f"""
            SELECT c.* FROM {child} c
            LEFT JOIN {parent} p ON p.{parent_column} = c.{child_column}
            {where_not_null} AND p.{parent_column} IS NULL LIMIT 100
        """,
    )


def invariant(
    name: str,
    table: str,
    holds_when: str,
    message: str,
    *,
    severity: str = "BLOCK",
    scope: str = "TRUE",
    max_failure_pct: float = 0.0,
) -> SqlCheck:
    """A predicate that must hold for every row in scope.

    `holds_when` is written as the condition that should be TRUE, not as the
    failure. Stating the rule rather than its negation is what makes a wall of
    these readable, and it removes a whole class of double-negative mistakes.
    """
    return SqlCheck(
        name=name,
        target_table=table,
        severity=severity,
        max_failure_pct=max_failure_pct,
        message=message,
        count_sql=f"""
            SELECT count(*), count(*) FILTER (WHERE NOT ({holds_when}))
            FROM {table} WHERE {scope}
        """,
        repro_sql=f"SELECT * FROM {table} WHERE {scope} AND NOT ({holds_when}) LIMIT 100",
    )


def not_empty(table: str, *, minimum: int = 1) -> SqlCheck:
    """The table has rows.

    Blunt, and the most valuable check in the file. Every other assertion here
    passes trivially against an empty table: zero orphans, zero invariant
    violations, zero of everything. A suite without this one reports a clean
    bill of health for a warehouse that failed to load.
    """
    return SqlCheck(
        name=f"not_empty_{table.split('.')[-1]}",
        target_table=table,
        severity="BLOCK",
        message=f"{table} must hold at least {minimum} rows",
        count_sql=f"SELECT {minimum}, CASE WHEN count(*) >= {minimum} THEN 0 ELSE {minimum} END FROM {table}",
        repro_sql=f"SELECT count(*) FROM {table}",
    )


def unique_key(table: str, columns: tuple[str, ...], *, severity: str = "BLOCK") -> SqlCheck:
    key = ", ".join(columns)
    return SqlCheck(
        name=f"unique_{table.split('.')[-1]}_{'_'.join(columns)}",
        target_table=table,
        target_column=key,
        severity=severity,
        message=f"({key}) must be unique in {table}",
        count_sql=f"""
            SELECT (SELECT count(*) FROM {table}),
                   coalesce((SELECT sum(n) - count(*) FROM
                       (SELECT count(*) AS n FROM {table} GROUP BY {key} HAVING count(*) > 1) d
                   ), 0)
        """,
        repro_sql=f"SELECT {key}, count(*) FROM {table} GROUP BY {key} HAVING count(*) > 1 LIMIT 100",
    )
