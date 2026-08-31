"""The Bronze writer.

Bronze is raw capture. Everything lands as VARCHAR and nothing is validated,
cast or repaired here, and that is the whole point: the seed injects malformed
dates and invalid enums into the third-party feeds, and an ingestion layer that
typed on the way in would reject those rows at the door. A row rejected at
ingestion leaves no trace — it is indistinguishable from a row that was never
sent — whereas a row captured raw can be quarantined in Silver with a record of
why. Bronze answers "what did they actually send us"; Silver answers "what do we
believe".

Every file carries the six frozen metadata columns from CONTRACTS.md §2 and
lands at the frozen path. Bronze is append-only: nothing rewrites it, and the
`part-{run_id}-{seq}` naming is what makes that structural rather than a rule
someone has to follow — a re-run has a different run id and cannot collide with
an earlier file.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass

import duckdb

from ..settings import Settings, settings
from .layout import BRONZE_METADATA_NAMES, bronze_file

_SAFE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")

# Separator and NULL sentinel for the record hash. chr(31) is ASCII Unit
# Separator, a control character that does not occur in any of these feeds.
_HASH_SEP = "chr(31)"
_HASH_NULL = "'\\x1FNULL'"


@dataclass(frozen=True)
class BronzeWrite:
    path: str
    rows: int
    source: str
    entity: str
    run_id: str
    seq: int


def _check_entity(entity: str) -> str:
    # Entity names reach an interpolated COPY target. They come from this
    # repository's own specs rather than from input, but validating them is one
    # line and makes that fact enforced instead of assumed.
    if not _SAFE_NAME.match(entity):
        raise ValueError(f"entity {entity!r} must match {_SAFE_NAME.pattern}")
    return entity


def record_hash_expr(business_columns: tuple[str, ...] | list[str]) -> str:
    """sha256 over the business columns, positionally.

    Each value is coalesced to a sentinel before joining, because `concat_ws`
    drops NULLs: without it ('a', NULL, 'b') and ('a', 'b', NULL) hash
    identically, and two genuinely different records would dedup into one in
    Silver. The separator makes ('ab', 'c') and ('a', 'bc') distinct for the
    same reason.
    """
    if not business_columns:
        raise ValueError("a record hash over no columns would be constant")
    parts = ", ".join(f"coalesce(CAST({col} AS VARCHAR), {_HASH_NULL})" for col in business_columns)
    return f"sha256(concat_ws({_HASH_SEP}, {parts}))"


def write(
    con: duckdb.DuckDBPyConnection,
    *,
    source: str,
    entity: str,
    select_sql: str,
    business_columns: tuple[str, ...] | list[str],
    run_id: str,
    source_file: str,
    ingest_date: dt.date | None = None,
    seq: int = 0,
    batch_offset: int = 0,
    order_by: str | None = None,
    cfg: Settings | None = None,
) -> BronzeWrite:
    """Wrap `select_sql` with the metadata columns and write one Parquet part."""
    cfg = cfg or settings()
    _check_entity(entity)
    ingest_date = ingest_date or dt.datetime.now(dt.UTC).date()
    path = bronze_file(cfg.lake_bucket, source, entity, ingest_date, run_id, seq)

    overlap = set(business_columns) & set(BRONZE_METADATA_NAMES)
    if overlap:
        raise ValueError(
            f"{entity}: source columns {sorted(overlap)} collide with the frozen "
            f"Bronze metadata columns. §2 freezes that set; a source field of the "
            f"same name would silently shadow the lineage value."
        )

    # Deterministic ordering so two runs over identical input produce identical
    # _batch_seq values. Without it row_number() is whatever order the scan
    # happened to produce, and Bronze stops being reproducible.
    ordering = order_by or business_columns[0]

    columns = ", ".join(business_columns)
    sql = f"""
    COPY (
        SELECT
            {columns},
            now()                                   AS _ingested_at,
            CAST($run_id   AS VARCHAR)              AS _ingest_run_id,
            CAST($source   AS VARCHAR)              AS _source_system,
            CAST($src_file AS VARCHAR)              AS _source_file,
            {record_hash_expr(business_columns)}    AS _record_hash,
            CAST(row_number() OVER (ORDER BY {ordering}) AS BIGINT)
                + CAST($offset AS BIGINT)           AS _batch_seq
        FROM ({select_sql}) AS _src
    ) TO '{path}' (FORMAT PARQUET, COMPRESSION zstd)
    """
    con.execute(
        sql,
        {
            "run_id": run_id,
            "source": source,
            "src_file": source_file,
            "offset": batch_offset,
        },
    )

    rows = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
    return BronzeWrite(path=path, rows=rows, source=source, entity=entity, run_id=run_id, seq=seq)
