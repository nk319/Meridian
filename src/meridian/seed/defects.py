"""Deliberate data defects, for the data-quality layer to catch.

Defects are injected **only** into the file-drop and vendor-API sources — never
into the OLTP tables. That constraint is load-bearing: orders are ingested by
several paths, and if corrupted rows entered the OLTP truth the Bronze
reconciliation check would fail permanently and every run would look broken.
The file and vendor feeds are third-party data in the story being told, so
defects there are realistic rather than self-inflicted.

Every injected defect is recorded in a manifest. Phase 6 asserts the DQ suite
caught them — a quality framework that has never been shown to fail on known-bad
data is not evidence of anything.
"""

from __future__ import annotations

import copy
import random

DEFECT_RATE = 0.018  # ~1.8% of rows in affected feeds


def inject(
    rng: random.Random,
    rows: list[dict],
    entity: str,
    enum_columns: dict[str, list[str]] | None = None,
    required_columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
    date_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (corrupted_rows, defect_manifest)."""
    rows = copy.deepcopy(rows)
    manifest: list[dict] = []

    enum_columns = enum_columns or {}
    required_columns = required_columns or []
    numeric_columns = numeric_columns or []
    date_columns = date_columns or []

    kinds: list[str] = []
    if required_columns:
        kinds.append("null_required")
    if enum_columns:
        kinds.append("invalid_enum")
    if numeric_columns:
        kinds.append("negative_amount")
    if date_columns:
        kinds.append("malformed_date")
    if not kinds:
        return rows, manifest

    n_defects = max(1, int(len(rows) * DEFECT_RATE))
    targets = rng.sample(range(len(rows)), min(n_defects, len(rows)))

    for idx in targets:
        kind = rng.choice(kinds)
        row = rows[idx]

        if kind == "null_required":
            col = rng.choice(required_columns)
            original = row.get(col)
            row[col] = ""
            manifest.append(
                {
                    "entity": entity,
                    "row_index": idx,
                    "column": col,
                    "defect": "null_required",
                    "original": original,
                }
            )

        elif kind == "invalid_enum":
            col = rng.choice(list(enum_columns))
            original = row.get(col)
            row[col] = rng.choice(["UNKNOWN", "n/a", "PENDING_REVIEW", ""])
            manifest.append(
                {
                    "entity": entity,
                    "row_index": idx,
                    "column": col,
                    "defect": "invalid_enum",
                    "original": original,
                }
            )

        elif kind == "negative_amount":
            col = rng.choice(numeric_columns)
            original = row.get(col)
            try:
                row[col] = -abs(float(original))
            except (TypeError, ValueError):
                row[col] = -1.0
            manifest.append(
                {
                    "entity": entity,
                    "row_index": idx,
                    "column": col,
                    "defect": "negative_amount",
                    "original": original,
                }
            )

        elif kind == "malformed_date":
            col = rng.choice(date_columns)
            original = row.get(col)
            row[col] = rng.choice(["2026-13-45", "not-a-date", "31/02/2026", ""])
            manifest.append(
                {
                    "entity": entity,
                    "row_index": idx,
                    "column": col,
                    "defect": "malformed_date",
                    "original": original,
                }
            )

    # Duplicates: a separate mechanism, since they are a property of the row set
    # rather than of any single field. This is what the dedup logic in the
    # Bronze→Silver hop has to remove, keyed on _record_hash.
    n_dupes = max(1, int(len(rows) * DEFECT_RATE / 3))
    for _ in range(n_dupes):
        src = rng.randrange(len(rows))
        rows.append(copy.deepcopy(rows[src]))
        manifest.append(
            {
                "entity": entity,
                "row_index": src,
                "column": None,
                "defect": "duplicate_row",
                "original": None,
            }
        )

    return rows, manifest
