"""Output writers.

Three destinations, matching the three batch ingestion sources in CONTRACTS.md §5:

  seeds/oltp/     CSV loaded into the oltp Postgres database (the source system)
  seeds/files/    CSV and JSON drops — the file-based ingestion source
  seeds/vendor/   JSON payloads the simulated PayFlow vendor API serves
  seeds/rag/      the support-ticket corpus as JSONL

Writers are deliberately dumb. Determinism is the generator's job; this module
only needs to be stable in column order, which it gets from the first row's keys.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path


def write_csv(path: Path, rows: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return 0
    # Union of keys, first-seen order. Defect injection can blank a value but
    # never removes a key, so this stays stable.
    fieldnames: list[str] = []
    for row in rows:
        for k in row:
            if k not in fieldnames:
                fieldnames.append(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def write_json(path: Path, payload) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return len(payload) if isinstance(payload, list) else 1


def write_jsonl(path: Path, rows: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(rows)


def write_vendor_pages(directory: Path, payments: list[dict], page_size: int = 500) -> int:
    """Vendor payloads, paginated.

    The vendor feed is paginated on purpose: it is what the ingestion layer's
    pagination-following logic is demonstrated against in Phase 3. Each page
    carries a next_cursor, and the last page carries null.
    """
    directory.mkdir(parents=True, exist_ok=True)
    pages = 0
    for start in range(0, len(payments), page_size):
        chunk = payments[start : start + page_size]
        page_no = start // page_size
        is_last = start + page_size >= len(payments)
        payload = {
            "object": "list",
            "page": page_no,
            "page_size": page_size,
            "has_more": not is_last,
            "next_cursor": None if is_last else f"cursor_{page_no + 1}",
            "data": chunk,
        }
        write_json(directory / f"payments_page_{page_no:03d}.json", payload)
        pages += 1
    return pages
