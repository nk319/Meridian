"""The object-storage layout, and the Bronze metadata columns.

Both are frozen in CONTRACTS.md §2. They live in one module because every writer
and every reader has to agree on them exactly: a producer that writes
`ingest_dt=` while a consumer globs `ingest_date=` produces an empty result set,
not an error, and an empty result set from object storage looks identical to
"there was no new data".
"""

from __future__ import annotations

import datetime as dt

# CONTRACTS.md §2. `stream` is written by the Kafka sink in Phase 5.
SOURCE_SYSTEMS = ("oltp", "files", "restapi", "vendor", "stream")

# The six columns every Bronze file carries in addition to its source fields.
# The set is frozen; adding one is a contract change. Order is the physical
# column order, so a reader can rely on it.
BRONZE_METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("_ingested_at", "TIMESTAMPTZ"),
    ("_ingest_run_id", "VARCHAR"),
    ("_source_system", "VARCHAR"),
    ("_source_file", "VARCHAR"),
    ("_record_hash", "VARCHAR"),
    ("_batch_seq", "BIGINT"),
)

BRONZE_METADATA_NAMES = tuple(name for name, _ in BRONZE_METADATA_COLUMNS)


def _check_source(source: str) -> str:
    if source not in SOURCE_SYSTEMS:
        raise ValueError(
            f"unknown source system {source!r}; CONTRACTS.md §2 freezes {SOURCE_SYSTEMS}"
        )
    return source


def bronze_prefix(bucket: str, source: str, entity: str) -> str:
    return f"s3://{bucket}/bronze/{_check_source(source)}/{entity}"


def bronze_partition(bucket: str, source: str, entity: str, ingest_date: dt.date) -> str:
    return f"{bronze_prefix(bucket, source, entity)}/ingest_date={ingest_date.isoformat()}"


def bronze_file(
    bucket: str, source: str, entity: str, ingest_date: dt.date, run_id: str, seq: int
) -> str:
    """One Bronze part file.

    `part-{run_id}-{seq}` rather than a timestamp or a random name: the run id
    ties every file back to a row in `meta.pipeline_run_log`, and the sequence
    orders the files a single run produced. A re-run gets a new run id and
    therefore never overwrites an earlier file, which is what makes Bronze
    append-only a property of the naming scheme rather than a rule someone has
    to remember.
    """
    partition = bronze_partition(bucket, source, entity, ingest_date)
    return f"{partition}/part-{run_id}-{seq:04d}.parquet"


def bronze_glob(bucket: str, source: str, entity: str) -> str:
    return f"{bronze_prefix(bucket, source, entity)}/**/*.parquet"


def silver_prefix(bucket: str, entity: str) -> str:
    return f"s3://{bucket}/silver/{entity}"


def silver_file(bucket: str, entity: str, run_id: str) -> str:
    return f"{silver_prefix(bucket, entity)}/part-{run_id}.parquet"


def silver_current(bucket: str, entity: str) -> str:
    """The single Silver part file for an entity.

    Silver is rebuilt in full from Bronze on every run, so it is one file that
    gets replaced rather than an accumulating set. Naming it by run id instead
    would leave every previous rebuild sitting under the same glob, and
    `silver_glob` would then read every generation of the entity at once — a
    duplicate explosion that grows by one full copy per run and looks like a
    dedup bug rather than a naming one.

    A production system at real volume would write to a new prefix and swap a
    pointer, so readers never see a partial file. At this size the rewrite is
    sub-second and the simpler thing is the right thing.
    """
    return f"{silver_prefix(bucket, entity)}/part-000.parquet"


def silver_glob(bucket: str, entity: str) -> str:
    return f"{silver_prefix(bucket, entity)}/part-*.parquet"


def quarantine_file(bucket: str, entity: str, ingest_date: dt.date, run_id: str) -> str:
    return (
        f"s3://{bucket}/quarantine/{entity}/ingest_date={ingest_date.isoformat()}"
        f"/part-{run_id}.parquet"
    )


def quarantine_glob(bucket: str, entity: str) -> str:
    return f"s3://{bucket}/quarantine/{entity}/**/*.parquet"
