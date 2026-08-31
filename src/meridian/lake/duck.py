"""A DuckDB connection configured for the lake.

DuckDB is the lake engine (CONTRACTS.md §3): it queries Parquet in place over
`httpfs` with no load step, which is what makes Bronze and Silver reachable at
all — `dbt-postgres` cannot read Parquet from object storage, so without this
the lake and the warehouse would never meet.

Connections are per-process and cheap. The extensions are the slow part, and
they are installed into DuckDB's own home directory, so the cost is paid once
per machine rather than once per run.
"""

from __future__ import annotations

import duckdb

from ..settings import Settings, settings


def connect(cfg: Settings | None = None, *, memory_limit: str = "2GB") -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB wired to MinIO.

    A memory limit is set deliberately. DuckDB will happily use every byte
    available, and this process shares a machine with Postgres, MinIO and later
    Airflow; an unbounded aggregation over web_events is exactly the kind of
    thing that gets the OOM killer to terminate the database instead.
    """
    cfg = cfg or settings()
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")

    # UTC, pinned. The Bronze record hash casts every column to VARCHAR, and a
    # timestamptz renders in the session time zone — so the same row ingested on
    # a machine set to Europe/Berlin would hash differently from one set to UTC,
    # dedup in Silver would stop matching, and the duplicates would look like a
    # source problem. Same class of bug as the hash() randomisation Phase 0
    # found in the generator, and it fails the same way: silently, and only
    # across environments.
    con.execute("SET TimeZone='UTC'")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    if not cfg.minio_access_key or not cfg.minio_secret_key:
        raise RuntimeError(
            "MINIO_ROOT_USER / MINIO_ROOT_PASSWORD are not set, so the lake is "
            "unreachable. Copy .env.example to .env and fill it in."
        )

    # A named secret rather than the deprecated SET s3_* variables. URL_STYLE
    # 'path' is required for MinIO: virtual-host style would resolve
    # `meridian-lake.minio` as a hostname, which does not exist, and the failure
    # reads as a DNS error rather than a configuration one.
    con.execute(
        """
        CREATE OR REPLACE SECRET lake (
            TYPE s3, PROVIDER config,
            KEY_ID $key, SECRET $secret,
            ENDPOINT $endpoint, USE_SSL $use_ssl, URL_STYLE 'path'
        )
        """,
        {
            "key": cfg.minio_access_key,
            "secret": cfg.minio_secret_key,
            "endpoint": cfg.minio_host,
            "use_ssl": cfg.minio_use_ssl,
        },
    )
    return con


def attach_oltp(
    con: duckdb.DuckDBPyConnection, cfg: Settings | None = None, alias: str = "src"
) -> None:
    """Attach the OLTP source database, read-only.

    READ_ONLY is not a formality: this is the live source system, and an
    ingestion job holding a writable handle to it is one typo away from being
    the thing that corrupts the data it exists to copy.
    """
    cfg = cfg or settings()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(
        f"ATTACH '{cfg.dsn('meridian_etl', cfg.oltp_db)}' AS {alias} (TYPE postgres, READ_ONLY)"
    )
