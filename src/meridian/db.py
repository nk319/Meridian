"""Postgres connections, opened as a named contract role.

There is no default role. Every caller states which of the five roles from
CONTRACTS.md §1 it is acting as, because that choice is the security boundary:
the indexer writing as `rag_indexer` and the retriever reading as
`analytics_ro` is what makes "the AI layer cannot reach PII" a property of the
cluster rather than of the code that happens to run there.
"""

from __future__ import annotations

import psycopg
from pgvector.psycopg import register_vector

from .settings import settings


class UpstreamUnavailable(RuntimeError):
    """Postgres is not reachable. Maps to exit code 3, not 1.

    The distinction is what lets Airflow retry a step that failed because a
    dependency was down, and page a human for one that failed on its own logic.
    """


def connect(role: str, dbname: str | None = None, *, vectors: bool = True) -> psycopg.Connection:
    cfg = settings()
    try:
        conn = psycopg.connect(cfg.dsn(role, dbname), autocommit=False)
    except psycopg.OperationalError as exc:
        raise UpstreamUnavailable(
            f"cannot reach Postgres at {cfg.pg_host}:{cfg.pg_port} as {role}: {exc}"
        ) from exc

    if vectors:
        # Registers the `vector` type so embeddings round-trip as numpy arrays
        # instead of being stringified. Requires the extension to exist, which
        # is asserted separately and with a better message below.
        assert_pgvector(conn)
        register_vector(conn)
    return conn


def assert_pgvector(conn: psycopg.Connection) -> str:
    """Fail loudly, here, if pgvector is missing.

    CONTRACTS.md §1 requires this be verified rather than assumed. Without it a
    plain `postgres:16` image fails at the first `::vector` cast — deep inside
    the indexer, with an error that names a type and not an image.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(
            "pgvector is not installed in this database. The Postgres image must "
            "be pgvector/pgvector:pg16 — postgres:16-alpine silently lacks it "
            "(CONTRACTS.md §1). Recreate the stack: "
            "`docker compose --profile core down -v && make up`."
        )
    return row[0]


def server_reachable() -> bool:
    """Cheap probe used by tests to skip cleanly when no stack is running."""
    try:
        with connect("analytics_ro", vectors=False):
            return True
    except (UpstreamUnavailable, RuntimeError):
        return False
