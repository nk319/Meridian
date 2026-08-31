"""Shared fixtures.

The split that matters here is between tests that need the stack running and
tests that do not. The PII guarantee, the masking rules and the chunking logic
are all checkable with nothing but the repository, so they run everywhere and
run in CI. Only the tests that assert something about the vector store itself
need Postgres, and those skip with a message that says how to get them running
rather than failing on a laptop that has not started Docker.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
# `src` for the package, and the root itself for `dashboard`, which is a
# top-level directory rather than part of the installed distribution — it is an
# application that consumes `meridian`, not a module of it.
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from meridian.rag.masking import Masker  # noqa: E402
from meridian.settings import settings  # noqa: E402

SEEDS = ROOT / "seeds"
CORPUS = SEEDS / "rag" / "support_tickets.jsonl"


def pytest_configure(config):
    config.addinivalue_line("markers", "db: requires the `core` compose profile running")


@pytest.fixture(scope="session")
def seeds_present() -> bool:
    return CORPUS.is_file() and (SEEDS / "known_pii_terms.json").is_file()


@pytest.fixture(scope="session")
def masker(seeds_present) -> Masker:
    if not seeds_present:
        pytest.skip("seeds not generated — run `make seed`")
    return Masker.from_project()


@pytest.fixture(scope="session")
def corpus(seeds_present) -> list[dict]:
    if not seeds_present:
        pytest.skip("seeds not generated — run `make seed`")
    with CORPUS.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


@pytest.fixture(scope="session")
def pii_manifest(seeds_present) -> dict:
    if not seeds_present:
        pytest.skip("seeds not generated — run `make seed`")
    return json.loads((SEEDS / "known_pii_terms.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def db():
    """A connection as analytics_ro, or a skip.

    analytics_ro deliberately, not a superuser: these tests assert things about
    what the RAG layer exposes, and asserting them through a role with more
    access than the RAG layer has would not be asserting the same thing.
    """
    from meridian.db import connect, server_reachable

    if not server_reachable():
        pytest.skip(
            "Postgres not reachable — run `make up` (and `make rag-index`) to "
            "exercise the vector-store tests"
        )
    conn = connect("analytics_ro")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def indexed(db) -> int:
    """Number of rows in the vector store, skipping if it was never built.

    A test asserting "no PII in the store" passes trivially against an empty
    store. Every DB-backed test here depends on this fixture so that outcome is
    a skip with a reason, not a green tick.
    """
    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM rag.chunks")
        (n,) = cur.fetchone()
    if n == 0:
        pytest.skip("rag.chunks is empty — run `make rag-index`")
    return n


@pytest.fixture(scope="session")
def cfg():
    return settings()


@pytest.fixture(scope="session")
def retriever(indexed):
    """Session-scoped: constructing one loads the 67 MB ONNX model, and paying
    that per test would dominate the suite's runtime."""
    from meridian.rag.retrieve import Retriever

    r = Retriever.open()
    yield r
    r.close()
