"""The governance control CONTRACTS.md §10 names as the proof.

"analytics_ro and rag_indexer have no grant on `secure`. This is the
enforcement mechanism; the documentation merely describes it."

A claim like that is worth exactly as much as the test that fails when it stops
being true. So this connects as the real roles, selects a real PII column, and
asserts the database refuses — rather than reading the grant statements back and
agreeing with them.

Every negative test here is paired with a positive one. "The query failed" is
also what you get from a typo in a table name, an empty schema, or a role that
cannot connect at all, and all three would pass a test that only checks for an
exception.
"""

from __future__ import annotations

import psycopg
import pytest

from meridian.db import connect, server_reachable
from meridian.lake.build_silver import restricted_column_names

pytestmark = pytest.mark.db

PII_COLUMNS = ("first_name", "last_name", "email", "phone")


@pytest.fixture(scope="module", autouse=True)
def _require_stack():
    """Autouse, because several tests here open their own connections.

    Without it those tests fail rather than skip on a machine with no stack
    running — and a suite that errors when the infrastructure is simply absent
    is one people learn to ignore.
    """
    if not server_reachable():
        pytest.skip("Postgres not reachable — run `make up`")


@pytest.fixture(scope="module")
def etl(_require_stack):
    conn = connect("meridian_etl", vectors=False)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def loaded(etl) -> int:
    """PII actually present, or skip.

    Without this, every assertion below passes against an empty table — and
    "analytics_ro could not read the PII" would be true because there was none.
    """
    with etl.cursor() as cur:
        cur.execute("SELECT count(*) FROM secure.customer_pii")
        (n,) = cur.fetchone()
    if n == 0:
        pytest.skip("secure.customer_pii is empty — run `make load-warehouse`")
    return n


# ---------------------------------------------------------------------------
# the positive half: the data is really there, and really is PII
# ---------------------------------------------------------------------------


def test_the_loader_can_read_pii(etl, loaded):
    """meridian_etl owns `secure` and must be able to read it."""
    with etl.cursor() as cur:
        cur.execute("SELECT first_name, last_name, email, phone FROM secure.customer_pii LIMIT 1")
        row = cur.fetchone()
    assert row and all(row), f"secure.customer_pii holds blank values: {row}"


def test_governance_file_classifies_these_columns_restricted():
    """The test and the policy must be talking about the same columns."""
    assert set(PII_COLUMNS) <= restricted_column_names()


# ---------------------------------------------------------------------------
# the control itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", ["analytics_ro", "rag_indexer"])
def test_role_cannot_select_a_pii_column(role, loaded):
    """The assertion CONTRACTS §10 asks for, for both roles it names."""
    with connect(role, vectors=False) as conn:
        for column in PII_COLUMNS:
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.cursor() as cur:
                cur.execute(f"SELECT {column} FROM secure.customer_pii LIMIT 1")
            conn.rollback()


@pytest.mark.parametrize("role", ["analytics_ro", "rag_indexer"])
def test_role_has_no_usage_on_the_secure_schema(role):
    """Blocked at the schema, not merely at the table.

    A table-level revoke would still let the role enumerate what exists there,
    and a table added later would arrive readable unless someone remembered.
    """
    with connect(role, vectors=False) as conn, conn.cursor() as cur:
        cur.execute("SELECT has_schema_privilege('secure', 'USAGE')")
        assert cur.fetchone()[0] is False


def test_analytics_ro_can_still_do_its_job(loaded):
    """The negative tests must not be passing because the role is simply broken."""
    with connect("analytics_ro", vectors=False) as conn, conn.cursor() as cur:
        cur.execute("SELECT has_schema_privilege('gold', 'USAGE')")
        assert cur.fetchone()[0] is True
        # §7: the dashboard's cache key reads this column.
        cur.execute("SELECT count(*) FROM meta.pipeline_run_log")
        assert cur.fetchone()[0] >= 0


def test_rag_indexer_reads_tickets_but_not_the_rest_of_silver():
    """CONTRACTS §1 grants this role `silver.support_*` and nothing more."""
    with connect("rag_indexer", vectors=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM silver.support_tickets")
            assert cur.fetchone()[0] >= 0
        for table in ("customers", "orders", "payments"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM silver.{table}")
            conn.rollback()


# ---------------------------------------------------------------------------
# PII must not have leaked sideways
# ---------------------------------------------------------------------------


def test_no_restricted_column_appears_outside_secure(etl):
    """Physical separation, checked against the catalogue.

    The grant tests prove `secure` is unreachable. This proves the data is not
    also sitting somewhere reachable under the same column name — which is how
    physical separation actually fails in practice.
    """
    restricted = restricted_column_names()
    with etl.cursor() as cur:
        cur.execute(
            "SELECT table_schema, table_name, column_name FROM information_schema.columns "
            "WHERE table_schema IN ('silver','gold','gold_stg','gold_int','rag','meta') "
            "  AND column_name = ANY(%s)",
            (sorted(restricted),),
        )
        leaks = cur.fetchall()
    assert not leaks, f"restricted PII column names outside `secure`: {leaks}"
