"""The frozen lake contracts, checked without a database.

CONTRACTS.md §2 freezes the object-storage layout and the six Bronze metadata
columns, and §9 freezes the vocabularies. Every one of those is a name that
crosses a component boundary, which means the failure mode when one drifts is
not an error — it is an empty result set, or a row that quietly fails to match.
These run everywhere, including CI with no Docker.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from meridian.ingest.base import OWNERSHIP, check_ownership
from meridian.lake import layout
from meridian.lake.bronze import record_hash_expr
from meridian.lake.build_silver import quarantine_reason_sql
from meridian.lake.silver_spec import SPECS

BUCKET = "meridian-lake"
DAY = dt.date(2026, 8, 27)


# ---------------------------------------------------------------------------
# §2 layout
# ---------------------------------------------------------------------------


def test_bronze_path_matches_the_frozen_layout():
    assert layout.bronze_file(BUCKET, "oltp", "orders", DAY, "run-1", 3) == (
        "s3://meridian-lake/bronze/oltp/orders/ingest_date=2026-08-27/part-run-1-0003.parquet"
    )


def test_silver_and_quarantine_paths_match_the_frozen_layout():
    assert layout.silver_glob(BUCKET, "orders") == "s3://meridian-lake/silver/orders/part-*.parquet"
    assert layout.silver_current(BUCKET, "orders").startswith("s3://meridian-lake/silver/orders/")
    assert layout.quarantine_file(BUCKET, "orders", DAY, "r") == (
        "s3://meridian-lake/quarantine/orders/ingest_date=2026-08-27/part-r.parquet"
    )


def test_silver_current_is_matched_by_the_silver_glob():
    """Silver is a full rebuild into one file; the reader globs.

    If the writer's name ever stopped matching the reader's pattern the loader
    would find nothing and report zero rows — which reads as "no data" rather
    than "wrong filename".
    """
    import fnmatch

    current = layout.silver_current(BUCKET, "orders")
    assert fnmatch.fnmatch(current, layout.silver_glob(BUCKET, "orders"))


def test_unknown_source_system_is_rejected():
    with pytest.raises(ValueError, match="unknown source system"):
        layout.bronze_prefix(BUCKET, "ftp", "orders")


def test_the_six_metadata_columns_are_exactly_these():
    assert layout.BRONZE_METADATA_NAMES == (
        "_ingested_at",
        "_ingest_run_id",
        "_source_system",
        "_source_file",
        "_record_hash",
        "_batch_seq",
    )


# ---------------------------------------------------------------------------
# record hash
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def duck():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con


def _hash(duck, values):
    expr = record_hash_expr(["a", "b", "c"])
    return duck.execute(
        f"SELECT {expr} FROM (SELECT ? AS a, ? AS b, ? AS c)", list(values)
    ).fetchone()[0]


def test_hash_distinguishes_null_position(duck):
    """concat_ws drops NULLs, so without the sentinel these collide — and two
    genuinely different records would dedup into one in Silver."""
    assert _hash(duck, ("a", None, "b")) != _hash(duck, ("a", "b", None))


def test_hash_distinguishes_field_boundaries(duck):
    assert _hash(duck, ("ab", "c", None)) != _hash(duck, ("a", "bc", None))


def test_hash_is_stable_for_identical_input(duck):
    assert _hash(duck, ("a", "b", "c")) == _hash(duck, ("a", "b", "c"))


def test_hash_over_no_columns_is_refused():
    with pytest.raises(ValueError, match="constant"):
        record_hash_expr([])


# ---------------------------------------------------------------------------
# one entity, one owner
# ---------------------------------------------------------------------------


def test_every_silver_entity_has_a_declared_owner():
    assert set(SPECS) == set(OWNERSHIP)


def test_spec_source_matches_the_ownership_map():
    for entity, spec in SPECS.items():
        assert spec.source == OWNERSHIP[entity], entity


def test_ingesting_an_entity_you_do_not_own_is_refused():
    """The Phase 0 lesson, enforced.

    support_tickets lives in the oltp database and is ingested from the REST
    source. Adding it to the OLTP ingestor is the obvious-looking change that
    double-counts it in Bronze and makes reconciliation fail permanently.
    """
    check_ownership("restapi", "support_tickets")
    with pytest.raises(ValueError, match="ingested from 'restapi'"):
        check_ownership("oltp", "support_tickets")


# ---------------------------------------------------------------------------
# the vocabularies are a copy — keep it honest
# ---------------------------------------------------------------------------


def test_silver_vocabularies_match_the_frozen_ones():
    """silver_spec duplicates §9's lists so the lake does not import the
    stdlib-only seed package. A copy nobody checks is a copy that drifts."""
    from meridian.lake import silver_spec
    from meridian.seed import config as seed_config

    for name in (
        "ORDER_STATUS",
        "PAYMENT_STATUS",
        "PAYMENT_METHOD",
        "LOYALTY_TIER",
        "CUSTOMER_SEGMENT",
        "CHANNEL",
        "DEVICE_TYPE",
        "EVENT_TYPE",
        "TICKET_INTENT",
        "TICKET_PRIORITY",
        "SENTIMENT",
    ):
        assert tuple(getattr(seed_config, name)) == getattr(silver_spec, name), name


def test_every_enum_column_uses_a_frozen_vocabulary():
    from meridian.lake import silver_spec

    frozen = {v for k, v in vars(silver_spec).items() if k.isupper() and isinstance(v, tuple)}
    for spec in SPECS.values():
        for col in spec.columns:
            if col.enum is not None:
                assert col.enum in frozen, f"{spec.entity}.{col.name} uses an ad-hoc enum"


# ---------------------------------------------------------------------------
# validation rules are generated for every column
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity", sorted(SPECS))
def test_quarantine_reason_covers_every_rule(entity):
    spec = SPECS[entity]
    sql = quarantine_reason_sql(spec)
    for col in spec.columns:
        if not col.nullable:
            assert f"'missing:{col.name}'" in sql
        assert f"'bad_type:{col.name}'" in sql
        if col.enum:
            assert f"'bad_enum:{col.name}'" in sql
        if col.minimum is not None:
            assert f"'out_of_range:{col.name}'" in sql


@pytest.mark.parametrize("entity", sorted(SPECS))
def test_quarantine_reason_is_valid_sql(duck, entity):
    """Generated SQL that does not parse fails at run time, on real data,
    after the ingestion that produced it has already been paid for."""
    spec = SPECS[entity]
    columns = ", ".join(f"NULL AS {c.name}" for c in spec.columns)
    duck.execute(f"SELECT {quarantine_reason_sql(spec)} FROM (SELECT {columns})")


def test_recency_column_exists_on_the_entity():
    for spec in SPECS.values():
        if spec.recency != "_ingested_at":
            known = set(spec.business_columns) | set(spec.drop)
            assert spec.recency in known, f"{spec.entity} orders by an absent column"
