"""End-to-end invariants of the Bronze → Silver → warehouse pipeline.

Database-backed; they skip cleanly without the stack. What they assert is the
set of things that are individually easy to get wrong and collectively
impossible to notice: a Silver table that silently lost rows, lineage that does
not join, a watermark that never advanced, a reconciliation check that was
recorded as passing without ever running.
"""

from __future__ import annotations

import pytest

from meridian.lake.layout import bronze_glob, silver_glob
from meridian.lake.silver_spec import SPECS

pytestmark = pytest.mark.db

WATERMARKED = [e for e, s in SPECS.items() if s.recency != "_ingested_at"]


@pytest.fixture(scope="module")
def etl():
    from meridian.db import connect, server_reachable

    if not server_reachable():
        pytest.skip("Postgres not reachable — run `make up`")
    conn = connect("meridian_etl", vectors=False)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def loaded(etl):
    """Row counts per silver table, or skip if the warehouse was never loaded."""
    counts = {}
    with etl.cursor() as cur:
        for entity in SPECS:
            cur.execute(f"SELECT count(*) FROM silver.{entity}")
            counts[entity] = cur.fetchone()[0]
    if not any(counts.values()):
        pytest.skip("silver is empty — run `make ingest && make silver && make load-warehouse`")
    return counts


@pytest.fixture(scope="module")
def duck():
    from meridian.lake.duck import connect

    return connect()


# ---------------------------------------------------------------------------
# the tables have data
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity", sorted(SPECS))
def test_every_silver_table_is_populated(entity, loaded):
    assert loaded[entity] > 0, f"silver.{entity} is empty"


def test_silver_matches_its_parquet(duck, etl, loaded):
    """The warehouse load must not have lost or invented rows.

    A count mismatch here is the load-bearing hop failing quietly, which is the
    one failure mode that looks like a business change rather than a bug.
    """
    from meridian.settings import settings

    cfg = settings()
    for entity, pg_rows in loaded.items():
        glob = silver_glob(cfg.lake_bucket, entity)
        (parquet_rows,) = duck.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()
        assert parquet_rows == pg_rows, f"{entity}: parquet {parquet_rows} vs postgres {pg_rows}"


# ---------------------------------------------------------------------------
# dedup actually happened
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity", sorted(SPECS))
def test_natural_key_is_unique_in_silver(entity, etl, loaded):
    """Bronze is append-only, so a re-ingested entity holds several versions of
    a changed row. Silver keeps one."""
    key = ", ".join(SPECS[entity].key)
    with etl.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM (SELECT {key} FROM silver.{entity} "
            f"GROUP BY {key} HAVING count(*) > 1) d"
        )
        assert cur.fetchone()[0] == 0


def test_bronze_holds_duplicates_that_silver_removed(duck, etl, loaded):
    """Guard against dedup looking correct because there was nothing to dedup.

    The generator injects exact duplicate rows into the third-party feeds. If
    Bronze has none of them, every dedup assertion above is vacuous.
    """
    from meridian.settings import settings

    cfg = settings()
    glob = bronze_glob(cfg.lake_bucket, "files", "web_events")
    (total, distinct) = duck.execute(
        f"SELECT count(*), count(DISTINCT _record_hash) FROM read_parquet('{glob}')"
    ).fetchone()
    assert total > distinct, "no duplicate rows in Bronze; the dedup tests prove nothing"


# ---------------------------------------------------------------------------
# lineage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity", sorted(SPECS))
def test_lineage_columns_are_populated(entity, etl, loaded):
    with etl.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM silver.{entity} WHERE _source_system IS NULL "
            f"OR _ingest_run_id IS NULL OR _ingested_at IS NULL OR _record_hash IS NULL"
        )
        assert cur.fetchone()[0] == 0


@pytest.mark.parametrize("entity", sorted(SPECS))
def test_lineage_run_id_joins_to_the_run_log(entity, etl, loaded):
    """`_ingest_run_id` is only lineage if it resolves to a run."""
    with etl.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM silver.{entity} s "
            f"LEFT JOIN meta.pipeline_run_log r ON r.run_id = s._ingest_run_id "
            f"WHERE r.run_id IS NULL"
        )
        assert cur.fetchone()[0] == 0, f"silver.{entity} references unknown ingest runs"


@pytest.mark.parametrize("entity", sorted(SPECS))
def test_source_system_matches_the_declared_owner(entity, etl, loaded):
    with etl.cursor() as cur:
        cur.execute(f"SELECT DISTINCT _source_system FROM silver.{entity}")
        sources = {r[0] for r in cur.fetchall()}
    assert sources == {SPECS[entity].source}, f"{entity} arrived from {sources}"


# ---------------------------------------------------------------------------
# watermarks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity", sorted(WATERMARKED))
def test_watermark_advanced(entity, etl, loaded):
    with etl.cursor() as cur:
        cur.execute(
            "SELECT watermark_value FROM meta.ingest_watermarks WHERE entity = %s", (entity,)
        )
        row = cur.fetchone()
    assert row is not None, f"no watermark recorded for {entity}; incremental would re-read all"
    assert row[0] is not None


def test_full_refresh_only_entities_have_no_watermark(etl, loaded):
    """products has no event timestamp. A watermark for it would imply an
    incremental capability that does not exist."""
    with etl.cursor() as cur:
        cur.execute("SELECT count(*) FROM meta.ingest_watermarks WHERE entity = 'products'")
        assert cur.fetchone()[0] == 0


# ---------------------------------------------------------------------------
# data quality was recorded, and the gates ran
# ---------------------------------------------------------------------------


def test_quarantine_rate_gate_ran_for_every_entity(etl, loaded):
    with etl.cursor() as cur:
        cur.execute(
            "SELECT target_table FROM meta.dq_check_results "
            "WHERE check_name = 'quarantine_rate' AND severity = 'BLOCK'"
        )
        checked = {r[0] for r in cur.fetchall()}
    assert {f"silver.{e}" for e in SPECS} <= checked


def test_quarantine_gate_passed(etl, loaded):
    """The most recent verdict per entity, not every verdict ever recorded.

    meta.dq_check_results is an append-only history, so a gate that fired once —
    a deliberately tightened threshold, a bad batch since reprocessed — leaves a
    FAIL row forever. Asserting the history contains no failure would make this
    test permanently red after the first legitimate block, which is a test that
    gets deleted rather than fixed.
    """
    with etl.cursor() as cur:
        cur.execute(
            "SELECT target_table, failure_pct FROM ("
            "  SELECT target_table, failure_pct, status,"
            "         row_number() OVER (PARTITION BY target_table"
            "                            ORDER BY checked_at DESC) AS rn"
            "  FROM meta.dq_check_results WHERE check_name = 'quarantine_rate'"
            ") latest WHERE rn = 1 AND status = 'FAIL'"
        )
        failures = cur.fetchall()
    assert not failures, f"entities currently over the quarantine ceiling: {failures}"


def test_defects_were_actually_caught(etl, loaded):
    """The seed injects known-bad rows into the third-party feeds.

    Zero quarantined rows would mean the validation never fired — which is
    exactly what a broken rule generator looks like from the outside.
    """
    with etl.cursor() as cur:
        cur.execute(
            "SELECT coalesce(sum(rows_failed), 0) FROM meta.dq_check_results "
            "WHERE severity = 'QUARANTINE' AND status = 'FAIL'"
        )
        assert cur.fetchone()[0] > 0


def test_reconciliation_recorded_for_every_loaded_entity(etl, loaded):
    with etl.cursor() as cur:
        cur.execute(
            "SELECT target_table FROM meta.dq_check_results "
            "WHERE check_name = 'row_count_reconciliation'"
        )
        checked = {r[0] for r in cur.fetchall()}
    assert {f"silver.{e}" for e in SPECS} <= checked


def test_clean_oltp_feeds_have_nothing_quarantined(etl, loaded):
    """The generator confines defects to the third-party feeds on purpose: a
    corrupted OLTP row would make Bronze reconciliation fail permanently."""
    with etl.cursor() as cur:
        cur.execute(
            "SELECT target_table, sum(rows_failed) FROM meta.dq_check_results "
            "WHERE severity = 'QUARANTINE' AND status = 'FAIL' "
            "  AND target_table IN ('silver.customers','silver.orders',"
            "                       'silver.order_items','silver.customer_change_log',"
            "                       'silver.support_tickets') "
            "GROUP BY 1"
        )
        assert cur.fetchall() == []


# ---------------------------------------------------------------------------
# the run log
# ---------------------------------------------------------------------------


def test_every_run_finished(etl, loaded):
    with etl.cursor() as cur:
        cur.execute("SELECT count(*) FROM meta.pipeline_run_log WHERE status = 'RUNNING'")
        assert cur.fetchone()[0] == 0, "a pipeline run was left marked RUNNING"


def test_the_dashboard_cache_key_is_populated(etl, loaded):
    """§7: the dashboard's cache key reads completed_at."""
    with etl.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM meta.pipeline_run_log "
            "WHERE status = 'SUCCESS' AND completed_at IS NULL"
        )
        assert cur.fetchone()[0] == 0
