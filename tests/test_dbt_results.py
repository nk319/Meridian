"""Parsing dbt's artefacts into the one data quality table.

These tests use hand-written `run_results.json` fragments rather than a live dbt
run, because the interesting cases are the ones a passing project does not
produce: a test that errored, a test that was skipped because its model failed,
a `warn` severity, a run with no manifest alongside it. A fixture built by
running dbt against this warehouse would contain 87 passes and prove none of it.
"""

from __future__ import annotations

import json

import pytest

from meridian.dbt.results import STATUS_MAP, DbtTestResult, _node_name, _node_target, parse


def run_results(*entries: dict) -> dict:
    return {"metadata": {"dbt_version": "1.12.3"}, "results": list(entries)}


def test_only_test_nodes_become_checks():
    """A model building is a pipeline event, not an assertion.

    Counting model nodes here would make "how many checks ran" a number that
    grows when somebody adds a model, which is the kind of metric that looks
    like improvement and is not.
    """
    payload = run_results(
        {"unique_id": "model.meridian.fact_orders", "status": "success"},
        {"unique_id": "test.meridian.unique_fact_orders_order_id.abc", "status": "pass"},
        {"unique_id": "snapshot.meridian.customers_snapshot", "status": "success"},
    )
    results = parse(payload)
    assert len(results) == 1
    assert results[0].status == "PASS"


@pytest.mark.parametrize(
    ("dbt_status", "expected"),
    [
        ("pass", "PASS"),
        ("success", "PASS"),
        ("warn", "FAIL"),
        ("fail", "FAIL"),
        ("error", "FAIL"),
        ("skipped", "FAIL"),
    ],
)
def test_status_mapping(dbt_status, expected):
    """`error` and `skipped` are failures, not passes.

    This is the mapping decision with consequences. A test that could not run
    has not passed, and recording it as PASS is how a test broken by a
    compilation error stays broken for a quarter while the dashboard stays
    green.
    """
    payload = run_results({"unique_id": "test.meridian.t.h", "status": dbt_status})
    assert parse(payload)[0].status == expected


def test_unknown_status_is_treated_as_a_blocking_failure():
    """A status dbt adds in a future version must not silently pass."""
    payload = run_results({"unique_id": "test.meridian.t.h", "status": "partial-success"})
    result = parse(payload)[0]
    assert result.status == "FAIL"
    assert result.blocking


def test_warn_severity_is_a_failure_that_does_not_block():
    """dbt's `warn` is the severity column's whole reason for existing.

    Recording it PASS loses the finding; recording it BLOCK stops a pipeline
    the test's author deliberately chose not to stop.
    """
    manifest = {
        "nodes": {
            "test.meridian.t.h": {
                "name": "some_tolerated_check",
                "config": {"severity": "warn"},
                "depends_on": {"nodes": ["model.meridian.fact_orders"]},
            }
        }
    }
    payload = run_results({"unique_id": "test.meridian.t.h", "status": "warn", "failures": 12})
    result = parse(payload, manifest)[0]
    assert (result.status, result.severity, result.blocking) == ("FAIL", "WARN", False)
    assert result.rows_failed == 12


def test_severity_defaults_to_block_without_a_manifest():
    """Absent evidence, assume the stricter reading.

    A test whose configured severity cannot be read is not one to quietly
    downgrade — the failure mode of guessing WARN is a blocking problem
    reported as advisory.
    """
    payload = run_results({"unique_id": "test.meridian.t.h", "status": "fail"})
    assert parse(payload)[0].severity == "BLOCK"


def test_singular_and_generic_test_names_both_survive():
    """The two unique_id shapes must not collapse to one name.

    A generic test is `test.<project>.<name>.<hash>`; a singular test is
    `test.<project>.<name>` with no hash. Taking a fixed offset from the end
    gets one right and labels every singular test with the project name — which
    the first version of this did, giving the six most interesting assertions in
    the project a single shared name in `meta.dq_check_results`.
    """
    assert (
        _node_name("test.meridian.scd2_no_overlapping_versions", {})
        == "scd2_no_overlapping_versions"
    )
    assert (
        _node_name("test.meridian.unique_dim_date_date_sk." + "a" * 32, {})
        == "unique_dim_date_date_sk"
    )
    # The manifest wins when present, because it is what dbt itself calls it.
    assert _node_name("test.meridian.whatever.h", {"name": "real_name"}) == "real_name"


def test_target_table_comes_from_the_first_dependency():
    """A relationships test depends on two models; it is *about* the first."""
    node = {
        "depends_on": {
            "nodes": [
                "model.meridian.fact_orders",
                "model.meridian.dim_customer",
            ]
        },
        "test_metadata": {"kwargs": {"column_name": "customer_sk"}},
    }
    assert _node_target(node) == ("fact_orders", "customer_sk")


def test_a_jinja_column_expression_is_not_stored_as_a_column_name():
    """Some test types render `column_name` as an unevaluated expression.

    Storing `{{ get_where_subquery(...) }}` in `target_column` would make the
    column useless for grouping, which is the only thing it is for.
    """
    node = {
        "depends_on": {"nodes": ["model.meridian.fact_orders"]},
        "test_metadata": {"kwargs": {"column_name": "{{ something }}"}},
    }
    assert _node_target(node)[1] is None


def test_a_test_with_no_dependencies_is_still_recorded():
    """Never drop a result for want of a target.

    An unattributable failure is still a failure, and a parser that skips what
    it cannot classify makes the results table quietly incomplete.
    """
    payload = run_results({"unique_id": "test.meridian.orphan", "status": "fail"})
    result = parse(payload)[0]
    assert result.target_table == "unknown"
    assert result.status == "FAIL"


def test_missing_failure_count_is_zero_not_none():
    """`rows_failed` is written to a bigint column that reads better as 0."""
    payload = run_results({"unique_id": "test.meridian.t.h", "status": "pass"})
    assert parse(payload)[0].rows_failed == 0


def test_every_mapped_status_produces_a_valid_table_value():
    """The status column has a CHECK constraint. Violating it fails the insert."""
    for status, _ in STATUS_MAP.values():
        assert status in ("PASS", "FAIL")


def test_result_is_json_round_trippable_for_the_run_log():
    payload = run_results({"unique_id": "test.meridian.t.h", "status": "fail", "failures": 3})
    result = parse(payload)[0]
    assert isinstance(result, DbtTestResult)
    assert json.dumps(result.__dict__)
