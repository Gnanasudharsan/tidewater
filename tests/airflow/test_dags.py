"""DAG integrity tests: every DAG imports, is scheduled as intended, and is safe to run."""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"
EXPECTED = {
    "dbt_build_hourly": "@hourly",
    "backfill_range": None,
    "lakehouse_maintenance_nightly": "0 3 * * *",
    "dq_daily_report": "0 6 * * *",
}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_and_schedules(dagbag):
    assert set(dagbag.dag_ids) == set(EXPECTED)
    for dag_id, schedule in EXPECTED.items():
        assert dagbag.get_dag(dag_id).schedule_interval == schedule


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_safe_defaults(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag.catchup is False, "a missed day should not trigger a burst of runs"
    assert dag.max_active_runs == 1, "jobs are not designed to overlap with themselves"
    assert "tidewater" in dag.tags
    assert all(t.retries >= 1 for t in dag.tasks)


def test_hourly_build_order(dagbag):
    dag = dagbag.get_dag("dbt_build_hourly")
    assert dag.get_task("load_warehouse").downstream_task_ids == {"source_freshness"}
    assert dag.get_task("dbt_build").trigger_rule == "all_done"


def test_backfill_params_are_constrained(dagbag):
    params = dagbag.get_dag("backfill_range").params
    assert params.get_param("table").schema["enum"] == [
        "customers", "products", "orders", "order_items", "payments",
    ]  # fmt: skip
    assert params.get_param("start_date").schema["format"] == "date"


def test_backfill_rejects_inverted_window():
    import sys

    sys.path.insert(0, str(DAGS_DIR))
    from backfill_range import validate_window

    validate_window({"start_date": "2026-11-01", "end_date": "2026-11-02"})
    with pytest.raises(ValueError):
        validate_window({"start_date": "2026-11-02", "end_date": "2026-11-01"})
