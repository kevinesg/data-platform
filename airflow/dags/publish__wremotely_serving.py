from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import DAG

from _wremotely import (
    create_onprem_clickhouse_publication_signal_task,
    create_onprem_clickhouse_publication_snapshot_task,
    create_onprem_clickhouse_dbt_build_task,
    create_dbt_build_task,
    create_publication_hold_task,
    create_publication_signal_task,
    create_serving_snapshot_task,
)

BASE_RUN_ID = "{{ dag_run.conf['publication_run_id'] }}"


def choose_publication_mode() -> str:
    from airflow.sdk import get_current_context

    mode = get_current_context()["dag_run"].conf.get("publication_mode", "legacy")
    if mode == "clickhouse":
        return "publish_clickhouse_snapshot"
    if mode == "legacy":
        return "legacy_dbt_build"
    raise ValueError(f"unsupported publication mode: {mode}")


with DAG(
    dag_id="publish__wremotely_serving",
    description=(
        "Serialize Wremotely serving publication through one trigger-only DAG; "
        "normal runs use ClickHouse and legacy BigQuery remains an explicit recovery path."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    tags=["wremotely", "publishing"],
) as dag:
    choose_mode = BranchPythonOperator(
        task_id="choose_publication_mode",
        python_callable=choose_publication_mode,
        execution_timeout=timedelta(minutes=5),
    )

    legacy_dbt_build = create_dbt_build_task(task_id="legacy_dbt_build")
    legacy_publication_hold = create_publication_hold_task(
        f"{BASE_RUN_ID}-publication-hold",
        task_id="legacy_publication_hold",
    )
    legacy_publish_serving_snapshot = create_serving_snapshot_task(
        f"{BASE_RUN_ID}-serving-snapshot",
        task_id="legacy_publish_serving_snapshot",
    )
    legacy_signal_publication = create_publication_signal_task(
        f"{BASE_RUN_ID}-serving-snapshot",
        task_id="legacy_signal_publication",
    )

    dbt_build = create_onprem_clickhouse_dbt_build_task()
    publish_clickhouse_snapshot = create_onprem_clickhouse_publication_snapshot_task(
        BASE_RUN_ID
    )
    signal_clickhouse_publication = create_onprem_clickhouse_publication_signal_task(
        BASE_RUN_ID,
        task_id="signal_clickhouse_publication",
    )

    choose_mode >> legacy_dbt_build >> legacy_publication_hold
    legacy_publication_hold >> legacy_publish_serving_snapshot >> legacy_signal_publication
    choose_mode >> dbt_build >> publish_clickhouse_snapshot >> signal_clickhouse_publication
