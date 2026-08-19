from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG

from _wremotely import (
    create_onprem_clickhouse_publication_signal_task,
    create_onprem_clickhouse_publication_review_export_task,
    create_onprem_clickhouse_publication_review_sync_task,
    create_onprem_clickhouse_publication_snapshot_task,
    create_onprem_clickhouse_dbt_build_task,
)

BASE_RUN_ID = "{{ dag_run.conf['publication_run_id'] }}"


with DAG(
    dag_id="publish__wremotely_serving",
    description=(
        "Serialize Wremotely serving publication through one trigger-only DAG; "
        "all production runs use the ClickHouse/filesystem contract."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    tags=["wremotely", "publishing"],
) as dag:
    dbt_build = create_onprem_clickhouse_dbt_build_task()
    sync_publication_review = create_onprem_clickhouse_publication_review_sync_task(BASE_RUN_ID)
    publish_clickhouse_snapshot = create_onprem_clickhouse_publication_snapshot_task(
        BASE_RUN_ID
    )
    export_publication_review = create_onprem_clickhouse_publication_review_export_task(BASE_RUN_ID)
    signal_clickhouse_publication = create_onprem_clickhouse_publication_signal_task(
        BASE_RUN_ID,
        task_id="signal_clickhouse_publication",
    )

    sync_publication_review >> dbt_build
    dbt_build >> publish_clickhouse_snapshot >> export_publication_review >> signal_clickhouse_publication
