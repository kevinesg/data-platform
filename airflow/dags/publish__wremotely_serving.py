from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG

from _wremotely import (
    create_dbt_build_task,
    create_publication_hold_task,
    create_publication_signal_task,
    create_serving_snapshot_task,
)

BASE_RUN_ID = "{{ dag_run.conf['publication_run_id'] }}"
PUBLICATION_HOLD_RUN_ID = f"{BASE_RUN_ID}-publication-hold"
SERVING_SNAPSHOT_RUN_ID = f"{BASE_RUN_ID}-serving-snapshot"

with DAG(
    dag_id="publish__wremotely_serving",
    description="Build and publish wremotely serving state for one completed producer load.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    tags=["wremotely", "publishing"],
) as dag:
    dbt_build = create_dbt_build_task()
    publication_hold = create_publication_hold_task(PUBLICATION_HOLD_RUN_ID)
    publish_serving_snapshot = create_serving_snapshot_task(SERVING_SNAPSHOT_RUN_ID)
    signal_publication = create_publication_signal_task(SERVING_SNAPSHOT_RUN_ID)

    dbt_build >> publication_hold >> publish_serving_snapshot >> signal_publication
