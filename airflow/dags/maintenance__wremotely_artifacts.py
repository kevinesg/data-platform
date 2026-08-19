from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG

from _wremotely import (
    WREMOTELY_DAG_RUN_TIMESTAMP,
    WREMOTELY_DOCKER_NETWORK_MODE,
    WREMOTELY_ETL_IMAGE,
    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
    WREMOTELY_WAREHOUSE_POOL,
    docker_task,
    etl_command,
    wremotely_environment,
    wremotely_mounts,
)

CLEANUP_RUN_ID = f"{WREMOTELY_DAG_RUN_TIMESTAMP}-wremotely-cleanup"
CLEANUP_MIN_AGE_DAYS = "3"
CLEANUP_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)
DAG_RUN_TIMEOUT = timedelta(hours=12)

with DAG(
    dag_id="maintenance__wremotely_artifacts",
    description=(
        "Filesystem artifact cleanup for completed on-prem Wremotely runs; manual "
        "until its production retention policy is enabled."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAG_RUN_TIMEOUT,
    tags=["wremotely", "maintenance", "artifacts"],
) as dag:
    cleanup = docker_task(
        task_id="cleanup",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "cleanup",
            "--run-id",
            CLEANUP_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--cleanup-min-age-days",
            CLEANUP_MIN_AGE_DAYS,
            "--cleanup-apply",
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=CLEANUP_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )
