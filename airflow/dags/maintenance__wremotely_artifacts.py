from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG

from _wremotely import (
    ENVIRONMENT,
    WREMOTELY_DOCKER_NETWORK_MODE,
    WREMOTELY_ETL_IMAGE,
    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
    WREMOTELY_WAREHOUSE_POOL,
    dag_schedule,
    docker_task,
    etl_command,
    required_env,
    wremotely_environment,
    wremotely_mounts,
)

CLEANUP_RUN_ID = "{{ dag_run.logical_date.strftime('%Y%m%dT%H%M%SZ') }}-wremotely-cleanup"
CLEANUP_MIN_AGE_DAYS = "3"
CLEANUP_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)
DAG_RUN_TIMEOUT = timedelta(hours=12)

with DAG(
    dag_id="maintenance__wremotely_artifacts",
    description="Delete verified-safe wremotely local and GCS artifacts after three days.",
    start_date=datetime(2026, 1, 1),
    schedule=dag_schedule(ENVIRONMENT, "WREMOTELY_ARTIFACT_CLEANUP_SCHEDULE"),
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
            "--cleanup-gcs",
            "--cleanup-apply",
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--gcs-bucket",
            required_env("WREMOTELY_GCS_BUCKET"),
            "--gcs-prefix",
            required_env("WREMOTELY_GCS_PREFIX"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=CLEANUP_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )
