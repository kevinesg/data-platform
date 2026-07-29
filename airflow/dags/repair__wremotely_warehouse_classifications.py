from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Param

from _wremotely import (
    WREMOTELY_DOCKER_NETWORK_MODE,
    WREMOTELY_ETL_IMAGE,
    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
    WREMOTELY_WAREHOUSE_POOL,
    docker_task,
    etl_command,
    optional_env,
    required_env,
    wremotely_environment,
    wremotely_mounts,
)

REPLAY_PREFIX = "warehouse-{{ params.replay_label }}"
PREPARE_RUN_ID = f"{REPLAY_PREFIX}-input"
CLASSIFICATION_RUN_ID = f"{REPLAY_PREFIX}-classify"
STAGE_RUN_ID = f"{REPLAY_PREFIX}-stage"

REPLAY_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)

with DAG(
    dag_id="repair__wremotely_warehouse_classifications",
    description=(
        "Rebuild current classifications from exact-lineage raw warehouse facts and load them."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=24),
    params={
        "replay_label": Param(
            type="string",
            pattern=r"^[a-z0-9][a-z0-9-]{0,39}$",
            title="Replay label",
            description=(
                "Stable classifier revision label. Reusing it verifies and skips completed work."
            ),
        ),
    },
    tags=["wremotely", "repair", "classification", "warehouse", "manual"],
) as dag:
    prepare = docker_task(
        task_id="prepare",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "prepare-classification-replay-from-warehouse",
            "--run-id",
            PREPARE_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--raw-dataset",
            required_env("RAW_DATASET"),
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=REPLAY_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )

    replay = docker_task(
        task_id="replay",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "replay-classification",
            "--run-id",
            CLASSIFICATION_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--classification-replay-input-run-id",
            PREPARE_RUN_ID,
            "--work-arrangement-mode",
            "raw_only",
            "--country-eligibility-mode",
            "raw_only",
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=REPLAY_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    stage = docker_task(
        task_id="stage",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "stage",
            "--stage-kind",
            "warehouse_classification_replay",
            "--run-id",
            STAGE_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--classification-run-id",
            CLASSIFICATION_RUN_ID,
            "--stage-chunk-row-count",
            optional_env("WREMOTELY_STAGE_CHUNK_ROW_COUNT", "5000"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=REPLAY_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    upload = docker_task(
        task_id="upload",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "upload",
            "--run-id",
            STAGE_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--gcs-bucket",
            required_env("WREMOTELY_GCS_BUCKET"),
            "--gcs-prefix",
            required_env("WREMOTELY_GCS_PREFIX"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=REPLAY_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    load = docker_task(
        task_id="load",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "load",
            "--run-id",
            STAGE_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--raw-dataset",
            required_env("RAW_DATASET"),
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=REPLAY_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )

    prepare >> replay >> stage >> upload >> load
