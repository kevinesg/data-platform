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

EXTRACTION_RUN_ID = "{{ params.extraction_run_id }}"
REPLAY_PREFIX = f"{EXTRACTION_RUN_ID}-{{{{ params.replay_label }}}}"
JOB_FACTS_RUN_ID = f"{REPLAY_PREFIX}-job-facts"
CLASSIFICATION_RUN_ID = f"{REPLAY_PREFIX}-classify"
EVALUATION_RUN_ID = f"{REPLAY_PREFIX}-evaluate"
STAGE_RUN_ID = f"{REPLAY_PREFIX}-stage"

REPLAY_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)

with DAG(
    dag_id="repair__wremotely_classifications",
    description="Replay one completed historical extraction through raw classification load.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=24),
    params={
        "extraction_run_id": Param(
            type="string",
            pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$",
            title="Completed extraction run ID",
            description="Replay this completed extraction artifact. Process runs oldest first.",
        ),
        "replay_label": Param(
            type="string",
            pattern=r"^[a-z0-9][a-z0-9-]{0,39}$",
            title="Replay label",
            description=(
                "Stable classifier revision label. Reusing it verifies and skips completed work."
            ),
        ),
    },
    tags=["wremotely", "repair", "classification", "manual"],
) as dag:
    job_facts = docker_task(
        task_id="job_facts",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "job-facts",
            "--run-id",
            JOB_FACTS_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--extraction-run-id",
            EXTRACTION_RUN_ID,
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=REPLAY_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    classify = docker_task(
        task_id="classify",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "classify",
            "--run-id",
            CLASSIFICATION_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--extraction-run-id",
            EXTRACTION_RUN_ID,
            "--job-facts-run-id",
            JOB_FACTS_RUN_ID,
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

    evaluate = docker_task(
        task_id="evaluate",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "evaluate",
            "--run-id",
            EVALUATION_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--extraction-run-id",
            EXTRACTION_RUN_ID,
            "--job-facts-run-id",
            JOB_FACTS_RUN_ID,
            "--classification-run-id",
            CLASSIFICATION_RUN_ID,
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
            "classification_replay",
            "--run-id",
            STAGE_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--extraction-run-id",
            EXTRACTION_RUN_ID,
            "--job-facts-run-id",
            JOB_FACTS_RUN_ID,
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

    job_facts >> classify >> evaluate >> stage >> upload >> load
