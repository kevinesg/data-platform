from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Param

from _wremotely import (
    ENVIRONMENT,
    RECHECK_TASK_EXECUTION_TIMEOUT,
    WREMOTELY_DOCKER_NETWORK_MODE,
    WREMOTELY_ETL_IMAGE,
    WREMOTELY_NETWORK_POOL,
    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
    WREMOTELY_WAREHOUSE_POOL,
    create_publication_trigger_task,
    dag_schedule,
    docker_task,
    etl_command,
    optional_env,
    required_env,
    wremotely_environment,
    wremotely_mounts,
)

BASE_RUN_ID = "{{ dag_run.logical_date.strftime('%Y%m%dT%H%M%SZ') }}-wremotely-lifecycle"
PREPARE_RECHECK_RUN_ID = f"{BASE_RUN_ID}-prepare"
RECHECK_RUN_ID = f"{BASE_RUN_ID}-recheck"
RECHECK_STAGE_RUN_ID = f"{BASE_RUN_ID}-stage"
PUBLICATION_RUN_ID = BASE_RUN_ID
RECHECK_BUCKET_COUNT = "7"
RECHECK_BUCKET_INDEX = "{{ ((dag_run.logical_date.timestamp() // 43200) % 7) | int }}"

DAG_RUN_TIMEOUT = timedelta(hours=12)

with DAG(
    dag_id="maintenance__wremotely_lifecycle",
    description="Recheck one stable active-job bucket and trigger serialized publication.",
    start_date=datetime(2026, 1, 1),
    schedule=dag_schedule(ENVIRONMENT, "WREMOTELY_LIFECYCLE_SCHEDULE"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAG_RUN_TIMEOUT,
    params={
        "recheck_limit": Param(
            default=0,
            type="integer",
            minimum=0,
            maximum=1000,
            title="Manual recheck safety limit",
            description=(
                "Use 0 for the complete scheduled bucket. Set a positive value only for a "
                "bounded manual development smoke."
            ),
        )
    },
    tags=["wremotely", "maintenance", "lifecycle"],
) as dag:
    prepare_recheck = docker_task(
        task_id="prepare_recheck",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "prepare-recheck-from-warehouse",
            "--run-id",
            PREPARE_RECHECK_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--raw-dataset",
            required_env("RAW_DATASET"),
            "--handoff-dataset",
            required_env("WREMOTELY_HANDOFF_DATASET"),
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
            "--recheck-bucket-count",
            RECHECK_BUCKET_COUNT,
            "--recheck-bucket-index",
            RECHECK_BUCKET_INDEX,
            "--recheck-limit",
            "{{ params.recheck_limit }}",
            "--recheck-min-age-hours",
            "0",
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    recheck = docker_task(
        task_id="recheck",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "recheck",
            "--run-id",
            RECHECK_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--recheck-input",
            (
                f"{WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH}/{PREPARE_RECHECK_RUN_ID}/"
                "prepare_recheck/recheck_candidates.jsonl"
            ),
            "--recheck-limit",
            "{{ params.recheck_limit }}",
            "--recheck-worker-count",
            optional_env("WREMOTELY_RECHECK_WORKER_COUNT", "16"),
            "--platform-worker-count",
            optional_env("WREMOTELY_PLATFORM_WORKER_COUNT", "2"),
            "--allow-empty-recheck-input",
            "--page-max-bytes",
            optional_env("WREMOTELY_PAGE_MAX_BYTES", "2097152"),
            "--domain-delay-seconds",
            optional_env("WREMOTELY_DOMAIN_DELAY_SECONDS", "1"),
            "--domain-failure-limit",
            optional_env("WREMOTELY_DOMAIN_FAILURE_LIMIT", "5"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=RECHECK_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_NETWORK_POOL,
    )

    stage_recheck = docker_task(
        task_id="stage_recheck",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "stage",
            "--stage-kind",
            "recheck",
            "--run-id",
            RECHECK_STAGE_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--recheck-run-id",
            RECHECK_RUN_ID,
            "--stage-chunk-row-count",
            optional_env("WREMOTELY_STAGE_CHUNK_ROW_COUNT", "5000"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    upload_recheck = docker_task(
        task_id="upload_recheck",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "upload",
            "--run-id",
            RECHECK_STAGE_RUN_ID,
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
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    load_recheck = docker_task(
        task_id="load_recheck",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "load",
            "--run-id",
            RECHECK_STAGE_RUN_ID,
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
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )

    trigger_publication = create_publication_trigger_task(PUBLICATION_RUN_ID)

    (
        prepare_recheck
        >> recheck
        >> stage_recheck
        >> upload_recheck
        >> load_recheck
        >> trigger_publication
    )
