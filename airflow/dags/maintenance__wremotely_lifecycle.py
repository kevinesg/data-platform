from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Param

from _wremotely import (
    RECHECK_TASK_EXECUTION_TIMEOUT,
    WREMOTELY_DAG_RUN_TIMESTAMP,
    WREMOTELY_DOCKER_NETWORK_MODE,
    WREMOTELY_ETL_IMAGE,
    WREMOTELY_NETWORK_POOL,
    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
    WREMOTELY_WAREHOUSE_POOL,
    WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
    ENVIRONMENT,
    create_publication_trigger_task,
    dag_schedule,
    docker_task,
    etl_command,
    onprem_wremotely_environment,
    onprem_wremotely_mounts,
    onprem_wremotely_private_environment,
    optional_env,
)

BASE_RUN_ID = f"{WREMOTELY_DAG_RUN_TIMESTAMP}-wremotely-lifecycle"
PREPARE_RECHECK_RUN_ID = f"{BASE_RUN_ID}-prepare"
RECHECK_RUN_ID = f"{BASE_RUN_ID}-recheck"
RECHECK_STAGE_RUN_ID = f"{BASE_RUN_ID}-stage"
RECHECK_LANDING_RUN_ID = f"{BASE_RUN_ID}-landing"
RECHECK_RAW_RUN_ID = f"{BASE_RUN_ID}-clickhouse-raw"
CLICKHOUSE_SNAPSHOT_RUN_ID = f"{BASE_RUN_ID}-clickhouse-snapshot"
RECHECK_BUCKET_COUNT = "7"
RECHECK_BUCKET_INDEX = (
    "{{ (((dag_run.logical_date or dag_run.run_after).timestamp() // 43200) % 7) | int }}"
)

DAG_RUN_TIMEOUT = timedelta(hours=12)

with DAG(
    dag_id="maintenance__wremotely_lifecycle",
    description=(
        "Recheck due Wremotely jobs from ClickHouse, land lifecycle facts locally, "
        "rebuild ClickHouse dbt models, and signal the immutable snapshot through Pub/Sub."
    ),
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
    tags=["wremotely", "maintenance", "lifecycle", "on-prem", "clickhouse"],
) as dag:
    prepare_recheck = docker_task(
        task_id="prepare_recheck",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "prepare-recheck-from-clickhouse",
            "--run-id",
            PREPARE_RECHECK_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--warehouse-root",
            WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
            "--recheck-bucket-count",
            RECHECK_BUCKET_COUNT,
            "--recheck-bucket-index",
            RECHECK_BUCKET_INDEX,
            "--recheck-limit",
            "{{ params.recheck_limit }}",
            "--recheck-min-age-hours",
            "0",
            "--recheck-min-posting-age-days",
            optional_env("WREMOTELY_LIFECYCLE_MIN_POSTING_AGE_DAYS", "21"),
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
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
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
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
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )

    land_filesystem = docker_task(
        task_id="land_filesystem",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "land-filesystem",
            "--run-id",
            RECHECK_LANDING_RUN_ID,
            "--stage-run-id",
            RECHECK_STAGE_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--warehouse-root",
            WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )

    load_clickhouse_raw = docker_task(
        task_id="load_clickhouse_raw",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "load-clickhouse-raw",
            "--run-id",
            RECHECK_RAW_RUN_ID,
            "--landing-run-id",
            RECHECK_LANDING_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--warehouse-root",
            WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )

    trigger_publication = create_publication_trigger_task(
        CLICKHOUSE_SNAPSHOT_RUN_ID,
        publication_mode="clickhouse",
    )

    (
        prepare_recheck
        >> recheck
        >> stage_recheck
        >> land_filesystem
        >> load_clickhouse_raw
        >> trigger_publication
    )
