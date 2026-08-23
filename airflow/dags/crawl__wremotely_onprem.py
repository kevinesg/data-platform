from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.sdk import DAG, Param

from _wremotely import (
    APPROVED_SOURCE_REGISTRY_CONTAINER_PATH,
    ENVIRONMENT,
    WREMOTELY_CRAWL_POOL,
    WREMOTELY_DAG_RUN_TIMESTAMP,
    WREMOTELY_DOCKER_NETWORK_MODE,
    WREMOTELY_ETL_IMAGE,
    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
    WREMOTELY_WAREHOUSE_POOL,
    dag_schedule,
    docker_task,
    etl_command,
    onprem_wremotely_environment,
    onprem_wremotely_mounts,
    onprem_wremotely_private_environment,
    optional_env,
)


GENERATION_RUN_ID = f"{WREMOTELY_DAG_RUN_TIMESTAMP}-wremotely-crawl"
SHARD_COUNT = optional_env("WREMOTELY_SOURCE_CRAWL_SHARD_COUNT", "6")
CRAWL_TASK_EXECUTION_TIMEOUT = timedelta(hours=4)
DAG_RUN_TIMEOUT = timedelta(hours=12)


def shard_run_id(shard_index: int) -> str:
    return f"{GENERATION_RUN_ID}-shard{shard_index}"


def refreshable_crawl_command(*args: str) -> str:
    """Add the explicit full-refresh flag only when requested at trigger time."""

    serialized = json.dumps(list(args))
    return f'{serialized[:-1]}{{% if params.full_refresh %}}, "--full-refresh"{{% endif %}}]'


with DAG(
    dag_id="crawl__wremotely_onprem",
    description=(
        "Crawl one deterministic source shard at a time and publish one complete "
        "generation for the ClickHouse-backed Wremotely ELT DAG."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=(
        dag_schedule(ENVIRONMENT, "WREMOTELY_CRAWL_SCHEDULE")
        if ENVIRONMENT == "prod"
        else None
    ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAG_RUN_TIMEOUT,
    params={
        "full_refresh": Param(
            default=False,
            type="boolean",
            title="Full source crawl refresh",
            description=(
                "Disable the seven-day Workday/Workable hot pass and refetch the "
                "approved registry. Use only for an intentional full crawl."
            ),
        )
    },
    tags=["wremotely", "crawl", "on-prem", "clickhouse"],
) as dag:
    shards = []
    for shard_index in range(int(SHARD_COUNT)):
        shards.append(
            docker_task(
                task_id=f"crawl_shard_{shard_index}",
                image=WREMOTELY_ETL_IMAGE,
                command=refreshable_crawl_command(
                    "--step",
                    "crawl",
                    "--run-id",
                    shard_run_id(shard_index),
                    "--output-root",
                    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
                    "--source-registry-input",
                    APPROVED_SOURCE_REGISTRY_CONTAINER_PATH,
                    "--source-crawl-limit",
                    "0",
                    "--source-crawl-max-job-urls",
                    "0",
                    "--source-crawl-max-pages-per-source",
                    "15",
                    "--source-crawl-max-links-per-page",
                    "1000",
                    "--source-crawl-shard-count",
                    SHARD_COUNT,
                    "--source-crawl-shard-index",
                    str(shard_index),
                    "--source-hot-pass-days",
                    optional_env("WREMOTELY_SOURCE_HOT_PASS_DAYS", "7"),
                    "--source-crawl-worker-count",
                    optional_env("WREMOTELY_SOURCE_CRAWL_WORKER_COUNT", "6"),
                    "--platform-worker-count",
                    optional_env("WREMOTELY_PLATFORM_WORKER_COUNT", "2"),
                    "--candidate-sample-seed",
                    GENERATION_RUN_ID,
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
                execution_timeout=CRAWL_TASK_EXECUTION_TIMEOUT,
                network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
                pool=WREMOTELY_CRAWL_POOL,
            )
        )

    merge = docker_task(
        task_id="merge_crawl_generation",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_crawl_command(
            "--step",
            "merge-crawl",
            "--run-id",
            f"{GENERATION_RUN_ID}-merged",
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            *sum(
                (["--source-crawl-shard-run-id", shard_run_id(index)] for index in range(int(SHARD_COUNT))),
                [],
            ),
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        execution_timeout=CRAWL_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )
    publish = docker_task(
        task_id="publish_crawl_generation",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "publish-crawl-generation",
            "--run-id",
            f"{GENERATION_RUN_ID}-publish",
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--source-crawl-generation-run-id",
            f"{GENERATION_RUN_ID}-merged",
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        execution_timeout=timedelta(minutes=15),
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )

    shards >> merge >> publish
