from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, TaskGroup
from docker.types import Mount

from _alerting import send_failure_alert

SCRIPTS_CREDENTIALS_CONTAINER_PATH = "/credentials/scripts-service-account.json"
DBT_CREDENTIALS_CONTAINER_PATH = "/credentials/dbt-service-account.json"
RUN_ID = "{{ dag_run.run_id }}"
LOAD_MODE_PARAM = "load_mode"
LOAD_MODE = f"{{{{ params.{LOAD_MODE_PARAM} }}}}"
SOURCE_ENTITIES = (
    "transactions",
    "pending_transactions",
    "paid_for_others",
    "transfers",
    "accounts",
)

DAG_RUN_TIMEOUT = timedelta(minutes=30)
TASK_EXECUTION_TIMEOUT = timedelta(minutes=10)
TASK_RETRIES = 2
TASK_RETRY_DELAY = timedelta(minutes=1)


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def optional_env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


def required_host_path_env(name: str) -> str:
    value = required_env(name)
    if not value.startswith("/"):
        raise RuntimeError(f"{name} must be an absolute host path")
    return value


def dag_schedule(environment: str, schedule_env_name: str) -> str | None:
    if environment != "prod":
        return None

    value = os.environ.get(schedule_env_name, "").strip()
    if not value:
        raise RuntimeError(f"missing required prod schedule: {schedule_env_name}")
    return value


def docker_task(
    task_id: str,
    image: str,
    command: list[str],
    environment: dict[str, str],
    mounts: list[Mount],
) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment,
        mounts=mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        execution_timeout=TASK_EXECUTION_TIMEOUT,
        retries=TASK_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        on_failure_callback=send_failure_alert,
    )


ENVIRONMENT = optional_env("ENVIRONMENT", "dev")
SCRIPTS_IMAGE = required_env("DATA_PLATFORM_SCRIPTS_IMAGE")
DBT_IMAGE = required_env("DATA_PLATFORM_DBT_IMAGE")

scripts_environment = {
    "ENVIRONMENT": ENVIRONMENT,
    "PROJECT_ID": required_env("PROJECT_ID"),
    "RAW_DATASET": required_env("RAW_DATASET"),
    "SCRIPTS_GOOGLE_APPLICATION_CREDENTIALS": SCRIPTS_CREDENTIALS_CONTAINER_PATH,
    "PERSONAL_FINANCE_GSHEET_URL": required_env("PERSONAL_FINANCE_GSHEET_URL"),
    "PERSONAL_FINANCE_GCS_BUCKET": required_env("PERSONAL_FINANCE_GCS_BUCKET"),
    "PERSONAL_FINANCE_GCS_PREFIX": required_env("PERSONAL_FINANCE_GCS_PREFIX"),
    "PERSONAL_FINANCE_CHUNK_SIZE": required_env("PERSONAL_FINANCE_CHUNK_SIZE"),
    "PERSONAL_FINANCE_JSONL_RETENTION_DAYS": required_env(
        "PERSONAL_FINANCE_JSONL_RETENTION_DAYS"
    ),
}

dbt_environment = {
    "DBT_TARGET": required_env("DBT_TARGET"),
    "PROJECT_ID": required_env("PROJECT_ID"),
    "RAW_DATASET": required_env("RAW_DATASET"),
    "DBT_DATASET": required_env("DBT_DATASET"),
    "DBT_GOOGLE_APPLICATION_CREDENTIALS": DBT_CREDENTIALS_CONTAINER_PATH,
    "BIGQUERY_LOCATION": required_env("BIGQUERY_LOCATION"),
    "DBT_THREADS": optional_env("DBT_THREADS", "4"),
}

scripts_mounts = [
    Mount(
        source=required_host_path_env("SCRIPTS_GOOGLE_APPLICATION_CREDENTIALS"),
        target=SCRIPTS_CREDENTIALS_CONTAINER_PATH,
        type="bind",
        read_only=True,
    )
]

dbt_mounts = [
    Mount(
        source=required_host_path_env("DBT_GOOGLE_APPLICATION_CREDENTIALS"),
        target=DBT_CREDENTIALS_CONTAINER_PATH,
        type="bind",
        read_only=True,
    )
]

with DAG(
    dag_id="etl__personal_finance",
    description="Extract and load personal finance source entities, then build dbt models.",
    start_date=datetime(2026, 1, 1),
    schedule=dag_schedule(ENVIRONMENT, "ETL__PERSONAL_FINANCE_SCHEDULE"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAG_RUN_TIMEOUT,
    params={LOAD_MODE_PARAM: "default"},
    tags=["personal_finance", "elt"],
) as dag:
    with TaskGroup(group_id="extract"):
        extract_tasks = {
            entity_name: docker_task(
                task_id=entity_name,
                image=SCRIPTS_IMAGE,
                command=[
                    "--step",
                    "extract",
                    "--entity",
                    entity_name,
                    "--run-id",
                    RUN_ID,
                ],
                environment=scripts_environment,
                mounts=scripts_mounts,
            )
            for entity_name in SOURCE_ENTITIES
        }

    with TaskGroup(group_id="load"):
        load_tasks = {
            entity_name: docker_task(
                task_id=entity_name,
                image=SCRIPTS_IMAGE,
                command=[
                    "--step",
                    "load",
                    "--entity",
                    entity_name,
                    "--run-id",
                    RUN_ID,
                    "--load-mode",
                    LOAD_MODE,
                ],
                environment=scripts_environment,
                mounts=scripts_mounts,
            )
            for entity_name in SOURCE_ENTITIES
        }

    dbt_build = docker_task(
        task_id="dbt_build",
        image=DBT_IMAGE,
        command=[
            "build",
            "--project-dir",
            "data_warehouse",
            "--target",
            required_env("DBT_TARGET"),
            "--select",
            "+path:models/marts/personal_finance",
        ],
        environment=dbt_environment,
        mounts=dbt_mounts,
    )

    cleanup = docker_task(
        task_id="cleanup",
        image=SCRIPTS_IMAGE,
        command=["--step", "cleanup"],
        environment=scripts_environment,
        mounts=scripts_mounts,
    )

    for entity_name in SOURCE_ENTITIES:
        extract_tasks[entity_name] >> load_tasks[entity_name]
        load_tasks[entity_name] >> dbt_build

    dbt_build >> cleanup
