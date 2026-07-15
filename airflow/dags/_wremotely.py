from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from docker.types import Mount

from _alerting import send_failure_alert

WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH = "/credentials/wremotely-etl-service-account.json"
DBT_CREDENTIALS_CONTAINER_PATH = "/credentials/dbt-service-account.json"
PUBLICATION_HOLD_POLICY_CONTAINER_PATH = "/run/secrets/wremotely-publication-hold-policy.md"
WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH = "/artifacts/wremotely-etl"
APPROVED_SOURCE_REGISTRY_CONTAINER_PATH = "/app/source_registry/approved_sources.jsonl"

DEFAULT_TASK_EXECUTION_TIMEOUT = timedelta(hours=2)
CRAWL_TASK_EXECUTION_TIMEOUT = timedelta(hours=18)
EXTRACT_TASK_EXECUTION_TIMEOUT = timedelta(hours=18)
PUBLICATION_HOLD_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)
RECHECK_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)
DBT_TASK_EXECUTION_TIMEOUT = timedelta(hours=2)
TASK_RETRIES = 2
TASK_RETRY_DELAY = timedelta(minutes=5)
SERVING_PUBLICATIONS_TABLE = "wremotely__serving_publication"
WREMOTELY_NETWORK_POOL = "wremotely_network"
WREMOTELY_WAREHOUSE_POOL = "wremotely_warehouse"
WREMOTELY_PUBLICATION_DAG_ID = "publish__wremotely_serving"


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


def dbt_schema_name(default_schema: str, custom_schema: str, environment: str) -> str:
    if environment in {"qa", "prod"}:
        return custom_schema
    return f"{default_schema}_{custom_schema}"


def docker_task(
    task_id: str,
    image: str,
    command: list[str] | str,
    environment: dict[str, str],
    mounts: list[Mount],
    execution_timeout: timedelta = DEFAULT_TASK_EXECUTION_TIMEOUT,
    network_mode: str | None = None,
    entrypoint: list[str] | None = None,
    pool: str | None = None,
) -> DockerOperator:
    operator_options = {"pool": pool} if pool else {}
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment,
        mounts=mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        auto_remove="force",
        force_pull=False,
        execution_timeout=execution_timeout,
        retries=TASK_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        network_mode=network_mode,
        entrypoint=entrypoint,
        on_failure_callback=send_failure_alert,
        **operator_options,
    )


def etl_command(*args: str) -> list[str]:
    return list(args)


ENVIRONMENT = optional_env("ENVIRONMENT", "dev")
WREMOTELY_ETL_IMAGE = required_env("DATA_PLATFORM_WREMOTELY_ETL_IMAGE")
SCRIPTS_IMAGE = required_env("DATA_PLATFORM_SCRIPTS_IMAGE")
DBT_IMAGE = required_env("DATA_PLATFORM_DBT_IMAGE")
WREMOTELY_DOCKER_NETWORK_MODE = optional_env("WREMOTELY_DOCKER_NETWORK_MODE", "host")
WREMOTELY_DBT_MART_DATASET = dbt_schema_name(
    required_env("DBT_DATASET"),
    "mart_wremotely",
    ENVIRONMENT,
)

wremotely_environment = {
    "ENVIRONMENT": ENVIRONMENT,
    "GOOGLE_APPLICATION_CREDENTIALS": WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH,
    "GOOGLE_CLOUD_PROJECT": required_env("PROJECT_ID"),
    "RAW_DATASET": required_env("RAW_DATASET"),
    "DBT_DATASET": required_env("DBT_DATASET"),
    "WREMOTELY_HANDOFF_DATASET": required_env("WREMOTELY_HANDOFF_DATASET"),
    "WREMOTELY_GCS_BUCKET": required_env("WREMOTELY_GCS_BUCKET"),
    "WREMOTELY_GCS_PREFIX": required_env("WREMOTELY_GCS_PREFIX"),
    "WREMOTELY_BIGQUERY_LOCATION": required_env("WREMOTELY_BIGQUERY_LOCATION"),
    "WREMOTELY_PUBLICATION_HOLD_POLICY": PUBLICATION_HOLD_POLICY_CONTAINER_PATH,
    "WREMOTELY_LOCAL_LLM_RUNTIME": required_env("WREMOTELY_LOCAL_LLM_RUNTIME"),
    "WREMOTELY_LOCAL_LLM_MODEL": required_env("WREMOTELY_LOCAL_LLM_MODEL"),
    "WREMOTELY_LOCAL_LLM_ENDPOINT": required_env("WREMOTELY_LOCAL_LLM_ENDPOINT"),
    "WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS": required_env(
        "WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS"
    ),
}

publication_signal_environment = {
    "GOOGLE_APPLICATION_CREDENTIALS": WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH,
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

wremotely_mounts = [
    Mount(
        source=required_host_path_env("WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS"),
        target=WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH,
        type="bind",
        read_only=True,
    ),
    Mount(
        source=required_host_path_env("WREMOTELY_ETL_ARTIFACTS_DIR"),
        target=WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
        type="bind",
    ),
    Mount(
        source=required_host_path_env("WREMOTELY_PUBLICATION_HOLD_POLICY"),
        target=PUBLICATION_HOLD_POLICY_CONTAINER_PATH,
        type="bind",
        read_only=True,
    ),
]

publication_signal_mounts = [
    Mount(
        source=required_host_path_env("WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS"),
        target=WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH,
        type="bind",
        read_only=True,
    ),
    Mount(
        source=required_host_path_env("WREMOTELY_ETL_ARTIFACTS_DIR"),
        target=WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
        type="bind",
        read_only=True,
    ),
]

dbt_mounts = [
    Mount(
        source=required_host_path_env("DBT_GOOGLE_APPLICATION_CREDENTIALS"),
        target=DBT_CREDENTIALS_CONTAINER_PATH,
        type="bind",
        read_only=True,
    )
]


def create_dbt_build_task() -> DockerOperator:
    return docker_task(
        task_id="dbt_build",
        image=DBT_IMAGE,
        command=[
            "build",
            "--project-dir",
            "data_warehouse",
            "--target",
            required_env("DBT_TARGET"),
            "--select",
            "path:seeds/wremotely",
            "path:models/staging/wremotely",
            "path:models/intermediate/wremotely",
            "path:models/marts/wremotely",
            "path:tests/wremotely",
        ],
        environment=dbt_environment,
        mounts=dbt_mounts,
        execution_timeout=DBT_TASK_EXECUTION_TIMEOUT,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_publication_hold_task(run_id: str) -> DockerOperator:
    return docker_task(
        task_id="publication_hold",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "publication-hold",
            "--run-id",
            run_id,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--dbt-dataset",
            WREMOTELY_DBT_MART_DATASET,
            "--handoff-dataset",
            required_env("WREMOTELY_HANDOFF_DATASET"),
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
            "--publication-hold-policy",
            PUBLICATION_HOLD_POLICY_CONTAINER_PATH,
            "--local-llm-runtime",
            required_env("WREMOTELY_LOCAL_LLM_RUNTIME"),
            "--local-llm-model",
            required_env("WREMOTELY_LOCAL_LLM_MODEL"),
            "--local-llm-endpoint",
            required_env("WREMOTELY_LOCAL_LLM_ENDPOINT"),
            "--local-llm-timeout-seconds",
            required_env("WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=PUBLICATION_HOLD_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_serving_snapshot_task(run_id: str) -> DockerOperator:
    return docker_task(
        task_id="publish_serving_snapshot",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "publish-serving-snapshot",
            "--run-id",
            run_id,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--dbt-dataset",
            WREMOTELY_DBT_MART_DATASET,
            "--handoff-dataset",
            required_env("WREMOTELY_HANDOFF_DATASET"),
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
            "--source-registry-input",
            APPROVED_SOURCE_REGISTRY_CONTAINER_PATH,
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_publication_signal_task(snapshot_run_id: str) -> DockerOperator:
    return docker_task(
        task_id="signal_publication",
        image=SCRIPTS_IMAGE,
        entrypoint=["python", "src/publication_signal.py"],
        command=[
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--handoff-dataset",
            required_env("WREMOTELY_HANDOFF_DATASET"),
            "--publication-table",
            SERVING_PUBLICATIONS_TABLE,
            "--publication-topic",
            required_env("WREMOTELY_PUBLICATION_TOPIC"),
            "--publication-artifact",
            (
                f"{WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH}/{snapshot_run_id}/"
                "publish_serving_snapshot/publish_serving_snapshot.json"
            ),
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
        ],
        environment=publication_signal_environment,
        mounts=publication_signal_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )


def create_publication_trigger_task(publication_run_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id="trigger_publication",
        trigger_dag_id=WREMOTELY_PUBLICATION_DAG_ID,
        trigger_run_id="publication__{{ dag.dag_id }}__{{ run_id }}",
        conf={"publication_run_id": publication_run_id},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
        retries=TASK_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        on_failure_callback=send_failure_alert,
    )
