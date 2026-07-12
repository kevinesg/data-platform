from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount

from _alerting import send_failure_alert

WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH = "/credentials/wremotely-etl-service-account.json"
DBT_CREDENTIALS_CONTAINER_PATH = "/credentials/dbt-service-account.json"
PUBLICATION_HOLD_POLICY_CONTAINER_PATH = "/run/secrets/wremotely-publication-hold-policy.md"
WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH = "/artifacts/wremotely-etl"
APPROVED_SOURCE_REGISTRY_CONTAINER_PATH = "/tmp/wremotely-approved-sources.jsonl"

BASE_RUN_ID = "{{ dag_run.logical_date.strftime('%Y%m%dT%H%M%SZ') }}-wremotely"
SOURCE_CRAWL_RUN_ID = BASE_RUN_ID
SELECTION_RUN_ID = BASE_RUN_ID
EXTRACTION_RUN_ID = f"{BASE_RUN_ID}-extract"
JOB_FACTS_RUN_ID = f"{BASE_RUN_ID}-job-facts"
CLASSIFICATION_RUN_ID = f"{BASE_RUN_ID}-classify"
PREPARE_RECHECK_RUN_ID = f"{BASE_RUN_ID}-lifecycle-prepare"
RECHECK_RUN_ID = f"{BASE_RUN_ID}-lifecycle-recheck"
RECHECK_STAGE_RUN_ID = f"{BASE_RUN_ID}-lifecycle-stage"
PUBLICATION_HOLD_RUN_ID = f"{BASE_RUN_ID}-publication-hold"
SERVING_SNAPSHOT_RUN_ID = f"{BASE_RUN_ID}-serving-snapshot"
EVALUATION_RUN_ID = f"{BASE_RUN_ID}-evaluate"
STAGE_RUN_ID = f"{BASE_RUN_ID}-stage"

DAG_RUN_TIMEOUT = timedelta(hours=24)
DEFAULT_TASK_EXECUTION_TIMEOUT = timedelta(hours=2)
CRAWL_TASK_EXECUTION_TIMEOUT = timedelta(hours=18)
EXTRACT_TASK_EXECUTION_TIMEOUT = timedelta(hours=18)
PUBLICATION_HOLD_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)
RECHECK_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)
DBT_TASK_EXECUTION_TIMEOUT = timedelta(hours=2)
TASK_RETRIES = 2
TASK_RETRY_DELAY = timedelta(minutes=5)
SERVING_PUBLICATIONS_TABLE = "wremotely__serving_publication"


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
    command: list[str],
    environment: dict[str, str],
    mounts: list[Mount],
    execution_timeout: timedelta = DEFAULT_TASK_EXECUTION_TIMEOUT,
    network_mode: str | None = None,
    entrypoint: list[str] | None = None,
) -> DockerOperator:
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
    Mount(
        source=required_host_path_env("WREMOTELY_APPROVED_SOURCES_FILE"),
        target=APPROVED_SOURCE_REGISTRY_CONTAINER_PATH,
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

with DAG(
    dag_id="etl__wremotely",
    description="Run the wremotely private extract/load chain and build its dbt serving snapshot.",
    start_date=datetime(2026, 1, 1),
    schedule=dag_schedule(ENVIRONMENT, "ETL__WREMOTELY_SCHEDULE"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAG_RUN_TIMEOUT,
    tags=["wremotely", "elt"],
) as dag:
    crawl = docker_task(
        task_id="crawl",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "crawl",
            "--run-id",
            SOURCE_CRAWL_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--source-registry-input",
            APPROVED_SOURCE_REGISTRY_CONTAINER_PATH,
            "--source-registry-input-sha256",
            required_env("WREMOTELY_APPROVED_SOURCES_SHA256"),
            "--source-crawl-limit",
            "0",
            "--source-crawl-max-job-urls",
            "0",
            "--source-crawl-worker-count",
            optional_env("WREMOTELY_SOURCE_CRAWL_WORKER_COUNT", "6"),
            "--platform-worker-count",
            optional_env("WREMOTELY_PLATFORM_WORKER_COUNT", "2"),
            "--candidate-sample-seed",
            SOURCE_CRAWL_RUN_ID,
            "--page-max-bytes",
            optional_env("WREMOTELY_PAGE_MAX_BYTES", "2097152"),
            "--domain-delay-seconds",
            optional_env("WREMOTELY_DOMAIN_DELAY_SECONDS", "1"),
            "--domain-failure-limit",
            optional_env("WREMOTELY_DOMAIN_FAILURE_LIMIT", "5"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=CRAWL_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    publish_handoff = docker_task(
        task_id="publish_handoff",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "publish-handoff",
            "--run-id",
            SOURCE_CRAWL_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--handoff-dataset",
            required_env("WREMOTELY_HANDOFF_DATASET"),
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    select = docker_task(
        task_id="select",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "select",
            "--run-id",
            SELECTION_RUN_ID,
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
            "--select-limit",
            "0",
            "--known-url-lookback-days",
            optional_env("WREMOTELY_KNOWN_URL_LOOKBACK_DAYS", "365"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    extract = docker_task(
        task_id="extract",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "extract",
            "--run-id",
            EXTRACTION_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--selection-run-id",
            SELECTION_RUN_ID,
            "--extract-limit",
            "0",
            "--extract-worker-count",
            optional_env("WREMOTELY_EXTRACT_WORKER_COUNT", "4"),
            "--platform-worker-count",
            optional_env("WREMOTELY_PLATFORM_WORKER_COUNT", "2"),
            "--candidate-selection",
            "domain-balanced",
            "--candidate-sample-seed",
            EXTRACTION_RUN_ID,
            "--page-max-bytes",
            optional_env("WREMOTELY_PAGE_MAX_BYTES", "2097152"),
            "--domain-delay-seconds",
            optional_env("WREMOTELY_DOMAIN_DELAY_SECONDS", "1"),
            "--domain-failure-limit",
            optional_env("WREMOTELY_DOMAIN_FAILURE_LIMIT", "5"),
            "--crawl4ai-fallback",
            optional_env("WREMOTELY_CRAWL4AI_FALLBACK", "auto"),
            "--crawl4ai-min-text-chars",
            optional_env("WREMOTELY_CRAWL4AI_MIN_TEXT_CHARS", "500"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        execution_timeout=EXTRACT_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

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
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

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
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
            "--recheck-limit",
            optional_env("WREMOTELY_RECHECK_LIMIT", "100"),
            "--recheck-min-age-hours",
            optional_env("WREMOTELY_RECHECK_MIN_AGE_HOURS", "72"),
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
            optional_env("WREMOTELY_RECHECK_LIMIT", "100"),
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
    )

    publication_hold = docker_task(
        task_id="publication_hold",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "publication-hold",
            "--run-id",
            PUBLICATION_HOLD_RUN_ID,
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
            "--selection-run-id",
            SELECTION_RUN_ID,
            "--extraction-run-id",
            EXTRACTION_RUN_ID,
            "--job-facts-run-id",
            JOB_FACTS_RUN_ID,
            "--classification-run-id",
            CLASSIFICATION_RUN_ID,
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    stage = docker_task(
        task_id="stage",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "stage",
            "--run-id",
            STAGE_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--selection-run-id",
            SELECTION_RUN_ID,
            "--extraction-run-id",
            EXTRACTION_RUN_ID,
            "--job-facts-run-id",
            JOB_FACTS_RUN_ID,
            "--classification-run-id",
            CLASSIFICATION_RUN_ID,
            "--stage-kind",
            "core",
            "--stage-chunk-row-count",
            optional_env("WREMOTELY_STAGE_CHUNK_ROW_COUNT", "5000"),
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
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
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

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
            "path:seeds/wremotely",
            "path:models/staging/wremotely",
            "path:models/intermediate/wremotely",
            "path:models/marts/wremotely",
            "path:tests/wremotely",
        ],
        environment=dbt_environment,
        mounts=dbt_mounts,
        execution_timeout=DBT_TASK_EXECUTION_TIMEOUT,
    )

    publish_serving_snapshot = docker_task(
        task_id="publish_serving_snapshot",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "publish-serving-snapshot",
            "--run-id",
            SERVING_SNAPSHOT_RUN_ID,
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
        ),
        environment=wremotely_environment,
        mounts=wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    signal_publication = docker_task(
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
                f"{WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH}/{SERVING_SNAPSHOT_RUN_ID}/"
                "publish_serving_snapshot/publish_serving_snapshot.json"
            ),
            "--bigquery-location",
            required_env("WREMOTELY_BIGQUERY_LOCATION"),
        ],
        environment=publication_signal_environment,
        mounts=publication_signal_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
    )

    core_load_chain = (
        crawl
        >> publish_handoff
        >> select
        >> extract
        >> job_facts
        >> classify
        >> evaluate
        >> stage
        >> upload
        >> load
    )
    lifecycle_load_chain = (
        core_load_chain
        >> prepare_recheck
        >> recheck
        >> stage_recheck
        >> upload_recheck
        >> load_recheck
    )
    (
        lifecycle_load_chain
        >> dbt_build
        >> publication_hold
        >> publish_serving_snapshot
        >> signal_publication
    )
