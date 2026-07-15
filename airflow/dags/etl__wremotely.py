from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG

from _wremotely import (
    APPROVED_SOURCE_REGISTRY_CONTAINER_PATH,
    CRAWL_TASK_EXECUTION_TIMEOUT,
    ENVIRONMENT,
    EXTRACT_TASK_EXECUTION_TIMEOUT,
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

BASE_RUN_ID = "{{ dag_run.logical_date.strftime('%Y%m%dT%H%M%SZ') }}-wremotely"
SOURCE_CRAWL_RUN_ID = BASE_RUN_ID
SELECTION_RUN_ID = BASE_RUN_ID
EXTRACTION_RUN_ID = f"{BASE_RUN_ID}-extract"
JOB_FACTS_RUN_ID = f"{BASE_RUN_ID}-job-facts"
CLASSIFICATION_RUN_ID = f"{BASE_RUN_ID}-classify"
PUBLICATION_RUN_ID = BASE_RUN_ID
EVALUATION_RUN_ID = f"{BASE_RUN_ID}-evaluate"
STAGE_RUN_ID = f"{BASE_RUN_ID}-stage"

DAG_RUN_TIMEOUT = timedelta(hours=24)
with DAG(
    dag_id="etl__wremotely",
    description="Load newly processed wremotely jobs and trigger serialized serving publication.",
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
        pool=WREMOTELY_NETWORK_POOL,
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
        pool=WREMOTELY_WAREHOUSE_POOL,
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
        pool=WREMOTELY_NETWORK_POOL,
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
        pool=WREMOTELY_WAREHOUSE_POOL,
    )

    trigger_publication = create_publication_trigger_task(PUBLICATION_RUN_ID)

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
    core_load_chain >> trigger_publication
