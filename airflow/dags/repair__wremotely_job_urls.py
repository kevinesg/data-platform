from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Param

from _wremotely import (
    ENVIRONMENT,
    EXTRACT_TASK_EXECUTION_TIMEOUT,
    WREMOTELY_DOCKER_NETWORK_MODE,
    WREMOTELY_ETL_IMAGE,
    WREMOTELY_NETWORK_POOL,
    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
    WREMOTELY_WAREHOUSE_POOL,
    create_publication_trigger_task,
    docker_task,
    etl_command,
    optional_env,
    required_env,
    wremotely_environment,
    wremotely_mounts,
)

BASE_RUN_ID = "{{ dag_run.logical_date.strftime('%Y%m%dT%H%M%SZ') }}-wremotely-repair"
SELECTION_RUN_ID = BASE_RUN_ID
EXTRACTION_RUN_ID = f"{BASE_RUN_ID}-extract"
JOB_FACTS_RUN_ID = f"{BASE_RUN_ID}-job-facts"
CLASSIFICATION_RUN_ID = f"{BASE_RUN_ID}-classify"
EVALUATION_RUN_ID = f"{BASE_RUN_ID}-evaluate"
STAGE_RUN_ID = f"{BASE_RUN_ID}-stage"
PUBLICATION_RUN_ID = BASE_RUN_ID

DAG_RUN_TIMEOUT = timedelta(hours=24)

# DockerOperator parses a rendered command that starts with "[" as a literal argv
# list. JSON-quoted Param values therefore remain individual URL arguments.
REPAIR_SELECT_COMMAND = """[
    "--step", "select",
    "--run-id", "{{ dag_run.logical_date.strftime('%Y%m%dT%H%M%SZ') }}-wremotely-repair",
    "--output-root", "/artifacts/wremotely-etl",
    "--gcp-project", "PROJECT_ID_VALUE",
    "--raw-dataset", "RAW_DATASET_VALUE",
    "--handoff-dataset", "HANDOFF_DATASET_VALUE",
    "--bigquery-location", "BIGQUERY_LOCATION_VALUE",
    "--select-limit", "100",
    "--known-url-lookback-days", "KNOWN_URL_LOOKBACK_VALUE",
{% for url in params.reprocess_urls %}
    "--reprocess-url", {{ url | tojson }},
{% endfor %}
]""".replace("PROJECT_ID_VALUE", required_env("PROJECT_ID")).replace(
    "RAW_DATASET_VALUE", required_env("RAW_DATASET")
).replace(
    "HANDOFF_DATASET_VALUE", required_env("WREMOTELY_HANDOFF_DATASET")
).replace(
    "BIGQUERY_LOCATION_VALUE", required_env("WREMOTELY_BIGQUERY_LOCATION")
).replace(
    "KNOWN_URL_LOOKBACK_VALUE", optional_env("WREMOTELY_KNOWN_URL_LOOKBACK_DAYS", "365")
)

with DAG(
    dag_id="repair__wremotely_job_urls",
    description="Reprocess a bounded explicit set of current wremotely job URLs.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAG_RUN_TIMEOUT,
    params={
        "reprocess_urls": Param(
            type="array",
            title="Job URLs to reprocess",
            description=(
                "Enter one exact URL per line. Every URL must exist in the current source-crawl "
                "handoff table."
            ),
            items={"type": "string", "format": "uri", "minLength": 1, "maxLength": 4096},
            minItems=1,
            maxItems=100,
            uniqueItems=True,
        )
    },
    tags=["wremotely", "repair", "manual"],
) as dag:
    select = docker_task(
        task_id="select",
        image=WREMOTELY_ETL_IMAGE,
        command=REPAIR_SELECT_COMMAND,
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
            "100",
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

    (
        select
        >> extract
        >> job_facts
        >> classify
        >> evaluate
        >> stage
        >> upload
        >> load
        >> trigger_publication
    )
