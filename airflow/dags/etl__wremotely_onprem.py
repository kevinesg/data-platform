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
    WREMOTELY_ONPREM_FULL_REFRESH_STEPS as _WREMOTELY_ONPREM_FULL_REFRESH_STEPS,
    WREMOTELY_ONPREM_REFRESH_BOUNDARIES as _WREMOTELY_ONPREM_REFRESH_BOUNDARIES,
    WREMOTELY_ONPREM_REFRESH_STEPS,
    WREMOTELY_WAREHOUSE_POOL,
    WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
    create_onprem_wremotely_refresh_branch_task,
    create_onprem_wremotely_refresh_request_task,
    create_publication_trigger_task,
    create_wremotely_refresh_ack_task,
    create_wremotely_refresh_gate_task,
    dag_schedule,
    docker_task,
    onprem_wremotely_environment,
    onprem_wremotely_mounts,
    onprem_wremotely_private_environment,
    normalize_onprem_wremotely_refresh_request as _normalize_onprem_wremotely_refresh_request,
    optional_env,
    refreshable_onprem_etl_command,
    refreshable_onprem_crawl_command,
)

WREMOTELY_ONPREM_FULL_REFRESH_STEPS = _WREMOTELY_ONPREM_FULL_REFRESH_STEPS
WREMOTELY_ONPREM_REFRESH_BOUNDARIES = _WREMOTELY_ONPREM_REFRESH_BOUNDARIES
normalize_onprem_wremotely_refresh_request = _normalize_onprem_wremotely_refresh_request


BASE_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['base_run_id'] }}"
SOURCE_CRAWL_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['run_ids']['crawl'] }}"
SELECTION_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['run_ids']['select'] }}"
EXTRACTION_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['run_ids']['extract'] }}"
JOB_FACTS_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['run_ids']['job_facts'] }}"
CLASSIFICATION_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['run_ids']['classify'] }}"
STAGE_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['run_ids']['stage'] }}"
LANDING_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['run_ids']['land_filesystem'] }}"
CLICKHOUSE_RAW_RUN_ID = "{{ ti.xcom_pull(task_ids='read_refresh_request')['run_ids']['load_clickhouse_raw'] }}"
CLICKHOUSE_SNAPSHOT_RUN_ID = f"{BASE_RUN_ID}-clickhouse-snapshot"


with DAG(
    dag_id="etl__wremotely",
    description=(
        "Run the scheduled ClickHouse-backed wremotely pipeline through local landing, "
        "ClickHouse raw loading, dbt, and Pub/Sub signalling."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=dag_schedule(ENVIRONMENT, "ETL__WREMOTELY_SCHEDULE"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=24),
    tags=["wremotely", "elt", "on-prem", "clickhouse"],
) as dag:
    read_refresh_request = create_onprem_wremotely_refresh_request_task()
    choose_refresh_start = create_onprem_wremotely_refresh_branch_task()
    refresh_gates = {
        step: create_wremotely_refresh_gate_task(
            step,
            steps=WREMOTELY_ONPREM_REFRESH_STEPS,
        )
        for step in WREMOTELY_ONPREM_REFRESH_STEPS
    }

    crawl = docker_task(
        task_id="crawl",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_onprem_crawl_command(
            crawl_args=[
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
                "--source-hot-pass-days",
                optional_env("WREMOTELY_SOURCE_HOT_PASS_DAYS", "7"),
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
            ],
            pin_args=[
                "--step",
                "pin-crawl-generation",
                "--run-id",
                SOURCE_CRAWL_RUN_ID,
                "--output-root",
                WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            ],
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        execution_timeout=CRAWL_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_NETWORK_POOL,
        trigger_rule="none_failed_min_one_success",
    )

    select = docker_task(
        task_id="select",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_onprem_etl_command(
            "select",
            "--step",
            "select",
            "--run-id",
            SELECTION_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--source-crawl-run-id",
            SOURCE_CRAWL_RUN_ID,
            "--select-limit",
            "0",
            "--known-url-lookback-days",
            optional_env("WREMOTELY_KNOWN_URL_LOOKBACK_DAYS", "365"),
            "--skip-known-url-lookup",
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        trigger_rule="none_failed_min_one_success",
    )

    extract = docker_task(
        task_id="extract",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_onprem_etl_command(
            "extract",
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
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        execution_timeout=EXTRACT_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_NETWORK_POOL,
        trigger_rule="none_failed_min_one_success",
    )

    job_facts = docker_task(
        task_id="job_facts",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_onprem_etl_command(
            "job_facts",
            "--step",
            "job-facts",
            "--run-id",
            JOB_FACTS_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--extraction-run-id",
            EXTRACTION_RUN_ID,
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        trigger_rule="none_failed_min_one_success",
    )

    classify = docker_task(
        task_id="classify",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_onprem_etl_command(
            "classify",
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
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        trigger_rule="none_failed_min_one_success",
    )

    stage = docker_task(
        task_id="stage",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_onprem_etl_command(
            "stage",
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
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
        trigger_rule="none_failed_min_one_success",
    )

    land_filesystem = docker_task(
        task_id="land_filesystem",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_onprem_etl_command(
            "land_filesystem",
            "--step",
            "land-filesystem",
            "--run-id",
            LANDING_RUN_ID,
            "--stage-run-id",
            STAGE_RUN_ID,
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
        trigger_rule="none_failed_min_one_success",
    )

    load_clickhouse_raw = docker_task(
        task_id="load_clickhouse_raw",
        image=WREMOTELY_ETL_IMAGE,
        command=refreshable_onprem_etl_command(
            "load_clickhouse_raw",
            "--step",
            "load-clickhouse-raw",
            "--run-id",
            CLICKHOUSE_RAW_RUN_ID,
            "--landing-run-id",
            LANDING_RUN_ID,
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
        trigger_rule="none_failed_min_one_success",
    )

    trigger_publication = create_publication_trigger_task(
        CLICKHOUSE_SNAPSHOT_RUN_ID,
        trigger_rule="none_failed_min_one_success",
    )

    (
        read_refresh_request
        >> choose_refresh_start
    )
    choose_refresh_start >> [refresh_gates[step] for step in WREMOTELY_ONPREM_REFRESH_STEPS]
    for step, gate in refresh_gates.items():
        gate >> dag.get_task(step)

    (
        crawl
        >> select
        >> extract
        >> job_facts
        >> classify
        >> stage
        >> land_filesystem
        >> load_clickhouse_raw
        >> trigger_publication
    )
    trigger_publication >> create_wremotely_refresh_ack_task()
