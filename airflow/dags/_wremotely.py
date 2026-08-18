from __future__ import annotations

import json
import os
import re
from datetime import timedelta
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable, get_current_context
from docker.errors import NotFound as DockerNotFound
from docker.types import Mount

from _alerting import send_failure_alert

WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH = "/credentials/wremotely-etl-service-account.json"
DBT_CREDENTIALS_CONTAINER_PATH = "/credentials/dbt-service-account.json"
PUBLICATION_HOLD_POLICY_CONTAINER_PATH = "/run/secrets/wremotely-publication-hold-policy.md"
WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH = "/artifacts/wremotely-etl"
WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH = "/warehouse/workmichi"
WREMOTELY_DBT_RUN_RESULTS_CONTAINER_PATH = (
    f"{WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH}/baseline/dbt-build/run_results.json"
)
WREMOTELY_DBT_FAILED_TARGET_ROOT_CONTAINER_PATH = (
    f"{WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH}/dbt-failures"
)
APPROVED_SOURCE_REGISTRY_CONTAINER_PATH = "/app/source_registry/approved_sources.jsonl"

DEFAULT_TASK_EXECUTION_TIMEOUT = timedelta(hours=2)
CRAWL_TASK_EXECUTION_TIMEOUT = timedelta(hours=18)
EXTRACT_TASK_EXECUTION_TIMEOUT = timedelta(hours=18)
PUBLICATION_HOLD_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)
RECHECK_TASK_EXECUTION_TIMEOUT = timedelta(hours=8)
SERVING_DBT_TASK_EXECUTION_TIMEOUT = timedelta(minutes=30)
ONPREM_CLICKHOUSE_DBT_TASK_EXECUTION_TIMEOUT = timedelta(hours=4)
PUBLICATION_TRIGGER_TASK_EXECUTION_TIMEOUT = timedelta(hours=12)
TASK_RETRIES = 2
TASK_RETRY_DELAY = timedelta(minutes=5)
SERVING_PUBLICATIONS_TABLE = "wremotely__serving_publication"
WREMOTELY_NETWORK_POOL = "wremotely_network"
WREMOTELY_WAREHOUSE_POOL = "wremotely_warehouse"
WREMOTELY_PUBLICATION_DAG_ID = "publish__wremotely_serving"
WREMOTELY_REFRESH_REQUEST_VARIABLE = "wremotely_refresh_request"
WREMOTELY_REFRESH_REQUEST_TASK_ID = "read_refresh_request"
WREMOTELY_REFRESH_BRANCH_TASK_ID = "choose_refresh_start"
WREMOTELY_REFRESH_ACK_TASK_ID = "acknowledge_refresh_request"
WREMOTELY_REFRESH_GATE_PREFIX = "refresh_start_"
WREMOTELY_REFRESH_STEPS = (
    "crawl",
    "publish_handoff",
    "select",
    "extract",
    "job_facts",
    "classify",
    "evaluate",
    "stage",
    "upload",
    "load",
)
WREMOTELY_REFRESH_BOUNDARIES = (
    "crawl",
    "select",
    "extract",
    "job_facts",
    "classify",
    "evaluate",
    "stage",
)
_REFRESH_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
WREMOTELY_DAG_RUN_TIMESTAMP = (
    "{{ dag_run.logical_date.strftime('%Y%m%dT%H%M%SZ') "
    "if dag_run.logical_date "
    "else dag_run.run_after.strftime('%Y%m%dT%H%M%S%fZ') }}"
)


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


def optional_host_path_env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip() or default
    if not value.startswith("/"):
        raise RuntimeError(f"{name} must be an absolute host path")
    return value


class WremotelyDockerOperator(DockerOperator):
    """Keep a task timeout authoritative when Docker already removed its container."""

    def on_kill(self) -> None:
        try:
            super().on_kill()
        except DockerNotFound:
            self.log.info("Docker container was already absent during task shutdown")


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
    private_environment: dict[str, str] | None = None,
    execution_timeout: timedelta = DEFAULT_TASK_EXECUTION_TIMEOUT,
    network_mode: str | None = None,
    entrypoint: list[str] | None = None,
    pool: str | None = None,
    trigger_rule: str | None = None,
) -> WremotelyDockerOperator:
    operator_options = {"pool": pool} if pool else {}
    return WremotelyDockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment,
        private_environment=private_environment,
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
        trigger_rule=trigger_rule or "all_success",
        on_failure_callback=send_failure_alert,
        **operator_options,
    )


def etl_command(*args: str) -> list[str]:
    return list(args)


def refreshable_etl_command(step: str, *args: str) -> str:
    """Render an argv list with a refresh flag only for the affected descendants."""
    if step not in WREMOTELY_REFRESH_STEPS:
        raise ValueError(f"unsupported wremotely refresh step: {step}")
    serialized = json.dumps(list(args))
    condition = (
        f"'{step}' in "
        "(ti.xcom_pull(task_ids='read_refresh_request') or {}).get('full_refresh_steps', [])"
    )
    return (
        f"{serialized[:-1]}"
        f"{{% if {condition} %}}, \"--full-refresh\"{{% endif %}}]"
    )


def _run_timestamp(logical_date: Any, run_after: Any) -> str:
    if logical_date is not None:
        return logical_date.strftime("%Y%m%dT%H%M%SZ")
    if run_after is None:
        raise ValueError("wremotely DAG run has neither logical_date nor run_after")
    return run_after.strftime("%Y%m%dT%H%M%S%fZ")


def _validate_run_id(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _RUN_ID_PATTERN.fullmatch(value):
        raise ValueError(
            f"wremotely refresh request {field_name} must match "
            "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
        )
    return value


def normalize_wremotely_refresh_request(
    raw_request: Any,
    *,
    logical_date: Any,
    run_after: Any,
) -> dict[str, Any]:
    """Validate one durable request and derive all stable run identities."""
    timestamp = _run_timestamp(logical_date, run_after)
    if raw_request in (None, "", {}):
        base_run_id = f"{timestamp}-wremotely"
        return {
            "refresh": False,
            "refresh_id": None,
            "from_step": "crawl",
            "input_base_run_id": base_run_id,
            "base_run_id": base_run_id,
            "full_refresh_steps": [],
            "run_ids": {
                step: f"{base_run_id}{_run_id_suffix(step)}"
                for step in WREMOTELY_REFRESH_STEPS
            },
            "declaration": None,
        }
    if not isinstance(raw_request, dict):
        raise ValueError(
            f"{WREMOTELY_REFRESH_REQUEST_VARIABLE} must contain a JSON object"
        )
    if set(raw_request) - {"refresh_id", "from_step", "input_run_id"}:
        raise ValueError(
            f"{WREMOTELY_REFRESH_REQUEST_VARIABLE} contains unsupported fields"
        )
    refresh_id = raw_request.get("refresh_id")
    if not isinstance(refresh_id, str) or not _REFRESH_ID_PATTERN.fullmatch(refresh_id):
        raise ValueError(
            "wremotely refresh request refresh_id must match "
            "^[a-z0-9][a-z0-9._-]{0,63}$"
        )
    from_step = raw_request.get("from_step")
    if from_step not in WREMOTELY_REFRESH_BOUNDARIES:
        raise ValueError(
            "wremotely refresh request from_step must be one of "
            + ", ".join(WREMOTELY_REFRESH_BOUNDARIES)
        )
    step_index = WREMOTELY_REFRESH_STEPS.index(from_step)
    input_base_run_id = raw_request.get("input_run_id")
    if step_index > 0:
        input_base_run_id = _validate_run_id(input_base_run_id, "input_run_id")
    elif input_base_run_id is not None:
        input_base_run_id = _validate_run_id(input_base_run_id, "input_run_id")
    else:
        input_base_run_id = f"{timestamp}-wremotely"

    base_run_id = f"refresh-{refresh_id}-wremotely"
    full_refresh_steps = list(WREMOTELY_REFRESH_STEPS[step_index:])
    run_ids = {
        step: (
            f"{input_base_run_id}{_run_id_suffix(step)}"
            if index < step_index
            else f"{base_run_id}{_run_id_suffix(step)}"
        )
        for index, step in enumerate(WREMOTELY_REFRESH_STEPS)
    }
    return {
        "refresh": True,
        "refresh_id": refresh_id,
        "from_step": from_step,
        "input_base_run_id": input_base_run_id,
        "base_run_id": base_run_id,
        "full_refresh_steps": full_refresh_steps,
        "run_ids": run_ids,
        "declaration": dict(raw_request),
    }


def _run_id_suffix(step: str) -> str:
    return {
        "crawl": "",
        "publish_handoff": "",
        "select": "",
        "extract": "-extract",
        "job_facts": "-job-facts",
        "classify": "-classify",
        "evaluate": "-evaluate",
        "stage": "-stage",
        "upload": "-stage",
        "load": "-stage",
    }[step]


def read_wremotely_refresh_request() -> dict[str, Any]:
    context = get_current_context()
    dag_run = context["dag_run"]
    raw_request = Variable.get(
        WREMOTELY_REFRESH_REQUEST_VARIABLE,
        default=None,
        deserialize_json=True,
    )
    return normalize_wremotely_refresh_request(
        raw_request,
        logical_date=dag_run.logical_date,
        run_after=dag_run.run_after,
    )


def choose_wremotely_refresh_start() -> str:
    context = get_current_context()
    request = context["ti"].xcom_pull(task_ids=WREMOTELY_REFRESH_REQUEST_TASK_ID)
    if (
        not isinstance(request, dict)
        or request.get("from_step") not in WREMOTELY_REFRESH_BOUNDARIES
    ):
        raise RuntimeError("refresh request task did not return a validated request")
    return f"{WREMOTELY_REFRESH_GATE_PREFIX}{request['from_step']}"


def acknowledge_wremotely_refresh_request() -> None:
    context = get_current_context()
    request = context["ti"].xcom_pull(task_ids=WREMOTELY_REFRESH_REQUEST_TASK_ID)
    if not isinstance(request, dict) or not request.get("refresh"):
        return
    current = Variable.get(
        WREMOTELY_REFRESH_REQUEST_VARIABLE,
        default=None,
        deserialize_json=True,
    )
    if current != request.get("declaration"):
        raise RuntimeError(
            "wremotely refresh request changed during the run; leaving the newer request "
            "unacknowledged"
        )
    Variable.delete(WREMOTELY_REFRESH_REQUEST_VARIABLE)


def create_wremotely_refresh_request_task() -> PythonOperator:
    return PythonOperator(
        task_id=WREMOTELY_REFRESH_REQUEST_TASK_ID,
        python_callable=read_wremotely_refresh_request,
        execution_timeout=timedelta(minutes=5),
        retries=TASK_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        on_failure_callback=send_failure_alert,
    )


def create_wremotely_refresh_branch_task() -> BranchPythonOperator:
    return BranchPythonOperator(
        task_id=WREMOTELY_REFRESH_BRANCH_TASK_ID,
        python_callable=choose_wremotely_refresh_start,
        execution_timeout=timedelta(minutes=5),
        retries=TASK_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        on_failure_callback=send_failure_alert,
    )


def create_wremotely_refresh_ack_task() -> PythonOperator:
    return PythonOperator(
        task_id=WREMOTELY_REFRESH_ACK_TASK_ID,
        python_callable=acknowledge_wremotely_refresh_request,
        execution_timeout=timedelta(minutes=5),
        retries=TASK_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        on_failure_callback=send_failure_alert,
    )


def create_wremotely_refresh_gate_task(step: str) -> EmptyOperator:
    if step not in WREMOTELY_REFRESH_STEPS:
        raise ValueError(f"unsupported wremotely refresh step: {step}")
    return EmptyOperator(
        task_id=f"{WREMOTELY_REFRESH_GATE_PREFIX}{step}",
        execution_timeout=timedelta(minutes=5),
    )


ENVIRONMENT = optional_env("ENVIRONMENT", "dev")
WREMOTELY_ETL_IMAGE = required_env("DATA_PLATFORM_WREMOTELY_ETL_IMAGE")
SCRIPTS_IMAGE = required_env("DATA_PLATFORM_SCRIPTS_IMAGE")
DBT_IMAGE = required_env("DATA_PLATFORM_DBT_IMAGE")
CLICKHOUSE_DBT_IMAGE = optional_env(
    "DATA_PLATFORM_WREMOTELY_CLICKHOUSE_DBT_IMAGE",
    "data-platform-wremotely-clickhouse-dbt:dev",
)
WREMOTELY_DOCKER_NETWORK_MODE = optional_env("WREMOTELY_DOCKER_NETWORK_MODE", "host")
WREMOTELY_LOCAL_LLM_RUNTIME = required_env("WREMOTELY_LOCAL_LLM_RUNTIME")
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
}

publication_hold_environment = {
    **wremotely_environment,
    "WREMOTELY_PUBLICATION_HOLD_POLICY": PUBLICATION_HOLD_POLICY_CONTAINER_PATH,
    "WREMOTELY_LOCAL_LLM_RUNTIME": WREMOTELY_LOCAL_LLM_RUNTIME,
    "WREMOTELY_LOCAL_LLM_MODEL": required_env("WREMOTELY_LOCAL_LLM_MODEL"),
    "WREMOTELY_LOCAL_LLM_ENDPOINT": required_env("WREMOTELY_LOCAL_LLM_ENDPOINT"),
    "WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS": required_env(
        "WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS"
    ),
}
publication_hold_private_environment = {}
if WREMOTELY_LOCAL_LLM_RUNTIME == "groq":
    publication_hold_private_environment["GROQ_API_KEY"] = required_env("GROQ_API_KEY")

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
    "DBT_JOB_CREATION_TIMEOUT_SECONDS": required_env(
        "WREMOTELY_DBT_JOB_CREATION_TIMEOUT_SECONDS"
    ),
    "DBT_JOB_EXECUTION_TIMEOUT_SECONDS": required_env(
        "WREMOTELY_DBT_JOB_EXECUTION_TIMEOUT_SECONDS"
    ),
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
]

publication_hold_mounts = [
    *wremotely_mounts,
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
    ),
    Mount(
        source=required_host_path_env("WREMOTELY_ETL_ARTIFACTS_DIR"),
        target=WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
        type="bind",
    ),
]

clickhouse_dbt_environment = {
    "WREMOTELY_CLICKHOUSE_DATABASE": optional_env(
        "WREMOTELY_CLICKHOUSE_DATABASE", "wremotely_dev"
    ),
    "WREMOTELY_CLICKHOUSE_HOST": optional_env(
        "WREMOTELY_CLICKHOUSE_HOST", "host.docker.internal"
    ),
    "WREMOTELY_CLICKHOUSE_PORT": optional_env("WREMOTELY_CLICKHOUSE_PORT", "8123"),
    "WREMOTELY_CLICKHOUSE_USER": optional_env(
        "WREMOTELY_CLICKHOUSE_USER", "wremotely_dev"
    ),
    "WREMOTELY_LIFECYCLE_MIN_POSTING_AGE_DAYS": optional_env(
        "WREMOTELY_LIFECYCLE_MIN_POSTING_AGE_DAYS", "21"
    ),
}
clickhouse_dbt_private_environment = {}
if os.environ.get("WREMOTELY_CLICKHOUSE_PASSWORD", "").strip():
    clickhouse_dbt_private_environment["WREMOTELY_CLICKHOUSE_PASSWORD"] = os.environ[
        "WREMOTELY_CLICKHOUSE_PASSWORD"
    ]

clickhouse_dbt_mounts = [
    Mount(
        source=required_host_path_env("WREMOTELY_ETL_ARTIFACTS_DIR"),
        target=WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
        type="bind",
    ),
]

onprem_wremotely_environment = {
    "ENVIRONMENT": ENVIRONMENT,
    "WREMOTELY_WAREHOUSE_ROOT": WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
    "WREMOTELY_CLICKHOUSE_URL": optional_env(
        "WREMOTELY_CLICKHOUSE_URL", "http://127.0.0.1:8123"
    ),
    "WREMOTELY_CLICKHOUSE_DATABASE": optional_env(
        "WREMOTELY_CLICKHOUSE_DATABASE", "wremotely_dev"
    ),
    "WREMOTELY_CLICKHOUSE_TABLE_PREFIX": optional_env(
        "WREMOTELY_CLICKHOUSE_TABLE_PREFIX", "wremotely__landing"
    ),
    "WREMOTELY_CLICKHOUSE_RAW_TABLE_PREFIX": optional_env(
        "WREMOTELY_CLICKHOUSE_RAW_TABLE_PREFIX", "wremotely__"
    ),
    "WREMOTELY_CLICKHOUSE_USER": optional_env(
        "WREMOTELY_CLICKHOUSE_USER", "wremotely_dev"
    ),
    "WREMOTELY_CLICKHOUSE_TIMEOUT_SECONDS": optional_env(
        "WREMOTELY_CLICKHOUSE_TIMEOUT_SECONDS", "60"
    ),
}
onprem_wremotely_private_environment = {}
if os.environ.get("WREMOTELY_CLICKHOUSE_PASSWORD", "").strip():
    onprem_wremotely_private_environment["WREMOTELY_CLICKHOUSE_PASSWORD"] = os.environ[
        "WREMOTELY_CLICKHOUSE_PASSWORD"
    ]

onprem_wremotely_mounts = [
    Mount(
        source=required_host_path_env("WREMOTELY_ETL_ARTIFACTS_DIR"),
        target=WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
        type="bind",
    ),
    Mount(
        source=optional_host_path_env(
            "WREMOTELY_WAREHOUSE_ROOT", "/srv/data/warehouse/workmichi"
        ),
        target=WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
        type="bind",
    ),
]

onprem_publication_signal_environment = {
    "GOOGLE_APPLICATION_CREDENTIALS": WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH,
}

onprem_publication_signal_mounts = [
    Mount(
        source=required_host_path_env("WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS"),
        target=WREMOTELY_ETL_CREDENTIALS_CONTAINER_PATH,
        type="bind",
        read_only=True,
    ),
    Mount(
        source=optional_host_path_env(
            "WREMOTELY_WAREHOUSE_ROOT", "/srv/data/warehouse/workmichi"
        ),
        target=WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
        type="bind",
        read_only=True,
    ),
]

onprem_clickhouse_dbt_environment = {
    "WREMOTELY_CLICKHOUSE_DATABASE": optional_env(
        "WREMOTELY_CLICKHOUSE_DATABASE", "wremotely_dev"
    ),
    "WREMOTELY_CLICKHOUSE_HOST": optional_env("WREMOTELY_CLICKHOUSE_HOST", "127.0.0.1"),
    "WREMOTELY_CLICKHOUSE_PORT": optional_env("WREMOTELY_CLICKHOUSE_PORT", "8123"),
    "WREMOTELY_CLICKHOUSE_USER": optional_env(
        "WREMOTELY_CLICKHOUSE_USER", "wremotely_dev"
    ),
}
onprem_clickhouse_dbt_private_environment = clickhouse_dbt_private_environment


def create_dbt_build_task(*, task_id: str = "dbt_build") -> DockerOperator:
    return docker_task(
        task_id=task_id,
        image=DBT_IMAGE,
        command=[
            "--output",
            WREMOTELY_DBT_RUN_RESULTS_CONTAINER_PATH,
            "--failed-target-root",
            WREMOTELY_DBT_FAILED_TARGET_ROOT_CONTAINER_PATH,
            "--",
            "build",
            "--project-dir",
            "wremotely",
            "--target",
            required_env("DBT_TARGET"),
            "--exclude-resource-type",
            "unit_test",
        ],
        environment=dbt_environment,
        mounts=dbt_mounts,
        entrypoint=["python", "/app/run_and_retain_results.py"],
        execution_timeout=SERVING_DBT_TASK_EXECUTION_TIMEOUT,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_clickhouse_dbt_build_task() -> DockerOperator:
    return docker_task(
        task_id="dbt_build",
        image=CLICKHOUSE_DBT_IMAGE,
        command=[
            "build",
            "--project-dir",
            "/app",
            "--profiles-dir",
            "/app/profiles",
            "--target-path",
            f"{WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH}/clickhouse-dbt/{{{{ dag_run.run_id }}}}",
            "--exclude-resource-type",
            "unit_test",
        ],
        environment=clickhouse_dbt_environment,
        private_environment=clickhouse_dbt_private_environment,
        mounts=clickhouse_dbt_mounts,
        execution_timeout=SERVING_DBT_TASK_EXECUTION_TIMEOUT,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_onprem_clickhouse_dbt_build_task() -> DockerOperator:
    return docker_task(
        task_id="dbt_build",
        image=CLICKHOUSE_DBT_IMAGE,
        command=[
            "build",
            "--project-dir",
            "/app",
            "--profiles-dir",
            "/app/profiles",
            "--target-path",
            f"{WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH}/clickhouse-dbt/{{{{ dag_run.run_id }}}}",
            "--exclude-resource-type",
            "unit_test",
        ],
        environment=onprem_clickhouse_dbt_environment,
        private_environment=onprem_clickhouse_dbt_private_environment,
        mounts=clickhouse_dbt_mounts,
        execution_timeout=ONPREM_CLICKHOUSE_DBT_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_onprem_clickhouse_publication_snapshot_task(run_id: str) -> DockerOperator:
    return docker_task(
        task_id="publish_clickhouse_snapshot",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "publish-clickhouse-snapshot",
            "--run-id",
            run_id,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--source-registry-input",
            APPROVED_SOURCE_REGISTRY_CONTAINER_PATH,
            "--warehouse-root",
            WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
            "--publication-snapshot-batch-row-count",
            optional_env("WREMOTELY_PUBLICATION_SNAPSHOT_BATCH_ROW_COUNT", "1000"),
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_onprem_clickhouse_publication_signal_task(
    snapshot_run_id: str,
    *,
    task_id: str = "signal_publication",
) -> DockerOperator:
    return docker_task(
        task_id=task_id,
        image=SCRIPTS_IMAGE,
        entrypoint=["python", "src/clickhouse_publication_signal.py"],
        command=[
            "--gcp-project",
            required_env("PROJECT_ID"),
            "--publication-topic",
            required_env("WREMOTELY_PUBLICATION_TOPIC"),
            "--publication-artifact",
            (
                f"{WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH}/control/"
                f"clickhouse-publication/{snapshot_run_id}/manifest.json"
            ),
            "--publish-timeout-seconds",
            optional_env("WREMOTELY_PUBLICATION_SIGNAL_TIMEOUT_SECONDS", "60"),
        ],
        environment=onprem_publication_signal_environment,
        mounts=onprem_publication_signal_mounts,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_publication_hold_task(
    run_id: str,
    *,
    task_id: str = "publication_hold",
) -> DockerOperator:
    return docker_task(
        task_id=task_id,
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
            WREMOTELY_LOCAL_LLM_RUNTIME,
            "--local-llm-model",
            required_env("WREMOTELY_LOCAL_LLM_MODEL"),
            "--local-llm-endpoint",
            required_env("WREMOTELY_LOCAL_LLM_ENDPOINT"),
            "--local-llm-timeout-seconds",
            required_env("WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS"),
        ),
        environment=publication_hold_environment,
        mounts=publication_hold_mounts,
        private_environment=publication_hold_private_environment,
        execution_timeout=PUBLICATION_HOLD_TASK_EXECUTION_TIMEOUT,
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )


def create_serving_snapshot_task(
    run_id: str,
    *,
    task_id: str = "publish_serving_snapshot",
) -> DockerOperator:
    return docker_task(
        task_id=task_id,
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


def create_publication_signal_task(
    snapshot_run_id: str,
    *,
    task_id: str = "signal_publication",
) -> DockerOperator:
    return docker_task(
        task_id=task_id,
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


def create_publication_trigger_task(
    publication_run_id: str,
    *,
    publication_mode: str = "legacy",
    trigger_rule: str = "all_success",
) -> TriggerDagRunOperator:
    if publication_mode not in {"legacy", "clickhouse"}:
        raise ValueError(f"unsupported publication mode: {publication_mode}")
    return TriggerDagRunOperator(
        task_id="trigger_publication",
        trigger_dag_id=WREMOTELY_PUBLICATION_DAG_ID,
        trigger_run_id="publication__{{ dag.dag_id }}__{{ run_id }}",
        conf={"publication_run_id": publication_run_id, "publication_mode": publication_mode},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
        trigger_rule=trigger_rule,
        execution_timeout=PUBLICATION_TRIGGER_TASK_EXECUTION_TIMEOUT,
        retries=TASK_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        on_failure_callback=send_failure_alert,
    )
