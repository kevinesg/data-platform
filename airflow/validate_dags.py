from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from docker.errors import NotFound as DockerNotFound
from jinja2 import Environment

EXPECTED_PROD_INGESTION_SCHEDULE = "0 */12 * * *"
EXPECTED_PROD_LIFECYCLE_SCHEDULE = "0 6,18 * * *"
EXPECTED_PROD_ARTIFACT_CLEANUP_SCHEDULE = "0 3 * * *"


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate packaged Airflow DAG contracts.")
    parser.add_argument("--dag-dir", type=Path, default=Path("/opt/airflow/dags"))
    args = parser.parse_args()

    modules = import_dag_modules(args.dag_dir)
    assert_all_tasks_have_execution_timeouts(modules)
    validate_wremotely_dags(modules)
    print("airflow DAG contracts OK")
    return 0


def import_dag_modules(dag_dir: Path) -> dict[str, ModuleType]:
    sys.path.insert(0, str(dag_dir))
    dag_files = sorted(
        path for path in dag_dir.glob("*.py") if path.is_file() and not path.name.startswith("_")
    )
    if not dag_files:
        raise RuntimeError("no DAG files found")

    modules = {}
    for path in dag_files:
        module_name = f"validate_dag_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"could not load spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        modules[path.stem] = module
        print(f"imported {path.name}")
    return modules


def assert_all_tasks_have_execution_timeouts(modules: dict[str, ModuleType]) -> None:
    for module_name, module in modules.items():
        dag = getattr(module, "dag", None)
        if not isinstance(dag, DAG):
            raise AssertionError(f"{module_name} does not expose a DAG named dag")
        for task in dag.tasks:
            if task.execution_timeout is None:
                raise AssertionError(f"{dag.dag_id}.{task.task_id} has no bounded timeout")


def assert_idempotent_docker_timeout_cleanup(task: DockerOperator) -> None:
    with patch.object(
        DockerOperator,
        "on_kill",
        side_effect=DockerNotFound("container already absent"),
    ):
        task.on_kill()

    sentinel = RuntimeError("non-Docker cleanup failure")
    with patch.object(DockerOperator, "on_kill", side_effect=sentinel):
        try:
            task.on_kill()
        except RuntimeError as exc:
            if exc is not sentinel:
                raise AssertionError("Docker cleanup changed an unrelated failure") from exc
        else:
            raise AssertionError("Docker cleanup swallowed an unrelated failure")


def validate_wremotely_dags(modules: dict[str, ModuleType]) -> None:
    ingestion = require_dag(modules, "etl__wremotely")
    artifact_cleanup = require_dag(modules, "maintenance__wremotely_artifacts")
    lifecycle = require_dag(modules, "maintenance__wremotely_lifecycle")
    publication = require_dag(modules, "publish__wremotely_serving")
    repair = require_dag(modules, "repair__wremotely_job_urls")
    classification_repair = require_dag(modules, "repair__wremotely_classifications")
    warehouse_classification_repair = require_dag(
        modules,
        "repair__wremotely_warehouse_classifications",
    )

    assert_ingestion_task_contract(ingestion)
    assert_task_contract(
        artifact_cleanup,
        ["cleanup"],
    )
    assert_task_contract(
        lifecycle,
        [
            "prepare_recheck",
            "recheck",
            "stage_recheck",
            "upload_recheck",
            "load_recheck",
            "trigger_publication",
        ],
    )
    assert_task_contract(
        repair,
        [
            "select",
            "extract",
            "job_facts",
            "classify",
            "evaluate",
            "stage",
            "upload",
            "load",
            "trigger_publication",
        ],
    )
    assert_task_contract(
        classification_repair,
        ["job_facts", "classify", "evaluate", "stage", "upload", "load"],
    )
    assert_task_contract(
        warehouse_classification_repair,
        ["prepare", "replay", "stage", "upload", "load"],
    )
    assert_task_contract(
        publication,
        [
            "dbt_build",
            "publication_hold",
            "publish_serving_snapshot",
            "signal_publication",
        ],
    )

    environment = os.environ.get("ENVIRONMENT", "dev").strip() or "dev"
    if environment == "prod":
        if ingestion.schedule != EXPECTED_PROD_INGESTION_SCHEDULE:
            raise AssertionError(
                "prod ingestion DAG schedule must be "
                f"{EXPECTED_PROD_INGESTION_SCHEDULE!r}, got {ingestion.schedule!r}"
            )
        if lifecycle.schedule != EXPECTED_PROD_LIFECYCLE_SCHEDULE:
            raise AssertionError(
                "prod lifecycle DAG schedule must be "
                f"{EXPECTED_PROD_LIFECYCLE_SCHEDULE!r}, got {lifecycle.schedule!r}"
            )
        if artifact_cleanup.schedule != EXPECTED_PROD_ARTIFACT_CLEANUP_SCHEDULE:
            raise AssertionError(
                "prod artifact cleanup DAG schedule must be "
                f"{EXPECTED_PROD_ARTIFACT_CLEANUP_SCHEDULE!r}, "
                f"got {artifact_cleanup.schedule!r}"
            )
    if environment != "prod" and artifact_cleanup.schedule is not None:
        raise AssertionError("non-prod artifact cleanup DAG must be manual")
    if environment != "prod" and lifecycle.schedule is not None:
        raise AssertionError("non-prod lifecycle DAG must be manual")
    if artifact_cleanup.max_active_runs != 1:
        raise AssertionError("artifact cleanup DAG must serialize cleanup runs")
    if repair.schedule is not None:
        raise AssertionError("repair DAG must always be manual")
    if classification_repair.schedule is not None:
        raise AssertionError("classification repair DAG must always be manual")
    if classification_repair.max_active_runs != 1:
        raise AssertionError("classification repair DAG must serialize historical loads")
    if warehouse_classification_repair.schedule is not None:
        raise AssertionError("warehouse classification repair DAG must always be manual")
    if warehouse_classification_repair.max_active_runs != 1:
        raise AssertionError("warehouse classification repair DAG must serialize loads")
    if publication.schedule is not None:
        raise AssertionError("publication DAG must always be trigger-only")
    if publication.max_active_runs != 1:
        raise AssertionError("publication DAG must serialize complete publication runs")

    assert_pool(ingestion, "crawl", "wremotely_network")
    assert_pool(ingestion, "extract", "wremotely_network")
    assert_pool(artifact_cleanup, "cleanup", "wremotely_warehouse")
    assert_pool(lifecycle, "recheck", "wremotely_network")
    assert_pool(repair, "extract", "wremotely_network")
    assert_pool(classification_repair, "load", "wremotely_warehouse")
    assert_pool(warehouse_classification_repair, "prepare", "wremotely_warehouse")
    assert_pool(warehouse_classification_repair, "load", "wremotely_warehouse")
    for dag in (ingestion, lifecycle, repair):
        assert_pool(dag, "load" if dag is not lifecycle else "load_recheck", "wremotely_warehouse")
        assert_publication_trigger(dag)
    assert_pool(publication, "dbt_build", "wremotely_warehouse")
    assert_pool(publication, "publication_hold", "wremotely_warehouse")
    assert_pool(publication, "publish_serving_snapshot", "wremotely_warehouse")

    dbt_build = publication.get_task("dbt_build")
    if dbt_build.execution_timeout != timedelta(minutes=30):
        raise AssertionError(
            "publish__wremotely_serving.dbt_build must have a 30-minute timeout"
        )
    if dbt_build.environment.get("DBT_JOB_CREATION_TIMEOUT_SECONDS") != os.environ[
        "WREMOTELY_DBT_JOB_CREATION_TIMEOUT_SECONDS"
    ]:
        raise AssertionError(
            "serving dbt build must pass its configured BigQuery job-creation timeout"
        )
    if dbt_build.environment.get("DBT_JOB_EXECUTION_TIMEOUT_SECONDS") != os.environ[
        "WREMOTELY_DBT_JOB_EXECUTION_TIMEOUT_SECONDS"
    ]:
        raise AssertionError(
            "serving dbt build must pass its configured BigQuery job timeout"
        )
    if dbt_build.command[-2:] != ["--exclude-resource-type", "unit_test"]:
        raise AssertionError(
            "serving production-data build must exclude development dbt unit tests"
        )
    assert_idempotent_docker_timeout_cleanup(dbt_build)

    assert_publication_hold_environment(publication)

    serving_snapshot_command = publication.get_task("publish_serving_snapshot").command
    if not isinstance(serving_snapshot_command, list):
        raise AssertionError("serving snapshot command must be an argv list")
    if command_argument(serving_snapshot_command, "--source-registry-input") != (
        "/app/source_registry/approved_sources.jsonl"
    ):
        raise AssertionError("serving snapshot must use the image-bundled approved registry")
    if "--source-registry-input-sha256" in serving_snapshot_command:
        raise AssertionError("serving snapshot must not depend on an external registry checksum")

    crawl_command = ingestion.get_task("crawl").command
    if not isinstance(crawl_command, str):
        raise AssertionError("crawl command must be a templated argv string")
    if "/app/source_registry/approved_sources.jsonl" not in crawl_command:
        raise AssertionError("crawl must use the image-bundled approved registry")
    if "--source-registry-input-sha256" in crawl_command:
        raise AssertionError("crawl must not depend on an external registry checksum")

    validate_wremotely_run_identity_contract(
        ingestion,
        artifact_cleanup,
        lifecycle,
        repair,
    )
    validate_ingestion_refresh_contract(ingestion, modules["etl__wremotely"])
    validate_lifecycle_bucket_contract(lifecycle)
    validate_artifact_cleanup_contract(artifact_cleanup)

    classification_params = {
        "extraction_run_id": "20260715T102748Z-wremotely-extract",
        "replay_label": "classification-reconciliation-v1",
    }
    rendered_commands = {
        task_id: [
            Environment().from_string(value).render(params=classification_params)
            for value in classification_repair.get_task(task_id).command
        ]
        for task_id in classification_repair.task_ids
    }
    if command_argument(rendered_commands["classify"], "--work-arrangement-mode") != "raw_only":
        raise AssertionError("classification replay must keep work arrangement inference disabled")
    if command_argument(rendered_commands["classify"], "--country-eligibility-mode") != "raw_only":
        raise AssertionError("classification replay must keep country inference disabled")
    if command_argument(rendered_commands["stage"], "--stage-kind") != "classification_replay":
        raise AssertionError("classification replay must stage only rebuilt classification artifacts")
    expected_stage_run_id = (
        "20260715T102748Z-wremotely-extract-classification-reconciliation-v1-stage"
    )
    if command_argument(rendered_commands["load"], "--run-id") != expected_stage_run_id:
        raise AssertionError("classification replay changed its stable load run ID")
    expected_replay_timeout = timedelta(hours=8)
    for task_id in classification_repair.task_ids:
        if classification_repair.get_task(task_id).execution_timeout != expected_replay_timeout:
            raise AssertionError(f"classification replay task {task_id} has no bounded timeout")

    warehouse_replay_params = {"replay_label": "classification-v13"}
    warehouse_rendered_commands = {
        task_id: [
            Environment().from_string(value).render(params=warehouse_replay_params)
            for value in warehouse_classification_repair.get_task(task_id).command
        ]
        for task_id in warehouse_classification_repair.task_ids
    }
    prepare_command = warehouse_rendered_commands["prepare"]
    if command_argument(prepare_command, "--step") != (
        "prepare-classification-replay-from-warehouse"
    ):
        raise AssertionError("warehouse replay must prepare input from raw warehouse facts")
    if command_argument(prepare_command, "--gcp-project") != os.environ["PROJECT_ID"]:
        raise AssertionError("warehouse replay preparation must use the environment project")
    if command_argument(prepare_command, "--raw-dataset") != os.environ["RAW_DATASET"]:
        raise AssertionError("warehouse replay preparation must use the environment raw dataset")
    if command_argument(prepare_command, "--bigquery-location") != os.environ[
        "WREMOTELY_BIGQUERY_LOCATION"
    ]:
        raise AssertionError("warehouse replay preparation must use the environment location")

    expected_prepare_run_id = "warehouse-classification-v13-input"
    expected_classification_run_id = "warehouse-classification-v13-classify"
    expected_warehouse_stage_run_id = "warehouse-classification-v13-stage"
    if command_argument(prepare_command, "--run-id") != expected_prepare_run_id:
        raise AssertionError("warehouse replay changed its stable preparation run ID")

    replay_command = warehouse_rendered_commands["replay"]
    if command_argument(replay_command, "--step") != "replay-classification":
        raise AssertionError("warehouse replay must use the dedicated replay step")
    if command_argument(replay_command, "--run-id") != expected_classification_run_id:
        raise AssertionError("warehouse replay changed its stable classification run ID")
    if (
        command_argument(replay_command, "--classification-replay-input-run-id")
        != expected_prepare_run_id
    ):
        raise AssertionError("warehouse replay does not consume its prepared input")
    if command_argument(replay_command, "--work-arrangement-mode") != "raw_only":
        raise AssertionError("warehouse replay must keep work arrangement inference disabled")
    if command_argument(replay_command, "--country-eligibility-mode") != "raw_only":
        raise AssertionError("warehouse replay must keep country inference disabled")

    warehouse_stage_command = warehouse_rendered_commands["stage"]
    if command_argument(warehouse_stage_command, "--stage-kind") != (
        "warehouse_classification_replay"
    ):
        raise AssertionError("warehouse replay must stage only rebuilt classification artifacts")
    if (
        command_argument(warehouse_stage_command, "--classification-run-id")
        != expected_classification_run_id
    ):
        raise AssertionError("warehouse replay stage uses the wrong classification run")
    if command_argument(warehouse_stage_command, "--run-id") != expected_warehouse_stage_run_id:
        raise AssertionError("warehouse replay changed its stable stage run ID")
    for task_id in ("upload", "load"):
        if (
            command_argument(warehouse_rendered_commands[task_id], "--run-id")
            != expected_warehouse_stage_run_id
        ):
            raise AssertionError(f"warehouse replay {task_id} uses the wrong stage run ID")
    for task_id in warehouse_classification_repair.task_ids:
        if (
            warehouse_classification_repair.get_task(task_id).execution_timeout
            != expected_replay_timeout
        ):
            raise AssertionError(f"warehouse replay task {task_id} has no bounded timeout")

def require_dag(modules: dict[str, ModuleType], module_name: str) -> DAG:
    module = modules.get(module_name)
    dag = getattr(module, "dag", None) if module else None
    if not isinstance(dag, DAG):
        raise AssertionError(f"{module_name} does not expose a DAG named dag")
    return dag


def assert_task_contract(dag: DAG, expected_chain: list[str]) -> None:
    if set(dag.task_ids) != set(expected_chain):
        raise AssertionError(f"{dag.dag_id} task set does not match its contract")
    for upstream_task_id, downstream_task_id in zip(expected_chain, expected_chain[1:]):
        downstream_ids = dag.get_task(upstream_task_id).downstream_task_ids
        if downstream_task_id not in downstream_ids:
            raise AssertionError(
                f"{dag.dag_id} is missing edge {upstream_task_id} -> {downstream_task_id}"
            )


def assert_ingestion_task_contract(ingestion: DAG) -> None:
    core_steps = [
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
    ]
    expected_task_ids = {
        "read_refresh_request",
        "choose_refresh_start",
        "acknowledge_refresh_request",
        "trigger_publication",
        *(f"refresh_start_{step}" for step in core_steps),
        *core_steps,
    }
    if set(ingestion.task_ids) != expected_task_ids:
        raise AssertionError(f"{ingestion.dag_id} task set does not match its contract")
    if ingestion.get_task("read_refresh_request").downstream_task_ids != {
        "choose_refresh_start"
    }:
        raise AssertionError("refresh request must be read before branch selection")
    if ingestion.get_task("choose_refresh_start").downstream_task_ids != {
        f"refresh_start_{step}" for step in core_steps
    }:
        raise AssertionError("refresh branch must control every EL step gate")
    for upstream_task_id, downstream_task_id in zip(core_steps, core_steps[1:]):
        if downstream_task_id not in ingestion.get_task(upstream_task_id).downstream_task_ids:
            raise AssertionError(
                f"{ingestion.dag_id} is missing edge {upstream_task_id} -> {downstream_task_id}"
            )
    for step in core_steps:
        if step not in ingestion.get_task(f"refresh_start_{step}").downstream_task_ids:
            raise AssertionError(f"refresh gate does not precede {step}")
    if "trigger_publication" not in ingestion.get_task("load").downstream_task_ids:
        raise AssertionError("ingestion load must trigger publication")
    if ingestion.get_task("trigger_publication").downstream_task_ids != {
        "acknowledge_refresh_request"
    }:
        raise AssertionError("refresh request must be acknowledged after publication")


def assert_pool(dag: DAG, task_id: str, expected_pool: str) -> None:
    actual_pool = dag.get_task(task_id).pool
    if actual_pool != expected_pool:
        raise AssertionError(
            f"{dag.dag_id}.{task_id} uses pool {actual_pool!r}, expected {expected_pool!r}"
        )


def assert_publication_trigger(dag: DAG) -> None:
    task = dag.get_task("trigger_publication")
    if not isinstance(task, TriggerDagRunOperator):
        raise AssertionError(f"{dag.dag_id}.trigger_publication is not TriggerDagRunOperator")
    if task.trigger_dag_id != "publish__wremotely_serving":
        raise AssertionError(f"{dag.dag_id} triggers the wrong publication DAG")
    if not task.reset_dag_run or not task.wait_for_completion or not task.deferrable:
        raise AssertionError(f"{dag.dag_id} publication trigger is not replay-safe and deferrable")


def assert_publication_hold_environment(publication: DAG) -> None:
    publication_hold_task = publication.get_task("publication_hold")
    publication_hold_environment = publication_hold_task.environment
    publication_hold_private_environment = publication_hold_task._private_environment
    serving_snapshot_environment = publication.get_task("publish_serving_snapshot").environment
    runtime = os.environ["WREMOTELY_LOCAL_LLM_RUNTIME"]

    if publication_hold_environment.get("WREMOTELY_LOCAL_LLM_RUNTIME") != runtime:
        raise AssertionError("publication hold does not receive its inference runtime")
    if runtime == "groq":
        expected_key = os.environ.get("GROQ_API_KEY", "")
        if (
            not expected_key
            or publication_hold_private_environment.get("GROQ_API_KEY") != expected_key
        ):
            raise AssertionError("Groq publication hold does not receive private GROQ_API_KEY")
    elif "GROQ_API_KEY" in publication_hold_private_environment:
        raise AssertionError("non-Groq publication hold must not receive GROQ_API_KEY")
    if "GROQ_API_KEY" in publication_hold_environment:
        raise AssertionError("GROQ_API_KEY must not appear in the visible task environment")

    for secret_name in (
        "GROQ_API_KEY",
        "WREMOTELY_PUBLICATION_HOLD_POLICY",
        "WREMOTELY_LOCAL_LLM_RUNTIME",
        "WREMOTELY_LOCAL_LLM_MODEL",
        "WREMOTELY_LOCAL_LLM_ENDPOINT",
        "WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS",
    ):
        if secret_name in serving_snapshot_environment:
            raise AssertionError(f"serving snapshot unexpectedly receives {secret_name}")


def validate_wremotely_run_identity_contract(
    ingestion: DAG,
    artifact_cleanup: DAG,
    lifecycle: DAG,
    repair: DAG,
) -> None:
    scheduled_logical_date = datetime(2026, 8, 3, 0, 15, tzinfo=UTC)
    manual_run_after = datetime(2026, 8, 3, 4, 6, 15, 123456, tzinfo=UTC)
    cases = [
        (
            "scheduled",
            SimpleNamespace(
                logical_date=scheduled_logical_date,
                run_after=manual_run_after,
            ),
            "20260803T001500Z",
        ),
        (
            "manual",
            SimpleNamespace(logical_date=None, run_after=manual_run_after),
            "20260803T040615123456Z",
        ),
    ]
    command_contracts = [
        (artifact_cleanup, "cleanup", "-wremotely-cleanup"),
        (lifecycle, "prepare_recheck", "-wremotely-lifecycle-prepare"),
    ]
    publication_contracts = [
        (lifecycle, "-wremotely-lifecycle"),
        (repair, "-wremotely-repair"),
    ]
    environment = Environment()

    for case_name, dag_run, expected_timestamp in cases:
        for dag, task_id, run_id_suffix in command_contracts:
            command = dag.get_task(task_id).command
            if not isinstance(command, list):
                raise AssertionError(
                    f"{dag.dag_id}.{task_id} command must be an argv list"
                )
            rendered_command = [
                environment.from_string(value).render(
                    dag_run=dag_run,
                    params={"recheck_limit": 0},
                )
                for value in command
            ]
            expected_run_id = f"{expected_timestamp}{run_id_suffix}"
            if command_argument(rendered_command, "--run-id") != expected_run_id:
                raise AssertionError(
                    f"{dag.dag_id} changed its {case_name} run identity"
                )

        for dag, run_id_suffix in publication_contracts:
            publication_conf = dag.get_task("trigger_publication").conf
            publication_run_id = publication_conf.get("publication_run_id")
            if not isinstance(publication_run_id, str):
                raise AssertionError(
                    f"{dag.dag_id} publication run ID must be templated"
                )
            rendered_publication_run_id = environment.from_string(
                publication_run_id
            ).render(dag_run=dag_run)
            if rendered_publication_run_id != f"{expected_timestamp}{run_id_suffix}":
                raise AssertionError(
                    f"{dag.dag_id} changed its {case_name} publication identity"
                )

        validate_repair_command(
            repair,
            dag_run,
            f"{expected_timestamp}-wremotely-repair",
            environment,
        )


def validate_ingestion_refresh_contract(ingestion: DAG, ingestion_module: ModuleType) -> None:
    steps = tuple(ingestion_module.WREMOTELY_REFRESH_STEPS)
    boundaries = tuple(ingestion_module.WREMOTELY_REFRESH_BOUNDARIES)
    if steps != (
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
    ):
        raise AssertionError("wremotely refresh steps changed without validator review")
    if boundaries != (
        "crawl",
        "select",
        "extract",
        "job_facts",
        "classify",
        "evaluate",
        "stage",
    ):
        raise AssertionError("wremotely refresh boundaries changed without validator review")
    for task_id in (*steps, "trigger_publication"):
        if ingestion.get_task(task_id).trigger_rule != "none_failed_min_one_success":
            raise AssertionError(
                f"{ingestion.dag_id}.{task_id} cannot cross skipped upstream tasks"
            )

    environment = Environment()
    logical_date = datetime(2026, 8, 3, 0, 15, tzinfo=UTC)
    run_after = datetime(2026, 8, 3, 4, 6, 15, 123456, tzinfo=UTC)
    dag_run = SimpleNamespace(logical_date=logical_date, run_after=run_after)

    def render_request(raw_request: object) -> tuple[dict[str, object], SimpleNamespace]:
        request = ingestion_module.normalize_wremotely_refresh_request(
            raw_request,
            logical_date=logical_date,
            run_after=run_after,
        )
        ti = SimpleNamespace(xcom_pull=lambda task_ids: request)
        return request, ti

    def render_command(task_id: str, ti: SimpleNamespace) -> list[str]:
        command = ingestion.get_task(task_id).command
        if not isinstance(command, str):
            raise AssertionError(f"{ingestion.dag_id}.{task_id} must use a templated argv string")
        rendered = environment.from_string(command).render(dag_run=dag_run, ti=ti)
        argv = DockerOperator.format_command(rendered)
        if not isinstance(argv, list):
            raise AssertionError(f"{ingestion.dag_id}.{task_id} did not render an argv list")
        return argv

    normal_request, normal_ti = render_request(None)
    if normal_request["base_run_id"] != "20260803T001500Z-wremotely":
        raise AssertionError("normal ingestion changed its scheduled run identity")
    normal_crawl = render_command("crawl", normal_ti)
    if "--full-refresh" in normal_crawl:
        raise AssertionError("normal ingestion unexpectedly requested a full refresh")
    if command_argument(normal_crawl, "--run-id") != "20260803T001500Z-wremotely":
        raise AssertionError("normal ingestion changed its crawl run identity")

    refresh_request, refresh_ti = render_request(
        {
            "refresh_id": "ats-parser-20260805",
            "from_step": "classify",
            "input_run_id": "20260803T001500Z-wremotely",
        }
    )
    if refresh_request["base_run_id"] != "refresh-ats-parser-20260805-wremotely":
        raise AssertionError("refresh identity is not stable")
    if refresh_request["run_ids"]["extract"] != "20260803T001500Z-wremotely-extract":
        raise AssertionError("downstream refresh changed retained extraction lineage")
    if "--full-refresh" in render_command("extract", refresh_ti):
        raise AssertionError("skipped extraction must not receive a refresh flag")
    classify_command = render_command("classify", refresh_ti)
    if "--full-refresh" not in classify_command:
        raise AssertionError("refresh boundary did not receive --full-refresh")
    if command_argument(classify_command, "--run-id") != (
        "refresh-ats-parser-20260805-wremotely-classify"
    ):
        raise AssertionError("refresh boundary did not receive the stable refresh run ID")
    if command_argument(classify_command, "--extraction-run-id") != (
        "20260803T001500Z-wremotely-extract"
    ):
        raise AssertionError("classification refresh did not reuse retained extraction evidence")
    if "--full-refresh" not in render_command("evaluate", refresh_ti):
        raise AssertionError("refresh descendants did not receive --full-refresh")

    for boundary in boundaries:
        boundary_index = steps.index(boundary)
        _, ti = render_request(
            {
                "refresh_id": f"boundary-{boundary_index}",
                "from_step": boundary,
                "input_run_id": "20260803T001500Z-wremotely",
            }
        )
        for step_index, task_id in enumerate(steps):
            has_refresh_flag = "--full-refresh" in render_command(task_id, ti)
            if has_refresh_flag != (step_index >= boundary_index):
                raise AssertionError(
                    f"refresh from {boundary} propagated incorrectly to {task_id}"
                )

    try:
        render_request(
            {"refresh_id": "missing-input", "from_step": "classify"}
        )
    except ValueError:
        pass
    else:
        raise AssertionError("downstream refresh without input_run_id must fail closed")


def validate_repair_command(
    repair: DAG,
    dag_run: SimpleNamespace,
    expected_run_id: str,
    environment: Environment,
) -> None:
    test_urls = [
        "https://company.example/jobs/one",
        "https://company.example/jobs/two?name=O'Reilly",
    ]
    select_command = repair.get_task("select").command
    if not isinstance(select_command, str):
        raise AssertionError("repair select command must be a templated string")
    rendered_command = environment.from_string(select_command).render(
        dag_run=dag_run,
        params={"reprocess_urls": test_urls},
    )
    repair_argv = DockerOperator.format_command(rendered_command)
    if not isinstance(repair_argv, list):
        raise AssertionError("repair select command did not render to an argv list")
    if command_argument(repair_argv, "--run-id") != expected_run_id:
        raise AssertionError("repair select command changed its run identity")
    rendered_urls = [
        repair_argv[index + 1]
        for index, value in enumerate(repair_argv)
        if value == "--reprocess-url"
    ]
    if rendered_urls != test_urls:
        raise AssertionError("repair select command changed the declared URL list")


def validate_lifecycle_bucket_contract(lifecycle: DAG) -> None:
    prepare_command = lifecycle.get_task("prepare_recheck").command
    recheck_command = lifecycle.get_task("recheck").command
    if not isinstance(prepare_command, list) or not isinstance(recheck_command, list):
        raise AssertionError("lifecycle commands must be argv lists")
    if command_argument(prepare_command, "--recheck-bucket-count") != "7":
        raise AssertionError("lifecycle preparation must use seven stable buckets")
    if command_argument(prepare_command, "--recheck-min-age-hours") != "0":
        raise AssertionError("each lifecycle bucket must include every current active row")
    if command_argument(prepare_command, "--handoff-dataset") != os.environ[
        "WREMOTELY_HANDOFF_DATASET"
    ]:
        raise AssertionError("lifecycle preparation must read the current serving handoff")
    if lifecycle.params["recheck_limit"] != 0:
        raise AssertionError("scheduled lifecycle runs must default to the complete bucket")

    environment = Environment()
    bucket_template = command_argument(prepare_command, "--recheck-bucket-index")
    limit_template = command_argument(prepare_command, "--recheck-limit")
    recheck_limit_template = command_argument(recheck_command, "--recheck-limit")
    bucket_indexes = []
    for offset in range(8):
        logical_date = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=12 * offset)
        context = {
            "dag_run": SimpleNamespace(
                logical_date=logical_date,
                run_after=logical_date + timedelta(minutes=5),
            ),
            "params": {"recheck_limit": 0},
        }
        bucket_indexes.append(int(environment.from_string(bucket_template).render(**context)))
        if environment.from_string(limit_template).render(**context) != "0":
            raise AssertionError("scheduled preparation does not select the complete bucket")
        if environment.from_string(recheck_limit_template).render(**context) != "0":
            raise AssertionError("scheduled recheck does not accept the complete bucket")
    if set(bucket_indexes[:7]) != set(range(7)) or bucket_indexes[7] != bucket_indexes[0]:
        raise AssertionError("12-hour lifecycle runs do not cover exactly seven stable buckets")

    manual_run_after = datetime(2026, 8, 3, 4, 6, 15, 123456, tzinfo=UTC)
    manual_bucket_index = int(
        environment.from_string(bucket_template).render(
            dag_run=SimpleNamespace(logical_date=None, run_after=manual_run_after),
            params={"recheck_limit": 12},
        )
    )
    expected_manual_bucket_index = int((manual_run_after.timestamp() // 43200) % 7)
    if manual_bucket_index != expected_manual_bucket_index:
        raise AssertionError("manual lifecycle run does not derive its bucket from run_after")


def validate_artifact_cleanup_contract(artifact_cleanup: DAG) -> None:
    cleanup_task = artifact_cleanup.get_task("cleanup")
    cleanup_command = cleanup_task.command
    if not isinstance(cleanup_command, list):
        raise AssertionError("artifact cleanup command must be an argv list")
    if command_argument(cleanup_command, "--step") != "cleanup":
        raise AssertionError("artifact cleanup must use the private cleanup step")
    if command_argument(cleanup_command, "--cleanup-min-age-days") != "3":
        raise AssertionError("artifact cleanup must retain three complete days")
    for required_flag in ("--cleanup-gcs", "--cleanup-apply"):
        if required_flag not in cleanup_command:
            raise AssertionError(f"artifact cleanup command is missing {required_flag}")
    if command_argument(cleanup_command, "--gcp-project") != os.environ["PROJECT_ID"]:
        raise AssertionError("artifact cleanup must use the environment GCP project")
    if command_argument(cleanup_command, "--gcs-bucket") != os.environ["WREMOTELY_GCS_BUCKET"]:
        raise AssertionError("artifact cleanup must use the environment GCS bucket")
    if command_argument(cleanup_command, "--gcs-prefix") != os.environ["WREMOTELY_GCS_PREFIX"]:
        raise AssertionError("artifact cleanup must use the environment wremotely GCS prefix")
    if cleanup_task.execution_timeout != timedelta(hours=8):
        raise AssertionError("artifact cleanup task must have its bounded extended timeout")


def command_argument(command: list[str], option: str) -> str:
    try:
        return command[command.index(option) + 1]
    except (ValueError, IndexError) as exc:
        raise AssertionError(f"command is missing {option}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
