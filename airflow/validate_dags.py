from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from jinja2 import Environment


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate packaged Airflow DAG contracts.")
    parser.add_argument("--dag-dir", type=Path, default=Path("/opt/airflow/dags"))
    args = parser.parse_args()

    modules = import_dag_modules(args.dag_dir)
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


def validate_wremotely_dags(modules: dict[str, ModuleType]) -> None:
    ingestion = require_dag(modules, "etl__wremotely")
    lifecycle = require_dag(modules, "maintenance__wremotely_lifecycle")
    publication = require_dag(modules, "publish__wremotely_serving")
    repair = require_dag(modules, "repair__wremotely_job_urls")

    assert_task_contract(
        ingestion,
        [
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
            "trigger_publication",
        ],
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
        publication,
        [
            "dbt_build",
            "publication_hold",
            "publish_serving_snapshot",
            "signal_publication",
        ],
    )

    environment = os.environ.get("ENVIRONMENT", "dev").strip() or "dev"
    if environment == "prod" and lifecycle.schedule is None:
        raise AssertionError("prod lifecycle DAG must have a schedule")
    if environment != "prod" and lifecycle.schedule is not None:
        raise AssertionError("non-prod lifecycle DAG must be manual")
    if repair.schedule is not None:
        raise AssertionError("repair DAG must always be manual")
    if publication.schedule is not None:
        raise AssertionError("publication DAG must always be trigger-only")
    if publication.max_active_runs != 1:
        raise AssertionError("publication DAG must serialize complete publication runs")

    assert_pool(ingestion, "crawl", "wremotely_network")
    assert_pool(ingestion, "extract", "wremotely_network")
    assert_pool(lifecycle, "recheck", "wremotely_network")
    assert_pool(repair, "extract", "wremotely_network")
    for dag in (ingestion, lifecycle, repair):
        assert_pool(dag, "load" if dag is not lifecycle else "load_recheck", "wremotely_warehouse")
        assert_publication_trigger(dag)
    assert_pool(publication, "dbt_build", "wremotely_warehouse")
    assert_pool(publication, "publication_hold", "wremotely_warehouse")
    assert_pool(publication, "publish_serving_snapshot", "wremotely_warehouse")

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
    if not isinstance(crawl_command, list):
        raise AssertionError("crawl command must be an argv list")
    if command_argument(crawl_command, "--source-registry-input") != (
        "/app/source_registry/approved_sources.jsonl"
    ):
        raise AssertionError("crawl must use the image-bundled approved registry")
    if "--source-registry-input-sha256" in crawl_command:
        raise AssertionError("crawl must not depend on an external registry checksum")

    validate_lifecycle_bucket_contract(lifecycle)

    test_urls = [
        "https://company.example/jobs/one",
        "https://company.example/jobs/two?name=O'Reilly",
    ]
    select_command = repair.get_task("select").command
    if not isinstance(select_command, str):
        raise AssertionError("repair select command must be a templated string")
    rendered_command = Environment().from_string(select_command).render(
        dag_run=SimpleNamespace(logical_date=datetime(2026, 1, 2, tzinfo=UTC)),
        params={"reprocess_urls": test_urls},
    )
    repair_argv = DockerOperator.format_command(rendered_command)
    if not isinstance(repair_argv, list):
        raise AssertionError("repair select command did not render to an argv list")
    rendered_urls = [
        repair_argv[index + 1]
        for index, value in enumerate(repair_argv)
        if value == "--reprocess-url"
    ]
    if rendered_urls != test_urls:
        raise AssertionError("repair select command changed the declared URL list")


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
            "dag_run": SimpleNamespace(logical_date=logical_date),
            "params": {"recheck_limit": 0},
        }
        bucket_indexes.append(int(environment.from_string(bucket_template).render(**context)))
        if environment.from_string(limit_template).render(**context) != "0":
            raise AssertionError("scheduled preparation does not select the complete bucket")
        if environment.from_string(recheck_limit_template).render(**context) != "0":
            raise AssertionError("scheduled recheck does not accept the complete bucket")
    if set(bucket_indexes[:7]) != set(range(7)) or bucket_indexes[7] != bucket_indexes[0]:
        raise AssertionError("12-hour lifecycle runs do not cover exactly seven stable buckets")


def command_argument(command: list[str], option: str) -> str:
    try:
        return command[command.index(option) + 1]
    except (ValueError, IndexError) as exc:
        raise AssertionError(f"command is missing {option}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
