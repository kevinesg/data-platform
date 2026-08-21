"""Airflow-side freshness checks for the on-prem Wremotely path."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime
from typing import Any


def check_airflow_freshness() -> None:
    checks = (
        (
            "etl__wremotely",
            "WREMOTELY_MONITOR_MAX_ETL_SUCCESS_AGE_MINUTES",
            1_500,
        ),
        (
            "maintenance__wremotely_lifecycle",
            "WREMOTELY_MONITOR_MAX_LIFECYCLE_SUCCESS_AGE_MINUTES",
            1_500,
        ),
        (
            "maintenance__wremotely_artifacts",
            "WREMOTELY_MONITOR_MAX_ARTIFACT_CLEANUP_SUCCESS_AGE_MINUTES",
            3_000,
        ),
    )
    for dag_id, environment_name, default_minutes in checks:
        maximum_age_minutes = int(os.getenv(environment_name, str(default_minutes)))
        latest = _latest_successful_run(dag_id)
        completed_at = _run_timestamp(latest)
        age_minutes = (datetime.now(UTC) - completed_at).total_seconds() / 60
        if age_minutes > maximum_age_minutes:
            raise RuntimeError(
                f"{dag_id} latest successful run is {age_minutes:.1f} minutes old; "
                f"maximum is {maximum_age_minutes}"
            )
        print(
            f"airflow_freshness dag_id={dag_id} run_id={latest.get('run_id')} "
            f"age_minutes={age_minutes:.1f}"
        )


def _latest_successful_run(dag_id: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["airflow", "dags", "list-runs", dag_id, "--state", "success", "--output", "json"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"could not inspect {dag_id} successful runs: {detail}")
    rows = _parse_json_array(completed.stdout)
    if not rows:
        raise RuntimeError(f"{dag_id} has no successful runs")
    rows.sort(key=_run_timestamp)
    return rows[-1]


def _parse_json_array(output: str) -> list[dict[str, Any]]:
    lines = output.splitlines()
    start = next(
        (index for index, line in enumerate(lines) if line.lstrip().startswith("[")),
        None,
    )
    if start is None:
        raise RuntimeError("Airflow list-runs output did not contain a JSON array")
    value = json.loads("\n".join(lines[start:]))
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise RuntimeError("Airflow list-runs output must be a JSON object array")
    return value


def _run_timestamp(row: dict[str, Any]) -> datetime:
    for field in ("end_date", "start_date", "logical_date", "run_after"):
        value = row.get(field)
        if isinstance(value, str) and value.strip():
            normalized = value.strip().replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            return parsed.astimezone(UTC)
    raise RuntimeError(f"Airflow run has no usable timestamp: {row.get('run_id')}")
