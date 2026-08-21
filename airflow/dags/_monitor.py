"""Airflow-side freshness checks for the on-prem Wremotely path."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from airflow.models import DagRun
from airflow.utils.state import DagRunState


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
    runs = DagRun.find(dag_id=dag_id, state=DagRunState.SUCCESS)
    if not runs:
        raise RuntimeError(f"{dag_id} has no successful runs")

    latest = max(
        runs,
        key=lambda run: _run_timestamp(
            {
                "end_date": run.end_date,
                "start_date": run.start_date,
                "logical_date": run.logical_date,
                "run_after": run.run_after,
            }
        ),
    )
    return {
        "run_id": latest.run_id,
        "end_date": latest.end_date,
        "start_date": latest.start_date,
        "logical_date": latest.logical_date,
        "run_after": latest.run_after,
    }


def _run_timestamp(row: dict[str, Any]) -> datetime:
    for field in ("end_date", "start_date", "logical_date", "run_after"):
        value = row.get(field)
        if isinstance(value, datetime):
            return value.astimezone(UTC)
        if isinstance(value, str) and value.strip():
            normalized = value.strip().replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            return parsed.astimezone(UTC)
    raise RuntimeError(f"Airflow run has no usable timestamp: {row.get('run_id')}")
