"""Airflow-side freshness checks for the on-prem Wremotely path."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from airflow.api_fastapi.app import get_auth_manager, init_auth_manager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser


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
    runs = _list_successful_runs(dag_id)
    if not runs:
        raise RuntimeError(f"{dag_id} has no successful runs")

    latest = max(runs, key=_run_timestamp)
    return {
        "run_id": latest.get("dag_run_id") or latest.get("run_id"),
        "end_date": latest.get("end_date"),
        "start_date": latest.get("start_date"),
        "logical_date": latest.get("logical_date"),
        "run_after": latest.get("run_after"),
    }


def _list_successful_runs(dag_id: str) -> list[dict[str, Any]]:
    """Read DAG history through Airflow's public API (ORM is forbidden in Airflow 3 tasks)."""
    init_auth_manager()
    token = get_auth_manager().generate_jwt(
        user=SimpleAuthManagerUser(username="wremotely-monitor", role="VIEWER"),
        expiration_time_in_seconds=60,
    )
    base_url = os.getenv("WREMOTELY_AIRFLOW_API_URL", "http://api-server:8080/api/v2").rstrip("/")
    query = urlencode({"state": "success", "limit": 100, "order_by": "-end_date"})
    request = Request(
        f"{base_url}/dags/{quote(dag_id, safe='')}/dagRuns?{query}",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.load(response)
    except Exception as exc:
        raise RuntimeError(f"could not inspect {dag_id} successful runs through Airflow API: {exc}") from exc
    runs = payload.get("dag_runs", [])
    if not isinstance(runs, list):
        raise RuntimeError(f"Airflow API returned an invalid run list for {dag_id}")
    return [run for run in runs if isinstance(run, dict)]


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
