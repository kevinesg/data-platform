import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

WEBHOOK_URL_ENV = "DATA_PLATFORM_AIRFLOW_FAILURE_ALERT_WEBHOOK_URL"
REQUEST_TIMEOUT_SECONDS = 5
EXCEPTION_SUMMARY_LIMIT = 500


def send_failure_alert(context: dict[str, Any]) -> None:
    """Send a best-effort Airflow task failure alert."""
    webhook_url = os.getenv(WEBHOOK_URL_ENV, "").strip()
    if not webhook_url:
        print(f"Airflow failure alert skipped because {WEBHOOK_URL_ENV} is unset.")
        return

    payload = json.dumps(build_failure_payload(context)).encode("utf-8")
    request = Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            if response.status >= 300:
                print(f"Airflow failure alert returned HTTP {response.status}.")
    except (HTTPError, URLError, OSError) as exc:
        print(f"Airflow failure alert delivery failed: {exc}")


def build_failure_payload(context: dict[str, Any]) -> dict[str, str]:
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    dag = context.get("dag")

    environment = first_present(os.getenv("ENVIRONMENT"), "unknown")
    dag_id = first_present(
        getattr(task_instance, "dag_id", None),
        getattr(dag, "dag_id", None),
        "unknown",
    )
    task_id = first_present(getattr(task_instance, "task_id", None), "unknown")
    run_id = first_present(
        getattr(dag_run, "run_id", None),
        context.get("run_id"),
        "unknown",
    )
    try_number = first_present(getattr(task_instance, "try_number", None), "unknown")
    max_tries = first_present(getattr(task_instance, "max_tries", None), "unknown")
    failure_time = first_present(
        context.get("ts"),
        context.get("logical_date"),
        context.get("execution_date"),
        "unknown",
    )
    log_url = first_present(getattr(task_instance, "log_url", None), "")
    exception_summary = summarize_exception(context.get("exception"))

    lines = [
        "Airflow task failed",
        f"Environment: {environment}",
        f"DAG: {dag_id}",
        f"Task: {task_id}",
        f"Run: {run_id}",
        f"Try: {try_number} of {max_tries}",
        f"Time: {failure_time}",
    ]

    if log_url:
        lines.append(f"Log: <{log_url}|Airflow task log>")
    if exception_summary:
        lines.append(f"Exception: {exception_summary}")

    return {"text": "\n".join(lines)}


def summarize_exception(exception: object) -> str:
    if exception is None:
        return ""

    summary = f"{type(exception).__name__}: {exception}"
    summary = " ".join(summary.split())
    if len(summary) > EXCEPTION_SUMMARY_LIMIT:
        return summary[: EXCEPTION_SUMMARY_LIMIT - 3] + "..."
    return summary


def first_present(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""
