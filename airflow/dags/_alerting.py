import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

WEBHOOK_URL_ENV = "DATA_PLATFORM_AIRFLOW_FAILURE_ALERT_WEBHOOK_URL"
REQUEST_TIMEOUT_SECONDS = 5
EXCEPTION_SUMMARY_LIMIT = 500
EXCEPTION_LOG_LINE_LIMIT = 25


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
    failure_time = first_present(
        context.get("ts"),
        context.get("logical_date"),
        context.get("execution_date"),
        "unknown",
    )
    log_url = first_present(getattr(task_instance, "log_url", None), "")
    exception_text = format_exception(context.get("exception"))
    task_text = f"<{log_url}|{task_id}>" if log_url else task_id

    lines = [
        f"DAG: {dag_id}",
        f"Task: {task_text}",
        f"Run: {run_id}",
        f"Time: {failure_time}",
    ]

    if exception_text:
        lines.append(f"Exception: {exception_text}")

    return {"text": "\n".join(lines)}


def format_exception(exception: object) -> str:
    if exception is None:
        return ""

    lines = [" ".join(f"{type(exception).__name__}: {exception}".split())]
    exception_logs = getattr(exception, "logs", None)
    if exception_logs:
        log_lines = [str(line).strip() for line in exception_logs if str(line).strip()]
        if log_lines:
            lines.append("Container logs:")
            lines.extend(log_lines[-EXCEPTION_LOG_LINE_LIMIT:])

    return truncate("\n".join(lines), EXCEPTION_SUMMARY_LIMIT)


def truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def first_present(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""
