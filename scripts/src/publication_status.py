"""Verify the latest ClickHouse publication was committed by the VPS worker."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from google.cloud import pubsub_v1

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
PUBLICATION_ID_PATTERN = re.compile(r"^wremotely-[a-f0-9]{64}$")
ACCEPTED_STATUSES = frozenset({"applied", "already_applied", "reconciled"})
MAX_MESSAGES = 100


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify the latest ClickHouse publication has a post-commit worker receipt."
    )
    parser.add_argument("--gcp-project", required=True)
    parser.add_argument("--status-subscription", required=True)
    parser.add_argument("--clickhouse-url", required=True)
    parser.add_argument("--clickhouse-database", required=True)
    parser.add_argument("--clickhouse-user", required=True)
    parser.add_argument("--clickhouse-password", default=None)
    parser.add_argument("--state-file", required=True, type=Path)
    parser.add_argument("--pull-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--max-messages", type=int, default=MAX_MESSAGES)
    args = parser.parse_args()

    subscriber = pubsub_v1.SubscriberClient()
    try:
        result = verify_publication_status(
            project_id=args.gcp_project,
            status_subscription=args.status_subscription,
            clickhouse_url=args.clickhouse_url,
            clickhouse_database=args.clickhouse_database,
            clickhouse_user=args.clickhouse_user,
            clickhouse_password=args.clickhouse_password,
            state_file=args.state_file,
            pull_timeout_seconds=args.pull_timeout_seconds,
            max_messages=args.max_messages,
            subscriber_client=subscriber,
        )
    finally:
        subscriber.close()
    print(json.dumps(result, sort_keys=True))
    return 0


def verify_publication_status(
    *,
    project_id: str,
    status_subscription: str,
    clickhouse_url: str,
    clickhouse_database: str,
    clickhouse_user: str,
    clickhouse_password: str | None,
    state_file: Path,
    pull_timeout_seconds: float,
    max_messages: int,
    subscriber_client: Any,
    clickhouse_reader: Any | None = None,
) -> dict[str, str]:
    validate_options(
        project_id,
        status_subscription,
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        pull_timeout_seconds,
        max_messages,
    )
    expected_publication_id = (clickhouse_reader or read_latest_clickhouse_publication)(
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
    )
    subscription_path = subscriber_client.subscription_path(project_id, status_subscription)
    response = subscriber_client.pull(
        request={
            "subscription": subscription_path,
            "max_messages": max_messages,
        },
        timeout=pull_timeout_seconds,
    )
    matching_status: dict[str, str] | None = None
    ack_ids: list[str] = []
    for received in response.received_messages:
        ack_ids.append(received.ack_id)
        status = parse_status_message(received.message.data)
        if status["publication_id"] == expected_publication_id:
            matching_status = status

    if matching_status is None:
        saved = load_state(state_file)
        if saved is None or saved.get("publication_id") != expected_publication_id:
            raise RuntimeError(
                "no post-commit receipt for latest ClickHouse publication: "
                f"{expected_publication_id}"
            )
        if saved.get("status") not in ACCEPTED_STATUSES:
            raise RuntimeError(
                f"publication {expected_publication_id} has unexpected worker status "
                f"{saved.get('status')}"
            )
        matching_status = saved
    elif matching_status["status"] not in ACCEPTED_STATUSES:
        raise RuntimeError(
            f"publication {expected_publication_id} has unexpected worker status "
            f"{matching_status['status']}"
        )
    else:
        write_state(state_file, matching_status)

    if ack_ids:
        subscriber_client.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
    result = {
        "publication_id": expected_publication_id,
        "status": matching_status["status"],
        "reported_at": matching_status["reported_at"],
    }
    print(
        f"publication_convergence=verified publication_id={expected_publication_id} "
        f"status={matching_status['status']}"
    )
    return result


def read_latest_clickhouse_publication(
    clickhouse_url: str,
    database: str,
    user: str,
    password: str | None,
) -> str:
    query = (
        f"SELECT publication_id FROM `{database}`.`wremotely__publication_control` "
        "WHERE publication_state = 'READY' ORDER BY completed_at DESC LIMIT 1 "
        "FORMAT JSONEachRow"
    )
    request = Request(
        f"{clickhouse_url.rstrip('/')}/?{urlencode({'query': query})}",
        headers={"Accept": "application/json"},
    )
    if password is not None:
        import base64

        credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
        request.add_header("Authorization", f"Basic {credentials}")
    else:
        request.add_header("X-ClickHouse-User", user)
    with urlopen(request, timeout=10) as response:
        rows = [json.loads(line) for line in response.read().decode().splitlines() if line.strip()]
    if len(rows) != 1 or not isinstance(rows[0].get("publication_id"), str):
        raise RuntimeError("ClickHouse has no single latest READY publication")
    publication_id = rows[0]["publication_id"]
    if not PUBLICATION_ID_PATTERN.fullmatch(publication_id):
        raise RuntimeError("ClickHouse latest READY publication ID is invalid")
    return publication_id


def parse_status_message(data: bytes) -> dict[str, str]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("publication status message is not valid JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError("publication status message must be an object")
    publication_id = value.get("publication_id")
    status = value.get("status")
    reported_at = value.get("reported_at")
    if (
        not isinstance(publication_id, str)
        or not PUBLICATION_ID_PATTERN.fullmatch(publication_id)
        or not isinstance(status, str)
        or not isinstance(reported_at, str)
    ):
        raise RuntimeError("publication status message has invalid fields")
    return {"publication_id": publication_id, "status": status, "reported_at": reported_at}


def load_state(path: Path) -> dict[str, str] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"publication status state is unreadable: {path}") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"publication status state must be an object: {path}")
    try:
        return {key: value[key] for key in ("publication_id", "status", "reported_at")}
    except KeyError as error:
        raise RuntimeError(f"publication status state is incomplete: {path}") from error


def write_state(path: Path, value: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as temporary:
        json.dump(value, temporary, sort_keys=True)
        temporary.write("\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def validate_options(
    project_id: str,
    status_subscription: str,
    clickhouse_url: str,
    clickhouse_database: str,
    clickhouse_user: str,
    pull_timeout_seconds: float,
    max_messages: int,
) -> None:
    if not re.fullmatch(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$", project_id):
        raise ValueError("gcp project must be a valid project ID")
    if not re.fullmatch(r"^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$", status_subscription):
        raise ValueError("status subscription must be a valid Pub/Sub ID")
    if status_subscription.lower().startswith("goog"):
        raise ValueError("status subscription cannot start with goog")
    if not clickhouse_url.startswith(("http://", "https://")):
        raise ValueError("ClickHouse URL must use HTTP or HTTPS")
    for label, value in (("database", clickhouse_database), ("user", clickhouse_user)):
        if not IDENTIFIER_PATTERN.fullmatch(value):
            raise ValueError(f"ClickHouse {label} must be a valid identifier")
    if pull_timeout_seconds <= 0:
        raise ValueError("pull timeout must be positive")
    if not 1 <= max_messages <= MAX_MESSAGES:
        raise ValueError(f"max messages must be between 1 and {MAX_MESSAGES}")


if __name__ == "__main__":
    raise SystemExit(main())
