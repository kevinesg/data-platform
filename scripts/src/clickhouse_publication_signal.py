from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
PUBSUB_TOPIC_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$")
PUBLICATION_ID_PATTERN = re.compile(r"^wremotely-[a-f0-9]{64}$")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish a verified READY ClickHouse publication ID to Pub/Sub."
    )
    parser.add_argument("--gcp-project", required=True)
    parser.add_argument("--publication-topic", required=True)
    parser.add_argument("--publication-artifact", required=True, type=Path)
    parser.add_argument("--publish-timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()

    from google.cloud import pubsub_v1

    result = publish_ready_clickhouse_publication(
        project_id=args.gcp_project,
        publication_topic=args.publication_topic,
        publication_artifact=args.publication_artifact,
        publish_timeout_seconds=args.publish_timeout_seconds,
        publisher_client=pubsub_v1.PublisherClient(),
    )
    print(f"publication_id={result['publication_id']}")
    print(f"pubsub_topic={result['topic_path']}")
    print(f"pubsub_message_id={result['message_id']}")
    return 0


def publish_ready_clickhouse_publication(
    project_id: str,
    publication_topic: str,
    publication_artifact: Path,
    publish_timeout_seconds: float,
    publisher_client: Any,
) -> dict[str, str]:
    validate_options(
        project_id,
        publication_topic,
        publication_artifact,
        publish_timeout_seconds,
    )
    publication_id = load_ready_clickhouse_publication(publication_artifact)
    topic_path = publisher_client.topic_path(project_id, publication_topic)
    message_id = publisher_client.publish(
        topic_path,
        publication_id.encode("utf-8"),
    ).result(timeout=publish_timeout_seconds)
    if not isinstance(message_id, str) or not message_id:
        raise RuntimeError("Pub/Sub publish returned no message ID")
    return {
        "publication_id": publication_id,
        "topic_path": topic_path,
        "message_id": message_id,
    }


def validate_options(
    project_id: str,
    publication_topic: str,
    publication_artifact: Path,
    publish_timeout_seconds: float,
) -> None:
    if not PROJECT_ID_PATTERN.fullmatch(project_id):
        raise ValueError("gcp project must be a valid project ID")
    if (
        not PUBSUB_TOPIC_PATTERN.fullmatch(publication_topic)
        or publication_topic.lower().startswith("goog")
    ):
        raise ValueError("publication topic must be a valid Pub/Sub topic ID")
    if not publication_artifact.is_file():
        raise ValueError(f"publication artifact does not exist: {publication_artifact}")
    if not 1 <= publish_timeout_seconds <= 300:
        raise ValueError("publish timeout must be between 1 and 300 seconds")


def load_ready_clickhouse_publication(path: Path) -> str:
    if path.name != "manifest.json":
        raise RuntimeError("ClickHouse publication artifact must be manifest.json")
    success_path = path.with_name("_SUCCESS")
    control_path = path.with_name("control.json")
    if not success_path.is_file():
        raise RuntimeError("ClickHouse publication artifact has no _SUCCESS marker")
    if not control_path.is_file():
        raise RuntimeError("ClickHouse publication artifact has no control.json")
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        control = json.loads(control_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"could not read ClickHouse publication artifact: {path}") from exc
    if not isinstance(manifest, dict) or not isinstance(control, dict):
        raise RuntimeError("ClickHouse publication artifact files must contain JSON objects")
    publication_id = manifest.get("publication_id")
    if not isinstance(publication_id, str) or not PUBLICATION_ID_PATTERN.fullmatch(publication_id):
        raise RuntimeError("ClickHouse publication artifact has an invalid publication_id")
    if manifest.get("publication_state") != "READY":
        raise RuntimeError("ClickHouse publication artifact is not READY")
    if control.get("manifest") != "manifest.json":
        raise RuntimeError("ClickHouse publication control does not point to manifest.json")
    if control.get("publication_id") != publication_id:
        raise RuntimeError("ClickHouse publication control ID does not match manifest")
    if not isinstance(control.get("run_id"), str) or not control["run_id"]:
        raise RuntimeError("ClickHouse publication control has no run_id")
    return publication_id


if __name__ == "__main__":
    raise SystemExit(main())
