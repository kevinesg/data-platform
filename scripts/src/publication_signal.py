from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
BIGQUERY_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
PUBSUB_TOPIC_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish a verified READY BigQuery publication ID to Pub/Sub."
    )
    parser.add_argument("--gcp-project", required=True)
    parser.add_argument("--handoff-dataset", required=True)
    parser.add_argument("--publication-table", required=True)
    parser.add_argument("--publication-topic", required=True)
    parser.add_argument("--publication-artifact", required=True, type=Path)
    parser.add_argument("--bigquery-location", required=True)
    parser.add_argument("--publish-timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()

    from google.cloud import bigquery, pubsub_v1

    result = publish_ready_publication(
        project_id=args.gcp_project,
        handoff_dataset=args.handoff_dataset,
        publication_table=args.publication_table,
        publication_topic=args.publication_topic,
        publication_artifact=args.publication_artifact,
        location=args.bigquery_location,
        publish_timeout_seconds=args.publish_timeout_seconds,
        bigquery_client=bigquery.Client(project=args.gcp_project),
        bigquery_module=bigquery,
        publisher_client=pubsub_v1.PublisherClient(),
    )
    print(f"publication_id={result['publication_id']}")
    print(f"pubsub_topic={result['topic_path']}")
    print(f"pubsub_message_id={result['message_id']}")
    return 0


def publish_ready_publication(
    project_id: str,
    handoff_dataset: str,
    publication_table: str,
    publication_topic: str,
    publication_artifact: Path,
    location: str,
    publish_timeout_seconds: float,
    bigquery_client: Any,
    bigquery_module: Any,
    publisher_client: Any,
) -> dict[str, str]:
    validate_options(
        project_id,
        handoff_dataset,
        publication_table,
        publication_topic,
        publication_artifact,
        location,
        publish_timeout_seconds,
    )
    artifact = load_ready_publication_artifact(publication_artifact)
    publication_id = artifact["publication_id"]
    verify_ready_publication(
        bigquery_client,
        bigquery_module,
        project_id,
        handoff_dataset,
        publication_table,
        publication_id,
        location,
    )
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
    handoff_dataset: str,
    publication_table: str,
    publication_topic: str,
    publication_artifact: Path,
    location: str,
    publish_timeout_seconds: float,
) -> None:
    if not PROJECT_ID_PATTERN.fullmatch(project_id):
        raise ValueError("gcp project must be a valid project ID")
    for name, value in (
        ("handoff dataset", handoff_dataset),
        ("publication table", publication_table),
    ):
        if not BIGQUERY_ID_PATTERN.fullmatch(value):
            raise ValueError(f"{name} must be a valid BigQuery identifier")
    if (
        not PUBSUB_TOPIC_PATTERN.fullmatch(publication_topic)
        or publication_topic.lower().startswith("goog")
    ):
        raise ValueError("publication topic must be a valid Pub/Sub topic ID")
    if not publication_artifact.is_file():
        raise ValueError(f"publication artifact does not exist: {publication_artifact}")
    if not location.strip():
        raise ValueError("BigQuery location is required")
    if not 1 <= publish_timeout_seconds <= 300:
        raise ValueError("publish timeout must be between 1 and 300 seconds")


def load_ready_publication_artifact(path: Path) -> dict[str, str]:
    if not path.with_name("_SUCCESS").is_file():
        raise RuntimeError("publication artifact has no _SUCCESS marker")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"could not read publication artifact: {path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("publication artifact must be a JSON object")
    publication_id = payload.get("publication_id")
    if not isinstance(publication_id, str) or not publication_id:
        raise RuntimeError("publication artifact has no publication_id")
    if payload.get("publication_state") != "READY":
        raise RuntimeError("publication artifact is not READY")
    return {"publication_id": publication_id, "publication_state": "READY"}


def verify_ready_publication(
    client: Any,
    bigquery_module: Any,
    project_id: str,
    handoff_dataset: str,
    publication_table: str,
    publication_id: str,
    location: str,
) -> None:
    table_id = f"{project_id}.{handoff_dataset}.{publication_table}"
    query = f"""
        SELECT publication_id, publication_state
        FROM `{table_id}`
        WHERE publication_id = @publication_id
    """
    rows = list(
        client.query(
            query,
            job_config=bigquery_module.QueryJobConfig(
                query_parameters=[
                    bigquery_module.ScalarQueryParameter(
                        "publication_id",
                        "STRING",
                        publication_id,
                    )
                ]
            ),
            location=location,
            project=project_id,
        ).result()
    )
    if len(rows) != 1:
        raise RuntimeError(
            f"expected one publication-control row for {publication_id}, found {len(rows)}"
        )
    row = dict(rows[0])
    if row.get("publication_id") != publication_id:
        raise RuntimeError(f"publication-control row ID does not match: {publication_id}")
    if row.get("publication_state") != "READY":
        raise RuntimeError(f"publication-control row is not READY: {publication_id}")


if __name__ == "__main__":
    raise SystemExit(main())
