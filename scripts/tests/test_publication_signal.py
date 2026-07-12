from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from publication_signal import load_ready_publication_artifact, publish_ready_publication


class FakeQueryJob:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def result(self) -> list[dict[str, object]]:
        return self.rows


class FakeBigQueryClient:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.query_calls: list[dict[str, object]] = []

    def query(self, query: str, **kwargs: object) -> FakeQueryJob:
        self.query_calls.append({"query": query, **kwargs})
        return FakeQueryJob(self.rows)


class FakeQueryJobConfig:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


class FakeScalarQueryParameter:
    def __init__(self, name: str, type_: str, value: object) -> None:
        self.name = name
        self.type_ = type_
        self.value = value


class FakeBigQueryModule:
    QueryJobConfig = FakeQueryJobConfig
    ScalarQueryParameter = FakeScalarQueryParameter


class FakePublishFuture:
    def __init__(self, message_id: str) -> None:
        self.message_id = message_id
        self.timeouts: list[float] = []

    def result(self, timeout: float) -> str:
        self.timeouts.append(timeout)
        return self.message_id


class FakePublisherClient:
    def __init__(self) -> None:
        self.publish_calls: list[tuple[str, bytes]] = []
        self.future = FakePublishFuture("message-123")

    def topic_path(self, project_id: str, topic: str) -> str:
        return f"projects/{project_id}/topics/{topic}"

    def publish(self, topic_path: str, data: bytes) -> FakePublishFuture:
        self.publish_calls.append((topic_path, data))
        return self.future


def write_artifact(path: Path, state: str = "READY") -> None:
    path.write_text(
        json.dumps(
            {
                "contract_version": 1,
                "publication_id": "wremotely-abc123",
                "publication_state": state,
            }
        ),
        encoding="utf-8",
    )
    path.with_name("_SUCCESS").write_text("", encoding="utf-8")


def test_publish_ready_publication_verifies_control_row_and_sends_only_id(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "publish_serving_snapshot.json"
    write_artifact(artifact)
    bigquery_client = FakeBigQueryClient(
        [{"publication_id": "wremotely-abc123", "publication_state": "READY"}]
    )
    publisher = FakePublisherClient()

    result = publish_ready_publication(
        project_id="kevinesg-dev",
        handoff_dataset="handoff_kevinesg",
        publication_table="wremotely__serving_publications",
        publication_topic="wremotely-serving-publications-kevinesg",
        publication_artifact=artifact,
        location="US",
        publish_timeout_seconds=30,
        bigquery_client=bigquery_client,
        bigquery_module=FakeBigQueryModule,
        publisher_client=publisher,
    )

    assert publisher.publish_calls == [
        (
            "projects/kevinesg-dev/topics/wremotely-serving-publications-kevinesg",
            b"wremotely-abc123",
        )
    ]
    assert publisher.future.timeouts == [30]
    assert result["message_id"] == "message-123"
    query_config = bigquery_client.query_calls[0]["job_config"]
    assert isinstance(query_config, FakeQueryJobConfig)
    parameter = query_config.kwargs["query_parameters"]
    assert isinstance(parameter, list)
    assert parameter[0].value == "wremotely-abc123"


def test_publish_ready_publication_allows_duplicate_delivery_on_retry(tmp_path: Path) -> None:
    artifact = tmp_path / "publish_serving_snapshot.json"
    write_artifact(artifact)
    bigquery_client = FakeBigQueryClient(
        [{"publication_id": "wremotely-abc123", "publication_state": "READY"}]
    )
    publisher = FakePublisherClient()
    kwargs: dict[str, Any] = {
        "project_id": "kevinesg-dev",
        "handoff_dataset": "handoff_kevinesg",
        "publication_table": "wremotely__serving_publications",
        "publication_topic": "wremotely-serving-publications-kevinesg",
        "publication_artifact": artifact,
        "location": "US",
        "publish_timeout_seconds": 30,
        "bigquery_client": bigquery_client,
        "bigquery_module": FakeBigQueryModule,
        "publisher_client": publisher,
    }

    publish_ready_publication(**kwargs)
    publish_ready_publication(**kwargs)

    assert len(publisher.publish_calls) == 2
    assert {call[1] for call in publisher.publish_calls} == {b"wremotely-abc123"}


def test_publish_ready_publication_rejects_non_ready_control_row(tmp_path: Path) -> None:
    artifact = tmp_path / "publish_serving_snapshot.json"
    write_artifact(artifact)
    publisher = FakePublisherClient()

    with pytest.raises(RuntimeError, match="control row is not READY"):
        publish_ready_publication(
            project_id="kevinesg-dev",
            handoff_dataset="handoff_kevinesg",
            publication_table="wremotely__serving_publications",
            publication_topic="wremotely-serving-publications-kevinesg",
            publication_artifact=artifact,
            location="US",
            publish_timeout_seconds=30,
            bigquery_client=FakeBigQueryClient(
                [{"publication_id": "wremotely-abc123", "publication_state": "FAILED"}]
            ),
            bigquery_module=FakeBigQueryModule,
            publisher_client=publisher,
        )

    assert publisher.publish_calls == []


def test_load_ready_publication_artifact_rejects_non_ready_state(tmp_path: Path) -> None:
    artifact = tmp_path / "publish_serving_snapshot.json"
    write_artifact(artifact, state="FAILED")

    with pytest.raises(RuntimeError, match="artifact is not READY"):
        load_ready_publication_artifact(artifact)


def test_load_ready_publication_artifact_requires_success_marker(tmp_path: Path) -> None:
    artifact = tmp_path / "publish_serving_snapshot.json"
    write_artifact(artifact)
    artifact.with_name("_SUCCESS").unlink()

    with pytest.raises(RuntimeError, match="no _SUCCESS marker"):
        load_ready_publication_artifact(artifact)
