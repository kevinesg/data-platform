from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from clickhouse_publication_signal import (
    load_ready_clickhouse_publication,
    publish_ready_clickhouse_publication,
)


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


def write_publication(directory: Path, state: str = "READY") -> Path:
    directory.mkdir()
    publication_id = "wremotely-" + "a" * 64
    manifest = directory / "manifest.json"
    manifest.write_text(
        json.dumps({"publication_id": publication_id, "publication_state": state}),
        encoding="utf-8",
    )
    (directory / "control.json").write_text(
        json.dumps(
            {
                "run_id": "onprem-run",
                "publication_id": publication_id,
                "manifest": "manifest.json",
            }
        ),
        encoding="utf-8",
    )
    (directory / "_SUCCESS").write_text("", encoding="utf-8")
    return manifest


def test_publish_ready_clickhouse_publication_sends_only_id(tmp_path: Path) -> None:
    artifact = write_publication(tmp_path / "publication")
    publisher = FakePublisherClient()

    result = publish_ready_clickhouse_publication(
        project_id="kevinesg-prod",
        publication_topic="wremotely-serving-publications",
        publication_artifact=artifact,
        publish_timeout_seconds=30,
        publisher_client=publisher,
    )

    publication_id = "wremotely-" + "a" * 64
    assert publisher.publish_calls == [
        (
            "projects/kevinesg-prod/topics/wremotely-serving-publications",
            publication_id.encode("utf-8"),
        )
    ]
    assert publisher.future.timeouts == [30]
    assert result == {
        "publication_id": publication_id,
        "topic_path": "projects/kevinesg-prod/topics/wremotely-serving-publications",
        "message_id": "message-123",
    }


def test_load_ready_clickhouse_publication_requires_success_and_control(tmp_path: Path) -> None:
    artifact = write_publication(tmp_path / "publication")
    assert load_ready_clickhouse_publication(artifact) == "wremotely-" + "a" * 64

    artifact.with_name("_SUCCESS").unlink()
    with pytest.raises(RuntimeError, match="_SUCCESS"):
        load_ready_clickhouse_publication(artifact)


def test_load_ready_clickhouse_publication_rejects_non_ready_state(tmp_path: Path) -> None:
    artifact = write_publication(tmp_path / "publication", state="FAILED")
    with pytest.raises(RuntimeError, match="not READY"):
        load_ready_clickhouse_publication(artifact)


def test_load_ready_clickhouse_publication_rejects_control_mismatch(tmp_path: Path) -> None:
    artifact = write_publication(tmp_path / "publication")
    control = json.loads(artifact.with_name("control.json").read_text(encoding="utf-8"))
    control["publication_id"] = "wremotely-" + "b" * 64
    artifact.with_name("control.json").write_text(json.dumps(control), encoding="utf-8")

    with pytest.raises(RuntimeError, match="does not match"):
        load_ready_clickhouse_publication(artifact)
