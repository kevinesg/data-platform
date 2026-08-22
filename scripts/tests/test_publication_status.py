from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from publication_status import (  # noqa: E402
    resolve_clickhouse_password,
    verify_publication_status,
)

PUBLICATION_ID = "wremotely-" + "a" * 64


@dataclass
class Message:
    data: bytes


@dataclass
class Received:
    ack_id: str
    message: Message


class Response:
    def __init__(self, messages: list[Received]) -> None:
        self.received_messages = messages


class Subscriber:
    def __init__(self, messages: list[Received]) -> None:
        self.messages = messages
        self.acknowledgements: list[dict[str, object]] = []

    def subscription_path(self, project_id: str, subscription: str) -> str:
        return f"projects/{project_id}/subscriptions/{subscription}"

    def pull(self, **_: object) -> Response:
        return Response(self.messages)

    def acknowledge(self, **kwargs: object) -> None:
        self.acknowledgements.append(kwargs)


def status_message(status: str = "applied") -> bytes:
    return json.dumps(
        {
            "publication_id": PUBLICATION_ID,
            "status": status,
            "reported_at": "2026-08-21T00:00:00+00:00",
        }
    ).encode()


def test_resolves_password_from_private_environment(monkeypatch) -> None:
    monkeypatch.setenv("WREMOTELY_CLICKHOUSE_PASSWORD", "private-secret")

    assert resolve_clickhouse_password(None) == "private-secret"
    assert resolve_clickhouse_password("explicit-secret") == "explicit-secret"


def test_verifies_matching_receipt_and_persists_state(tmp_path: Path) -> None:
    subscriber = Subscriber([Received("ack-1", Message(status_message()))])

    result = verify_publication_status(
        project_id="kevinesg-prod",
        status_subscription="wremotely-serving-publication-status-worker",
        clickhouse_url="http://127.0.0.1:8123",
        clickhouse_database="wremotely_prod",
        clickhouse_user="wremotely_reader",
        clickhouse_password="secret",
        state_file=tmp_path / "status.json",
        pull_timeout_seconds=5,
        max_messages=100,
        subscriber_client=subscriber,
        clickhouse_reader=lambda *_: PUBLICATION_ID,
    )

    assert result["status"] == "applied"
    assert json.loads((tmp_path / "status.json").read_text()) == {
        "publication_id": PUBLICATION_ID,
        "reported_at": "2026-08-21T00:00:00+00:00",
        "status": "applied",
    }
    assert subscriber.acknowledgements == [
        {
            "request": {
                "subscription": (
                    "projects/kevinesg-prod/subscriptions/"
                    "wremotely-serving-publication-status-worker"
                ),
                "ack_ids": ["ack-1"],
            }
        }
    ]


def test_reuses_checkpoint_when_subscription_is_empty(tmp_path: Path) -> None:
    state_file = tmp_path / "status.json"
    state_file.write_text(
        json.dumps(
            {
                "publication_id": PUBLICATION_ID,
                "status": "already_applied",
                "reported_at": "2026-08-21T00:00:00+00:00",
            }
        )
    )

    result = verify_publication_status(
        project_id="kevinesg-prod",
        status_subscription="wremotely-serving-publication-status-worker",
        clickhouse_url="http://127.0.0.1:8123",
        clickhouse_database="wremotely_prod",
        clickhouse_user="wremotely_reader",
        clickhouse_password=None,
        state_file=state_file,
        pull_timeout_seconds=5,
        max_messages=100,
        subscriber_client=Subscriber([]),
        clickhouse_reader=lambda *_: PUBLICATION_ID,
    )

    assert result["status"] == "already_applied"


def test_fails_when_receipt_is_missing(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="no post-commit receipt"):
        verify_publication_status(
            project_id="kevinesg-prod",
            status_subscription="wremotely-serving-publication-status-worker",
            clickhouse_url="http://127.0.0.1:8123",
            clickhouse_database="wremotely_prod",
            clickhouse_user="wremotely_reader",
            clickhouse_password=None,
            state_file=tmp_path / "status.json",
            pull_timeout_seconds=5,
            max_messages=100,
            subscriber_client=Subscriber([]),
            clickhouse_reader=lambda *_: PUBLICATION_ID,
        )
