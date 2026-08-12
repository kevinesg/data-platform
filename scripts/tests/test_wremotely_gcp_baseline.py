from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from wremotely_gcp_baseline import (  # noqa: E402
    BASELINE_JOB_LABELS,
    DEFAULT_BIGQUERY_MAX_BYTES_BILLED,
    GOOGLE_AUTH_SCOPES,
    collect_baseline,
    collect_dbt_run_results,
    collect_gcs,
    collect_pubsub,
    validate_options,
    write_report,
)


def test_auth_scope_allows_bounded_bigquery_metadata_jobs() -> None:
    assert "https://www.googleapis.com/auth/cloud-platform" in GOOGLE_AUTH_SCOPES
    assert "https://www.googleapis.com/auth/cloud-platform.read-only" not in (
        GOOGLE_AUTH_SCOPES
    )


def test_default_bigquery_bound_covers_measured_dev_metadata_query() -> None:
    assert DEFAULT_BIGQUERY_MAX_BYTES_BILLED == 300_000_000
    assert DEFAULT_BIGQUERY_MAX_BYTES_BILLED > 227_540_992


class FakeQueryJob:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def result(self) -> list[dict[str, Any]]:
        return self.rows


class FakeBigQueryClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def query(self, query: str, **kwargs: Any) -> FakeQueryJob:
        self.calls.append({"query": query, **kwargs})
        if "TABLE_STORAGE_USAGE_TIMELINE_BY_PROJECT" in query:
            rows = [
                {
                    "usage_date": "2026-08-10",
                    "average_billable_logical_bytes": 1_000,
                    "average_billable_physical_bytes": 400,
                }
            ]
        elif "TABLE_STORAGE_BY_PROJECT" in query:
            rows = [
                {
                    "table_schema": "raw_prod",
                    "table_name": "wremotely__job_facts",
                    "table_type": "BASE TABLE",
                    "total_rows": 1_000,
                    "total_logical_bytes": 1_000,
                    "current_physical_bytes": 400,
                    "total_physical_bytes": 450,
                    "time_travel_physical_bytes": 50,
                },
                {
                    "table_schema": "dbt_prod_mart_wremotely",
                    "table_name": "serving_jobs",
                    "table_type": "BASE TABLE",
                    "total_rows": 2_000,
                    "total_logical_bytes": 2_000,
                    "current_physical_bytes": 800,
                    "total_physical_bytes": 900,
                    "time_travel_physical_bytes": 100,
                },
            ]
        elif "JOBS_BY_PROJECT" in query:
            rows = [
                {
                    "usage_date": "2026-08-10",
                    "workload_type": "SELECT",
                    "job_count": 3,
                    "succeeded_job_count": 2,
                    "failed_job_count": 1,
                    "cache_hit_count": 1,
                    "total_bytes_processed": 3_000,
                    "total_bytes_billed": 10_000_000,
                    "total_slot_ms": 700,
                    "total_duration_ms": 900,
                }
            ]
        else:
            raise AssertionError(f"unexpected query: {query}")
        return FakeQueryJob(rows)


class FakeQueryJobConfig:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


class FakeScalarQueryParameter:
    def __init__(self, name: str, type_: str, value: Any) -> None:
        self.name = name
        self.type_ = type_
        self.value = value


class FakeBigQueryModule:
    QueryJobConfig = FakeQueryJobConfig
    ScalarQueryParameter = FakeScalarQueryParameter


class FakeBlob:
    def __init__(
        self,
        name: str,
        size: int,
        storage_class: str,
        updated: datetime,
    ) -> None:
        self.name = name
        self.size = size
        self.storage_class = storage_class
        self.updated = updated


class FakeStorageClient:
    def __init__(self, blobs: list[FakeBlob]) -> None:
        self.blobs = blobs
        self.calls: list[tuple[str, str]] = []

    def list_blobs(self, bucket: str, prefix: str) -> list[FakeBlob]:
        self.calls.append((bucket, prefix))
        return self.blobs


class FakeResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def json(self) -> dict[str, Any]:
        return self.payload


class FakeMonitoringSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(
            {
                "url": url,
                **kwargs,
                "params": dict(kwargs.get("params", {})),
            }
        )
        return self.responses.pop(0)


def monitoring_page(
    *, count: int, mean: float, next_page_token: str | None = None
) -> FakeResponse:
    payload: dict[str, Any] = {
        "timeSeries": [
            {
                "points": [
                    {
                        "value": {
                            "distributionValue": {"count": str(count), "mean": mean}
                        }
                    }
                ]
            }
        ]
    }
    if next_page_token:
        payload["nextPageToken"] = next_page_token
    return FakeResponse(payload)


def test_collect_baseline_is_domain_scoped_bounded_and_secret_safe(tmp_path: Path) -> None:
    dbt_results = tmp_path / "run_results.json"
    dbt_results.write_text(
        json.dumps(
            {
                "metadata": {"generated_at": "2026-08-10T00:10:00Z"},
                "elapsed_time": 75.25,
                "results": [
                    {
                        "status": "success",
                        "unique_id": "model.wremotely.serving_jobs",
                    },
                    {
                        "status": "fail",
                        "unique_id": "test.wremotely.serving_jobs_contract",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    bigquery_client = FakeBigQueryClient()
    storage_client = FakeStorageClient(
        [
            FakeBlob(
                "wremotely/run-1/stage/jobs.jsonl",
                120,
                "STANDARD",
                datetime(2026, 8, 9, tzinfo=UTC),
            ),
            FakeBlob(
                "wremotely/run-2/load/result.json",
                80,
                "NEARLINE",
                datetime(2026, 8, 10, tzinfo=UTC),
            ),
        ]
    )
    monitoring = FakeMonitoringSession([monitoring_page(count=4, mean=64)])

    report = collect_baseline(
        project_id="kevinesg-prod",
        location="US",
        bucket="kevinesg-prod-wremotely-artifacts",
        prefix="wremotely",
        topic="wremotely-serving-publications",
        window_start=datetime(2026, 8, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 11, tzinfo=UTC),
        max_gcs_objects=100,
        bigquery_max_bytes_billed=100_000_000,
        dbt_run_results=[dbt_results],
        bigquery_client=bigquery_client,
        bigquery_module=FakeBigQueryModule,
        storage_client=storage_client,
        monitoring_session=monitoring,
    )

    assert report["bigquery"]["current_storage"] == {
        "table_count": 2,
        "total_rows": 3_000,
        "total_logical_bytes": 3_000,
        "current_physical_bytes": 1_200,
        "total_physical_bytes": 1_350,
        "time_travel_physical_bytes": 150,
        "by_dataset": [
            {
                "dataset": "dbt_prod_mart_wremotely",
                "table_count": 1,
                "total_rows": 2_000,
                "total_logical_bytes": 2_000,
                "current_physical_bytes": 800,
                "total_physical_bytes": 900,
                "time_travel_physical_bytes": 100,
            },
            {
                "dataset": "raw_prod",
                "table_count": 1,
                "total_rows": 1_000,
                "total_logical_bytes": 1_000,
                "current_physical_bytes": 400,
                "total_physical_bytes": 450,
                "time_travel_physical_bytes": 50,
            },
        ],
    }
    assert report["bigquery"]["jobs"]["job_count"] == 3
    assert report["gcs"]["object_count"] == 2
    assert report["gcs"]["total_bytes"] == 200
    assert report["gcs"]["by_age_bucket"] == [
        {"age_bucket": "1_to_7_days", "object_count": 2, "total_bytes": 200}
    ]
    assert report["pubsub"]["published_message_count"] == 4
    assert report["pubsub"]["approximate_published_message_bytes"] == 256
    assert report["dbt"]["elapsed_seconds_average"] == 75.25

    serialized = json.dumps(report)
    assert "jobs.jsonl" not in serialized
    assert "result.json" not in serialized
    assert str(dbt_results) not in serialized
    assert "personal_finance" not in serialized
    assert storage_client.calls == [
        ("kevinesg-prod-wremotely-artifacts", "wremotely/")
    ]

    assert len(bigquery_client.calls) == 3
    for call in bigquery_client.calls:
        config = call["job_config"]
        assert isinstance(config, FakeQueryJobConfig)
        assert config.kwargs["labels"] == BASELINE_JOB_LABELS
        assert config.kwargs["maximum_bytes_billed"] == 100_000_000
        assert config.kwargs["use_query_cache"] is False
        query = call["query"]
        assert "personal_finance" not in query
        assert "wremotely__" in query
        assert "_wremotely" in query

    jobs_call = next(
        call for call in bigquery_client.calls if "JOBS_BY_PROJECT" in call["query"]
    )
    assert "COALESCE(statement_type, '') != 'SCRIPT'" in jobs_call["query"]
    assert "data_platform_operation" in jobs_call["query"]
    assert "UNNEST(referenced_tables)" in jobs_call["query"]
    assert "destination_table.dataset_id" in jobs_call["query"]


def test_validate_options_rejects_cross_domain_and_unbounded_inputs(tmp_path: Path) -> None:
    output = tmp_path / "baseline.json"
    common = {
        "project_id": "kevinesg-prod",
        "location": "US",
        "bucket": "kevinesg-prod-data-platform",
        "prefix": "wremotely",
        "topic": "wremotely-serving-publications",
        "lookback_days": 30,
        "window_end": datetime(2026, 8, 11, tzinfo=UTC),
        "max_gcs_objects": 100,
        "bigquery_max_bytes_billed": 100_000_000,
        "dbt_run_results": [],
        "output": output,
        "overwrite": False,
    }

    with pytest.raises(ValueError, match="GCS prefix must be wremotely"):
        validate_options(**{**common, "prefix": "personal_finance"})
    with pytest.raises(ValueError, match="lookback days"):
        validate_options(**{**common, "lookback_days": 43})
    with pytest.raises(ValueError, match="explicitly scoped"):
        validate_options(**{**common, "topic": "serving-publications"})
    with pytest.raises(ValueError, match="maximum bytes billed"):
        validate_options(**{**common, "bigquery_max_bytes_billed": 2_000_000_000})

    output.write_text("existing", encoding="utf-8")
    with pytest.raises(ValueError, match="output already exists"):
        validate_options(**common)


def test_collect_gcs_fails_closed_at_object_bound() -> None:
    blobs = [
        FakeBlob(
            f"wremotely/run-{index}/result.json",
            1,
            "STANDARD",
            datetime(2026, 8, 10, tzinfo=UTC),
        )
        for index in range(3)
    ]

    with pytest.raises(RuntimeError, match="no partial report written"):
        collect_gcs(
            FakeStorageClient(blobs),
            bucket="kevinesg-prod-data-platform",
            prefix="wremotely",
            max_objects=2,
            observed_at=datetime(2026, 8, 11, tzinfo=UTC),
        )


def test_collect_pubsub_aggregates_paginated_distribution_points() -> None:
    session = FakeMonitoringSession(
        [
            monitoring_page(count=2, mean=50.5, next_page_token="next"),
            monitoring_page(count=3, mean=40),
        ]
    )

    result = collect_pubsub(
        session,
        project_id="kevinesg-prod",
        topic="wremotely-serving-publications",
        window_start=datetime(2026, 8, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 11, tzinfo=UTC),
    )

    assert result["published_message_count"] == 5
    assert result["approximate_published_message_bytes"] == 221
    assert result["api_page_count"] == 2
    assert "pageToken" not in session.calls[0]["params"]
    assert session.calls[1]["params"]["pageToken"] == "next"
    assert "wremotely-serving-publications" in session.calls[0]["params"]["filter"]
    assert session.calls[0]["params"]["aggregation.alignmentPeriod"] == "3600s"
    assert session.calls[0]["params"]["aggregation.perSeriesAligner"] == "ALIGN_SUM"


def test_collect_pubsub_reports_empty_metrics_without_claiming_zero_traffic() -> None:
    result = collect_pubsub(
        FakeMonitoringSession([FakeResponse({})]),
        project_id="kevinesg-prod",
        topic="wremotely-serving-publications",
        window_start=datetime(2026, 8, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 11, tzinfo=UTC),
    )

    assert result["published_message_count"] == 0
    assert "distinguish zero traffic" in result["metric_gap_warning"]


def test_collect_dbt_run_results_rejects_invalid_or_cross_domain_content(
    tmp_path: Path,
) -> None:
    invalid = tmp_path / "run_results.json"
    invalid.write_text(json.dumps({"elapsed_time": -1}), encoding="utf-8")

    with pytest.raises(RuntimeError, match="invalid elapsed_time"):
        collect_dbt_run_results([invalid])

    cross_domain = tmp_path / "cross-domain.json"
    cross_domain.write_text(
        json.dumps(
            {
                "metadata": {"generated_at": "2026-08-10T00:00:00Z"},
                "elapsed_time": 10,
                "results": [
                    {
                        "status": "success",
                        "unique_id": "model.personal_finance.private_model",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="exclusively scoped to wremotely"):
        collect_dbt_run_results([cross_domain])

    valid = tmp_path / "valid.json"
    valid.write_text(
        json.dumps(
            {
                "metadata": {
                    "generated_at": "2026-08-10T00:00:00Z",
                    "invocation_id": "private-invocation-id",
                },
                "elapsed_time": 10,
                "results": [
                    {
                        "status": "success",
                        "unique_id": "model.wremotely.secret_model",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = collect_dbt_run_results([valid])

    assert result["samples"] == [
        {
            "sample": 1,
            "generated_at": "2026-08-10T00:00:00Z",
            "elapsed_seconds": 10.0,
            "result_count": 1,
            "status_counts": {"success": 1},
        }
    ]
    serialized = json.dumps(result)
    assert "private-invocation-id" not in serialized
    assert "secret_model" not in serialized
    assert str(valid) not in serialized


def test_write_report_refuses_overwrite_and_replaces_atomically(tmp_path: Path) -> None:
    output = tmp_path / "baseline.json"
    write_report(output, {"contract_version": 1}, overwrite=False)
    assert json.loads(output.read_text(encoding="utf-8")) == {"contract_version": 1}

    with pytest.raises(ValueError, match="output already exists"):
        write_report(output, {"contract_version": 2}, overwrite=False)

    write_report(output, {"contract_version": 2}, overwrite=True)
    assert json.loads(output.read_text(encoding="utf-8")) == {"contract_version": 2}
    assert list(tmp_path.glob("*.tmp")) == []
