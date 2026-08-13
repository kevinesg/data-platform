from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
LOCATION_PATTERN = re.compile(r"^[A-Za-z0-9-]+$")
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")
TOPIC_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$")
TABLE_PREFIXES = ("wremotely__", "stg_wremotely__", "int_wremotely__")
DATASET_SUFFIXES = ("_wremotely",)
BASELINE_JOB_LABELS = {"data_platform_operation": "wremotely_gcp_baseline"}
DEFAULT_BIGQUERY_MAX_BYTES_BILLED = 300_000_000
DEFAULT_BIGQUERY_MAX_TOTAL_BYTES_BILLED = 2_000_000_000
MAX_BIGQUERY_TOTAL_BYTES_BILLED = 5_000_000_000
JOBS_QUERY_SLICE_DAYS = 1
PUBSUB_MESSAGE_SIZE_METRIC = "pubsub.googleapis.com/topic/message_sizes"
MONITORING_ENDPOINT = "https://monitoring.googleapis.com/v3"
GOOGLE_AUTH_SCOPES = (
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/monitoring.read",
)
MIB = 1024 * 1024
SUCCESSFUL_DBT_STATUSES = frozenset({"no-op", "pass", "reused", "success", "warn"})
RETAINED_ARTIFACT_METADATA_KEY = "wremotely_retention"
RETAINED_ARTIFACT_CONTRACT_VERSION = 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture a bounded, read-only wremotely GCP workload baseline."
    )
    parser.add_argument("--gcp-project", required=True)
    parser.add_argument("--bigquery-location", required=True)
    parser.add_argument("--gcs-bucket", required=True)
    parser.add_argument("--gcs-prefix", required=True)
    parser.add_argument("--publication-topic", required=True)
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--window-end", type=parse_timestamp)
    parser.add_argument("--max-gcs-objects", type=int, default=250_000)
    parser.add_argument(
        "--bigquery-max-bytes-billed",
        type=int,
        default=DEFAULT_BIGQUERY_MAX_BYTES_BILLED,
    )
    parser.add_argument(
        "--bigquery-max-total-bytes-billed",
        type=int,
        default=DEFAULT_BIGQUERY_MAX_TOTAL_BYTES_BILLED,
    )
    parser.add_argument("--dbt-run-results", action="append", default=[], type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    try:
        window_end = args.window_end or datetime.now(UTC).replace(microsecond=0)
        options = validate_options(
            project_id=args.gcp_project,
            location=args.bigquery_location,
            bucket=args.gcs_bucket,
            prefix=args.gcs_prefix,
            topic=args.publication_topic,
            lookback_days=args.lookback_days,
            window_end=window_end,
            max_gcs_objects=args.max_gcs_objects,
            bigquery_max_bytes_billed=args.bigquery_max_bytes_billed,
            bigquery_max_total_bytes_billed=args.bigquery_max_total_bytes_billed,
            dbt_run_results=args.dbt_run_results,
            output=args.output,
            overwrite=args.overwrite,
        )

        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        from google.cloud import bigquery, storage

        credentials, _ = google.auth.default(scopes=GOOGLE_AUTH_SCOPES)
        report = collect_baseline(
            **options,
            bigquery_client=bigquery.Client(
                project=options["project_id"], credentials=credentials
            ),
            bigquery_module=bigquery,
            storage_client=storage.Client(
                project=options["project_id"], credentials=credentials
            ),
            monitoring_session=AuthorizedSession(credentials),
        )
        write_report(args.output, report, overwrite=args.overwrite)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(f"baseline_report={args.output}")
    print(f"measurement_window={report['scope']['window_start']}/{report['scope']['window_end']}")
    print(f"bigquery_job_count={report['bigquery']['jobs']['job_count']}")
    print(f"gcs_object_count={report['gcs']['object_count']}")
    print(f"pubsub_message_count={report['pubsub']['published_message_count']}")
    return 0


def parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timestamp must be ISO 8601") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed.astimezone(UTC).replace(microsecond=0)


def validate_options(
    *,
    project_id: str,
    location: str,
    bucket: str,
    prefix: str,
    topic: str,
    lookback_days: int,
    window_end: datetime,
    max_gcs_objects: int,
    bigquery_max_bytes_billed: int,
    dbt_run_results: list[Path],
    output: Path,
    overwrite: bool,
    bigquery_max_total_bytes_billed: int = DEFAULT_BIGQUERY_MAX_TOTAL_BYTES_BILLED,
) -> dict[str, Any]:
    if not PROJECT_ID_PATTERN.fullmatch(project_id):
        raise ValueError("gcp project must be a valid project ID")
    if not LOCATION_PATTERN.fullmatch(location):
        raise ValueError("BigQuery location must contain only letters, digits, or hyphens")
    if not BUCKET_PATTERN.fullmatch(bucket):
        raise ValueError("GCS bucket must be a valid bucket name")
    normalized_prefix = prefix.strip("/")
    if normalized_prefix != "wremotely" and not normalized_prefix.startswith("wremotely/"):
        raise ValueError("GCS prefix must be wremotely or a child of wremotely")
    if not TOPIC_PATTERN.fullmatch(topic) or topic.lower().startswith("goog"):
        raise ValueError("publication topic must be a valid Pub/Sub topic ID")
    if "wremotely" not in topic.lower():
        raise ValueError("publication topic must be explicitly scoped to wremotely")
    if not 1 <= lookback_days <= 42:
        raise ValueError("lookback days must be between 1 and 42")
    if window_end > datetime.now(UTC) + timedelta(minutes=5):
        raise ValueError("window end cannot be in the future")
    if not 1 <= max_gcs_objects <= 1_000_000:
        raise ValueError("max GCS objects must be between 1 and 1000000")
    if not 10_000_000 <= bigquery_max_bytes_billed <= 1_000_000_000:
        raise ValueError("BigQuery maximum bytes billed must be between 10 MB and 1 GB")
    if not (
        bigquery_max_bytes_billed
        <= bigquery_max_total_bytes_billed
        <= MAX_BIGQUERY_TOTAL_BYTES_BILLED
    ):
        raise ValueError(
            "BigQuery maximum total bytes billed must be at least the per-query maximum "
            "and no more than 5 GB"
        )
    missing_artifacts = [str(path) for path in dbt_run_results if not path.is_file()]
    if missing_artifacts:
        raise ValueError("dbt run-results files do not exist: " + ", ".join(missing_artifacts))
    if output.exists() and not overwrite:
        raise ValueError(f"output already exists; pass --overwrite to replace it: {output}")
    if not output.parent.is_dir():
        raise ValueError(f"output parent directory does not exist: {output.parent}")

    return {
        "project_id": project_id,
        "location": location,
        "bucket": bucket,
        "prefix": normalized_prefix,
        "topic": topic,
        "window_start": window_end - timedelta(days=lookback_days),
        "window_end": window_end,
        "max_gcs_objects": max_gcs_objects,
        "bigquery_max_bytes_billed": bigquery_max_bytes_billed,
        "bigquery_max_total_bytes_billed": bigquery_max_total_bytes_billed,
        "dbt_run_results": dbt_run_results,
    }


def collect_baseline(
    *,
    project_id: str,
    location: str,
    bucket: str,
    prefix: str,
    topic: str,
    window_start: datetime,
    window_end: datetime,
    max_gcs_objects: int,
    bigquery_max_bytes_billed: int,
    dbt_run_results: list[Path],
    bigquery_client: Any,
    bigquery_module: Any,
    storage_client: Any,
    monitoring_session: Any,
    bigquery_max_total_bytes_billed: int = DEFAULT_BIGQUERY_MAX_TOTAL_BYTES_BILLED,
) -> dict[str, Any]:
    return {
        "contract_version": 1,
        "scope": {
            "product": "wremotely",
            "project_id": project_id,
            "bigquery_location": location,
            "window_start": format_timestamp(window_start),
            "window_end": format_timestamp(window_end),
            "lookback_days": (window_end - window_start).days,
            "bigquery_table_prefixes": list(TABLE_PREFIXES),
            "bigquery_dataset_suffixes": list(DATASET_SUFFIXES),
            "gcs_bucket": bucket,
            "gcs_prefix": prefix,
            "publication_topic": topic,
        },
        "bigquery": collect_bigquery(
            client=bigquery_client,
            bigquery_module=bigquery_module,
            project_id=project_id,
            location=location,
            window_start=window_start,
            window_end=window_end,
            maximum_bytes_billed=bigquery_max_bytes_billed,
            maximum_total_bytes_billed=bigquery_max_total_bytes_billed,
        ),
        "gcs": collect_gcs(
            storage_client,
            bucket=bucket,
            prefix=prefix,
            max_objects=max_gcs_objects,
            observed_at=window_end,
        ),
        "pubsub": collect_pubsub(
            monitoring_session,
            project_id=project_id,
            topic=topic,
            window_start=window_start,
            window_end=window_end,
        ),
        "dbt": collect_dbt_run_results(dbt_run_results),
        "limitations": [
            "Usage is reported instead of currency because shared-project billing does not "
            "provide defensible wremotely-only invoice allocation.",
            "BigQuery jobs are attributed by exact wremotely table prefixes, referenced or "
            "destination relations in domain-owned dbt datasets, or the wremotely identifier "
            "in query text.",
            "BigQuery storage history is a delayed preview view and can lag by about 72 hours.",
            "GCS totals require a bounded object-prefix listing and exclude "
            "request-operation cost.",
            "Pub/Sub Monitoring metrics can contain occasional gaps and have a shorter retention "
            "window than BigQuery job history.",
            "End-to-end dbt duration is included only for explicitly supplied run_results.json "
            "artifacts; BigQuery query duration is not treated as dbt wall-clock duration.",
        ],
    }


@dataclass(frozen=True)
class BigQueryResult:
    rows: list[dict[str, Any]]
    total_bytes_billed: int


def collect_bigquery(
    *,
    client: Any,
    bigquery_module: Any,
    project_id: str,
    location: str,
    window_start: datetime,
    window_end: datetime,
    maximum_bytes_billed: int,
    maximum_total_bytes_billed: int = DEFAULT_BIGQUERY_MAX_TOTAL_BYTES_BILLED,
) -> dict[str, Any]:
    total_bytes_billed = 0
    query_count = 0

    def execute(query: str, parameters: list[Any]) -> list[dict[str, Any]]:
        nonlocal total_bytes_billed, query_count
        remaining = maximum_total_bytes_billed - total_bytes_billed
        if remaining < 10_000_000:
            raise RuntimeError(
                "BigQuery total bytes billed budget exhausted before the baseline completed"
            )
        result = run_bigquery(
            client,
            bigquery_module,
            query,
            location,
            project_id,
            min(maximum_bytes_billed, remaining),
            parameters,
        )
        total_bytes_billed += result.total_bytes_billed
        query_count += 1
        if total_bytes_billed > maximum_total_bytes_billed:
            raise RuntimeError("BigQuery total bytes billed exceeded the baseline budget")
        return result.rows

    parameters = [
        bigquery_module.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
        bigquery_module.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
    ]
    current_rows = execute(current_storage_query(project_id, location), [])
    timeline_rows = execute(storage_timeline_query(project_id, location), parameters)
    job_rows: list[dict[str, Any]] = []
    for slice_start, slice_end in time_slices(
        window_start, window_end, days=JOBS_QUERY_SLICE_DAYS
    ):
        job_rows.extend(
            execute(
                jobs_query(project_id, location),
                [
                    bigquery_module.ScalarQueryParameter(
                        "window_start", "TIMESTAMP", slice_start
                    ),
                    bigquery_module.ScalarQueryParameter(
                        "window_end", "TIMESTAMP", slice_end
                    ),
                ],
            )
        )
    return {
        "current_storage": summarize_current_storage(current_rows),
        "storage_timeline": [normalize_mapping(row) for row in timeline_rows],
        "jobs": summarize_jobs(job_rows),
        "query_budget": {
            "per_query_max_bytes_billed": maximum_bytes_billed,
            "total_max_bytes_billed": maximum_total_bytes_billed,
            "total_bytes_billed": total_bytes_billed,
            "query_count": query_count,
            "jobs_query_slice_days": JOBS_QUERY_SLICE_DAYS,
        },
        "attribution": {
            "table_prefixes": list(TABLE_PREFIXES),
            "dataset_suffixes": list(DATASET_SUFFIXES),
            "query_text_identifier": "wremotely",
            "excluded_job_label": BASELINE_JOB_LABELS,
            "script_parent_jobs_excluded": True,
        },
    }


def run_bigquery(
    client: Any,
    bigquery_module: Any,
    query: str,
    location: str,
    project_id: str,
    maximum_bytes_billed: int,
    parameters: list[Any],
) -> BigQueryResult:
    config = bigquery_module.QueryJobConfig(
        labels=BASELINE_JOB_LABELS,
        maximum_bytes_billed=maximum_bytes_billed,
        query_parameters=parameters,
        use_query_cache=False,
    )
    job = client.query(
        query,
        job_config=config,
        location=location,
        project=project_id,
    )
    return BigQueryResult(
        rows=[
            dict(row)
            for row in job.result()
        ],
        total_bytes_billed=int(getattr(job, "total_bytes_billed", 0) or 0),
    )


def time_slices(
    window_start: datetime, window_end: datetime, *, days: int
) -> Iterable[tuple[datetime, datetime]]:
    if days < 1:
        raise ValueError("time-slice days must be positive")
    cursor = window_start
    step = timedelta(days=days)
    while cursor < window_end:
        slice_end = min(cursor + step, window_end)
        yield cursor, slice_end
        cursor = slice_end


def current_storage_query(project_id: str, location: str) -> str:
    view = information_schema_view(project_id, location, "TABLE_STORAGE_BY_PROJECT")
    return f"""
        SELECT
            table_schema,
            table_name,
            table_type,
            total_rows,
            total_logical_bytes,
            current_physical_bytes,
            total_physical_bytes,
            time_travel_physical_bytes
        FROM {view}
        WHERE NOT deleted
          AND {relation_scope_sql("table_schema", "table_name")}
        ORDER BY table_schema, table_name
    """


def storage_timeline_query(project_id: str, location: str) -> str:
    view = information_schema_view(
        project_id, location, "TABLE_STORAGE_USAGE_TIMELINE_BY_PROJECT"
    )
    return f"""
        SELECT
            CAST(usage_date AS STRING) AS usage_date,
            CAST(DIV(SUM(billable_total_logical_usage) * {MIB}, 86400) AS INT64)
                AS average_billable_logical_bytes,
            CAST(DIV(SUM(billable_total_physical_usage) * {MIB}, 86400) AS INT64)
                AS average_billable_physical_bytes
        FROM {view}
        WHERE usage_date >= DATE(@window_start)
          AND usage_date < DATE(@window_end)
          AND {relation_scope_sql("table_schema", "table_name")}
        GROUP BY usage_date
        ORDER BY usage_date
    """


def jobs_query(project_id: str, location: str) -> str:
    view = information_schema_view(project_id, location, "JOBS_BY_PROJECT")
    prefixes = ", ".join(repr(prefix) for prefix in TABLE_PREFIXES)
    suffixes = ", ".join(repr(suffix) for suffix in DATASET_SUFFIXES)
    return f"""
        SELECT
            CAST(DATE(creation_time) AS STRING) AS usage_date,
            COALESCE(statement_type, job_type, 'UNKNOWN') AS workload_type,
            COUNT(*) AS job_count,
            COUNTIF(error_result IS NULL) AS succeeded_job_count,
            COUNTIF(error_result IS NOT NULL) AS failed_job_count,
            COUNTIF(cache_hit) AS cache_hit_count,
            COALESCE(SUM(total_bytes_processed), 0) AS total_bytes_processed,
            COALESCE(SUM(total_bytes_billed), 0) AS total_bytes_billed,
            COALESCE(SUM(total_slot_ms), 0) AS total_slot_ms,
            COALESCE(SUM(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 0)
                AS total_duration_ms
        FROM {view}
        WHERE creation_time >= @window_start
          AND creation_time < @window_end
          AND state = 'DONE'
          AND COALESCE(statement_type, '') != 'SCRIPT'
          AND NOT EXISTS (
              SELECT 1
              FROM UNNEST(labels) AS job_label
              WHERE job_label.key = 'data_platform_operation'
                AND job_label.value = 'wremotely_gcp_baseline'
          )
          AND (
              REGEXP_CONTAINS(COALESCE(query, ''), r'(?i)(^|[^a-z0-9])wremotely([^a-z0-9]|$)')
              OR EXISTS (
                  SELECT 1
                  FROM UNNEST([{prefixes}]) AS table_prefix
                  WHERE STARTS_WITH(COALESCE(destination_table.table_id, ''), table_prefix)
              )
              OR EXISTS (
                  SELECT 1
                  FROM UNNEST([{suffixes}]) AS dataset_suffix
                  WHERE ENDS_WITH(
                      COALESCE(destination_table.dataset_id, ''), dataset_suffix
                  )
              )
              OR EXISTS (
                  SELECT 1
                  FROM UNNEST(referenced_tables) AS referenced_table
                  CROSS JOIN UNNEST([{prefixes}]) AS table_prefix
                  WHERE STARTS_WITH(referenced_table.table_id, table_prefix)
              )
              OR EXISTS (
                  SELECT 1
                  FROM UNNEST(referenced_tables) AS referenced_table
                  CROSS JOIN UNNEST([{suffixes}]) AS dataset_suffix
                  WHERE ENDS_WITH(referenced_table.dataset_id, dataset_suffix)
              )
          )
        GROUP BY usage_date, workload_type
        ORDER BY usage_date, workload_type
    """


def information_schema_view(project_id: str, location: str, view: str) -> str:
    return f"`{project_id}.region-{location.lower()}.INFORMATION_SCHEMA.{view}`"


def relation_scope_sql(dataset_column: str, table_column: str) -> str:
    clauses = [
        *(f"STARTS_WITH({table_column}, {prefix!r})" for prefix in TABLE_PREFIXES),
        *(f"ENDS_WITH({dataset_column}, {suffix!r})" for suffix in DATASET_SUFFIXES),
    ]
    return "(" + " OR ".join(clauses) + ")"


def summarize_current_storage(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    totals = defaultdict(int)
    datasets: dict[str, dict[str, int]] = {}
    for row in rows:
        dataset = str(row["table_schema"])
        summary = datasets.setdefault(
            dataset,
            {
                "table_count": 0,
                "total_rows": 0,
                "total_logical_bytes": 0,
                "current_physical_bytes": 0,
                "total_physical_bytes": 0,
                "time_travel_physical_bytes": 0,
            },
        )
        summary["table_count"] += 1
        for key in (
            "total_rows",
            "total_logical_bytes",
            "current_physical_bytes",
            "total_physical_bytes",
            "time_travel_physical_bytes",
        ):
            value = int(row.get(key) or 0)
            summary[key] += value
            totals[key] += value
        totals["table_count"] += 1
    return {
        "table_count": totals["table_count"],
        "total_rows": totals["total_rows"],
        "total_logical_bytes": totals["total_logical_bytes"],
        "current_physical_bytes": totals["current_physical_bytes"],
        "total_physical_bytes": totals["total_physical_bytes"],
        "time_travel_physical_bytes": totals["time_travel_physical_bytes"],
        "by_dataset": [
            {"dataset": dataset, **summary}
            for dataset, summary in sorted(datasets.items())
        ],
    }


def summarize_jobs(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    keys = (
        "job_count",
        "succeeded_job_count",
        "failed_job_count",
        "cache_hit_count",
        "total_bytes_processed",
        "total_bytes_billed",
        "total_slot_ms",
        "total_duration_ms",
    )
    totals = defaultdict(int)
    normalized_rows = []
    for row in rows:
        normalized = {
            "usage_date": str(row["usage_date"]),
            "workload_type": str(row["workload_type"]),
        }
        for key in keys:
            normalized[key] = int(row.get(key) or 0)
            totals[key] += normalized[key]
        normalized_rows.append(normalized)
    return {**{key: totals[key] for key in keys}, "daily_by_workload_type": normalized_rows}


def collect_gcs(
    client: Any,
    *,
    bucket: str,
    prefix: str,
    max_objects: int,
    observed_at: datetime,
) -> dict[str, Any]:
    object_count = 0
    total_bytes = 0
    by_storage_class: dict[str, dict[str, int]] = {}
    by_age_bucket: dict[str, dict[str, int]] = {}
    oldest: datetime | None = None
    newest: datetime | None = None
    listing_prefix = prefix.rstrip("/") + "/"
    for blob in client.list_blobs(bucket, prefix=listing_prefix):
        object_count += 1
        if object_count > max_objects:
            raise RuntimeError(
                f"GCS prefix exceeds --max-gcs-objects={max_objects}; no partial report written"
            )
        size = int(blob.size or 0)
        total_bytes += size
        storage_class = str(blob.storage_class or "UNKNOWN")
        increment_bucket(by_storage_class, storage_class, size)
        updated = blob.updated
        if updated is not None:
            updated = updated.astimezone(UTC)
            increment_bucket(
                by_age_bucket,
                object_age_bucket(updated=updated, observed_at=observed_at),
                size,
            )
            oldest = updated if oldest is None or updated < oldest else oldest
            newest = updated if newest is None or updated > newest else newest
        else:
            increment_bucket(by_age_bucket, "unknown", size)
    return {
        "bucket": bucket,
        "prefix": prefix,
        "object_count": object_count,
        "total_bytes": total_bytes,
        "oldest_updated_at": format_timestamp(oldest) if oldest else None,
        "latest_updated_at": format_timestamp(newest) if newest else None,
        "by_storage_class": keyed_summaries(by_storage_class, "storage_class"),
        "by_age_bucket": keyed_summaries(by_age_bucket, "age_bucket"),
        "listing_complete": True,
        "max_objects": max_objects,
    }


def object_age_bucket(*, updated: datetime, observed_at: datetime) -> str:
    age = observed_at - updated
    if age < timedelta(days=1):
        return "lt_1_day"
    if age < timedelta(days=8):
        return "1_to_7_days"
    if age < timedelta(days=31):
        return "8_to_30_days"
    if age < timedelta(days=91):
        return "31_to_90_days"
    if age < timedelta(days=366):
        return "91_to_365_days"
    return "gt_365_days"


def increment_bucket(target: dict[str, dict[str, int]], key: str, size: int) -> None:
    summary = target.setdefault(key, {"object_count": 0, "total_bytes": 0})
    summary["object_count"] += 1
    summary["total_bytes"] += size


def keyed_summaries(values: dict[str, dict[str, int]], key_name: str) -> list[dict[str, Any]]:
    return [{key_name: key, **summary} for key, summary in sorted(values.items())]


def collect_pubsub(
    session: Any,
    *,
    project_id: str,
    topic: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    url = f"{MONITORING_ENDPOINT}/projects/{project_id}/timeSeries"
    params = {
        "filter": (
            f'metric.type = "{PUBSUB_MESSAGE_SIZE_METRIC}" '
            'AND resource.type = "pubsub_topic" '
            f'AND resource.labels.topic_id = "{topic}"'
        ),
        "interval.startTime": format_timestamp(window_start),
        "interval.endTime": format_timestamp(window_end),
        "aggregation.alignmentPeriod": "3600s",
        "aggregation.perSeriesAligner": "ALIGN_SUM",
        "view": "FULL",
        "pageSize": "1000",
    }
    message_count = 0
    approximate_bytes = 0
    point_count = 0
    page_count = 0
    while True:
        response = session.get(url, params=params, timeout=60)
        if response.status_code != 200:
            raise RuntimeError(
                f"Cloud Monitoring query failed with HTTP {response.status_code}"
            )
        payload = response.json()
        page_count += 1
        for series in payload.get("timeSeries", []):
            for point in series.get("points", []):
                distribution = point.get("value", {}).get("distributionValue", {})
                count = int(distribution.get("count", 0))
                mean = float(distribution.get("mean", 0.0))
                message_count += count
                approximate_bytes += round(count * mean)
                point_count += 1
        page_token = payload.get("nextPageToken")
        if not page_token:
            break
        params["pageToken"] = page_token
    return {
        "topic": topic,
        "metric_type": PUBSUB_MESSAGE_SIZE_METRIC,
        "window_start": format_timestamp(window_start),
        "window_end": format_timestamp(window_end),
        "published_message_count": message_count,
        "approximate_published_message_bytes": approximate_bytes,
        "metric_point_count": point_count,
        "api_page_count": page_count,
        "alignment_period_seconds": 3600,
        "per_series_aligner": "ALIGN_SUM",
        "metric_gap_warning": (
            "No metric points were returned; distinguish zero traffic from a Monitoring gap."
            if point_count == 0
            else None
        ),
    }


def collect_dbt_run_results(paths: list[Path]) -> dict[str, Any]:
    samples = []
    for index, path in enumerate(paths, start=1):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"could not read dbt run results: {path}") from exc
        elapsed = payload.get("elapsed_time")
        results = payload.get("results")
        metadata = payload.get("metadata")
        if not isinstance(elapsed, (int, float)) or elapsed < 0:
            raise RuntimeError(f"dbt run results has invalid elapsed_time: {path}")
        if not isinstance(results, list) or not isinstance(metadata, dict):
            raise RuntimeError(f"dbt run results has invalid structure: {path}")
        if not results:
            raise RuntimeError(f"dbt run results contains no executed nodes: {path}")
        metadata_args = metadata.get("args")
        if isinstance(metadata_args, dict) and metadata_args.get("which") == "build":
            pass
        elif metadata_args is not None:
            raise RuntimeError(f"dbt run results is not from dbt build: {path}")
        else:
            retention = metadata.get(RETAINED_ARTIFACT_METADATA_KEY)
            if not isinstance(retention, dict) or retention != {
                "contract_version": RETAINED_ARTIFACT_CONTRACT_VERSION,
                "invocation": "dbt build",
            }:
                raise RuntimeError(f"dbt run results is not from dbt build: {path}")
        statuses: dict[str, int] = defaultdict(int)
        for result in results:
            if not isinstance(result, dict):
                raise RuntimeError(f"dbt run results contains an invalid result: {path}")
            unique_id = result.get("unique_id")
            unique_id_parts = unique_id.split(".") if isinstance(unique_id, str) else []
            if len(unique_id_parts) < 3 or unique_id_parts[1] != "wremotely":
                raise RuntimeError(
                    f"dbt run results is not exclusively scoped to wremotely: {path}"
                )
            status = str(result.get("status", "unknown"))
            if status not in SUCCESSFUL_DBT_STATUSES:
                raise RuntimeError(
                    f"dbt run results contains a non-successful status {status!r}: {path}"
                )
            statuses[status] += 1
        samples.append(
            {
                "sample": index,
                "generated_at": metadata.get("generated_at"),
                "elapsed_seconds": round(float(elapsed), 3),
                "result_count": len(results),
                "status_counts": dict(sorted(statuses.items())),
            }
        )
    elapsed_values = [sample["elapsed_seconds"] for sample in samples]
    return {
        "sample_count": len(samples),
        "elapsed_seconds_min": min(elapsed_values) if elapsed_values else None,
        "elapsed_seconds_max": max(elapsed_values) if elapsed_values else None,
        "elapsed_seconds_average": (
            round(sum(elapsed_values) / len(elapsed_values), 3) if elapsed_values else None
        ),
        "samples": samples,
        "status": "collected" if samples else "not_collected",
    }


def normalize_mapping(row: dict[str, Any]) -> dict[str, Any]:
    return {key: normalize_value(value) for key, value in row.items()}


def normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_timestamp(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, int | float | str | bool) or value is None:
        return value
    return str(value)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def write_report(path: Path, report: dict[str, Any], *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise ValueError(f"output already exists; pass --overwrite to replace it: {path}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


if __name__ == "__main__":
    raise SystemExit(main())
