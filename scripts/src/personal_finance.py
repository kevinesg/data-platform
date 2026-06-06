from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import gspread
from dotenv import load_dotenv
from google.cloud import storage
from gspread.utils import ValueRenderOption, rowcol_to_a1

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = SCRIPTS_DIR / ".env"
SCHEMA_DIR = SCRIPTS_DIR / "schemas"


@dataclass(frozen=True)
class SourceEntity:
    name: str
    sheet_name: str
    raw_table: str
    schema_path: Path
    gcs_prefix: str


SOURCE_ENTITIES = (
    SourceEntity(
        name="transactions",
        sheet_name="transactions",
        raw_table="personal_finance__transactions",
        schema_path=SCHEMA_DIR / "personal_finance__transactions.json",
        gcs_prefix="transactions",
    ),
    SourceEntity(
        name="paid_for_others",
        sheet_name="paid_for_others",
        raw_table="personal_finance__paid_for_others",
        schema_path=SCHEMA_DIR / "personal_finance__paid_for_others.json",
        gcs_prefix="paid_for_others",
    ),
    SourceEntity(
        name="transfers",
        sheet_name="transfers",
        raw_table="personal_finance__transfers",
        schema_path=SCHEMA_DIR / "personal_finance__transfers.json",
        gcs_prefix="transfers",
    ),
    SourceEntity(
        name="accounts",
        sheet_name="accounts",
        raw_table="personal_finance__accounts",
        schema_path=SCHEMA_DIR / "personal_finance__accounts.json",
        gcs_prefix="accounts",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract personal_finance Google Sheets rows to GCS JSONL staging."
    )
    parser.add_argument("--entity", choices=[entity.name for entity in SOURCE_ENTITIES])
    parser.add_argument("--run-id")
    args = parser.parse_args()

    if DEFAULT_ENV_FILE.exists():
        load_dotenv(DEFAULT_ENV_FILE, override=False)

    run_id = args.run_id or dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    try:
        extract(run_id=run_id, entity_name=args.entity)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


def extract(run_id: str, entity_name: str | None = None) -> None:
    config = load_config()
    extracted_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    storage_client = storage.Client.from_service_account_json(
        config["credentials"], project=config["project_id"]
    )
    bucket = storage_client.bucket(config["bucket_name"])
    spreadsheet = gspread.service_account(filename=config["credentials"]).open_by_url(
        config["gsheet_url"]
    )

    print(f"run_id={run_id}")
    for entity in select_entities(entity_name):
        schema = load_entity_schema(entity)
        source_fields = source_schema_fields(schema)
        worksheet = spreadsheet.worksheet(entity.sheet_name)
        prefix = entity_gcs_prefix(config, entity)

        source_rows, extracted_rows, chunk_count = extract_sheet_chunks_to_gcs(
            worksheet=worksheet,
            bucket=bucket,
            bucket_name=config["bucket_name"],
            prefix=prefix,
            run_id=run_id,
            chunk_size=config["chunk_size"],
            source_schema_fields=source_fields,
            extracted_at=extracted_at,
        )

        print(f"entity={entity.name}")
        print(f"raw_table={entity.raw_table}")
        print(f"source_rows={source_rows}")
        print(f"extracted_rows={extracted_rows}")
        print(f"chunk_count={chunk_count}")
        print(f"gcs_prefix=gs://{config['bucket_name']}/{prefix}/{run_id}/extract/")


def load_config() -> dict[str, Any]:
    prefix = env("PERSONAL_FINANCE_GCS_PREFIX").strip("/")
    if not prefix:
        raise ValueError("PERSONAL_FINANCE_GCS_PREFIX must not be empty")

    return {
        "project_id": env("PROJECT_ID"),
        "credentials": env("GOOGLE_APPLICATION_CREDENTIALS"),
        "gsheet_url": env("PERSONAL_FINANCE_GSHEET_URL"),
        "bucket_name": env("PERSONAL_FINANCE_GCS_BUCKET"),
        "prefix": prefix,
        "chunk_size": positive_int_env("PERSONAL_FINANCE_CHUNK_SIZE"),
    }


def load_entity_schema(entity: SourceEntity) -> dict[str, Any]:
    return json.loads(entity.schema_path.read_text(encoding="utf-8"))


def source_schema_fields(schema: dict[str, Any]) -> list[dict[str, Any]]:
    return [field for field in schema["fields"] if field.get("source_field")]


def entity_gcs_prefix(config: dict[str, Any], entity: SourceEntity) -> str:
    return f"{config['prefix']}/{entity.gcs_prefix}"


def select_entities(entity_name: str | None = None) -> tuple[SourceEntity, ...]:
    if entity_name is None:
        return SOURCE_ENTITIES
    return tuple(entity for entity in SOURCE_ENTITIES if entity.name == entity_name)


def extract_sheet_chunks_to_gcs(
    worksheet: gspread.Worksheet,
    bucket: storage.Bucket,
    bucket_name: str,
    prefix: str,
    run_id: str,
    chunk_size: int,
    source_schema_fields: list[dict[str, Any]],
    extracted_at: str,
) -> tuple[int, int, int]:
    source_fields = [str(field["name"]) for field in source_schema_fields]
    required_fields = [
        str(field["name"]) for field in source_schema_fields if field["mode"] == "REQUIRED"
    ]

    headers = [header.strip() for header in worksheet.row_values(1)]
    header_indexes = {header: index for index, header in enumerate(headers) if header}
    missing_headers = [name for name in source_fields if name not in header_indexes]
    if missing_headers:
        raise ValueError("missing source columns: " + ", ".join(missing_headers))

    source_rows = 0
    extracted_rows = 0
    chunk_count = 0

    for start_row in range(2, worksheet.row_count + 1, chunk_size):
        end_row = min(start_row + chunk_size - 1, worksheet.row_count)
        rows = worksheet.get(
            f"A{start_row}:{rowcol_to_a1(end_row, len(headers))}",
            value_render_option=ValueRenderOption.formatted,
        )
        if not rows:
            break

        extract_chunk = []
        for offset, row in enumerate(rows):
            source_row_number = start_row + offset
            if not any(str(value).strip() for value in row):
                continue

            record = {
                name: str(row[index]).strip() if index < len(row) else ""
                for name, index in header_indexes.items()
            }
            source_rows += 1

            missing_required_values = [name for name in required_fields if not record[name]]
            if missing_required_values:
                raise ValueError(
                    f"row {source_row_number} missing required values: "
                    + ", ".join(missing_required_values)
                )

            output_row = coerce_record(record, source_schema_fields, source_row_number)
            output_row["_extracted_at"] = extracted_at
            extract_chunk.append(output_row)

        if extract_chunk:
            chunk_count += 1
            blob_name = f"{prefix}/{run_id}/extract/chunk-{chunk_count:06d}.jsonl"
            upload_jsonl(bucket, blob_name, extract_chunk)
            extracted_rows += len(extract_chunk)
            print(f"wrote {len(extract_chunk)} rows to gs://{bucket_name}/{blob_name}")

    return source_rows, extracted_rows, chunk_count


def coerce_record(
    record: dict[str, str],
    schema_fields: list[dict[str, Any]],
    source_row_number: int,
) -> dict[str, Any]:
    coerced = {}
    for field in schema_fields:
        name = str(field["name"])
        value = record[name]
        try:
            coerced[name] = coerce_value(value, str(field["type"]), str(field["mode"]))
        except ValueError as exc:
            raise ValueError(f"row {source_row_number} invalid {name}: {exc}") from exc
    return coerced


def coerce_value(value: str, field_type: str, mode: str) -> Any:
    if value == "" and mode == "NULLABLE":
        return None
    if value == "" and mode == "REQUIRED":
        raise ValueError("required value is empty")
    if field_type == "INTEGER":
        return int(value)
    if field_type == "FLOAT":
        return float(value)
    if field_type == "BOOLEAN":
        normalized = value.lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
        raise ValueError("expected boolean")
    return value


def upload_jsonl(bucket: storage.Bucket, blob_name: str, rows: list[dict[str, Any]]) -> None:
    body = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)
    bucket.blob(blob_name).upload_from_string(body, content_type="application/x-ndjson")


def env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"missing required environment variable: {name}")
    return value


def positive_int_env(name: str) -> int:
    value = env(name)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
