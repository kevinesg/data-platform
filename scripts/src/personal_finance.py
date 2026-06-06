from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import google.auth
import gspread
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from gspread.utils import ValueRenderOption, rowcol_to_a1

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
SCHEMA_DIR = SCRIPTS_DIR / "schemas"
PREFERRED_DEV_ENV_FILE = Path.home() / "dev/secrets/data-platform/.env"
GOOGLE_API_SCOPES = (
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
)


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
    parser = argparse.ArgumentParser(description="Run personal_finance extract/load steps.")
    parser.add_argument("--step", required=True, choices=["extract", "load"])
    parser.add_argument("--entity", choices=[entity.name for entity in SOURCE_ENTITIES])
    parser.add_argument("--run-id")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=default_env_file(),
        help=(
            "external dotenv file; discovers DATA_PLATFORM_ENV_FILE, "
            "DATA_PLATFORM_SECRETS_DIR, or the preferred dev path when present"
        ),
    )
    args = parser.parse_args()

    try:
        if args.env_file is not None:
            load_environment(args.env_file)

        if args.step == "extract":
            run_id = args.run_id or dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
            extract(run_id=run_id, entity_name=args.entity)
        else:
            if not args.run_id:
                raise ValueError("--run-id is required for load")
            load(run_id=args.run_id, entity_name=args.entity)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


def extract(run_id: str, entity_name: str | None = None) -> None:
    config = load_config()
    extracted_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    credentials, _ = google.auth.default(scopes=GOOGLE_API_SCOPES)
    storage_client = storage.Client(project=config["project_id"], credentials=credentials)
    bucket = storage_client.bucket(config["bucket_name"])
    spreadsheet = gspread.authorize(credentials).open_by_url(config["gsheet_url"])

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


def load(run_id: str, entity_name: str | None = None) -> None:
    config = load_config()
    credentials, _ = google.auth.default(scopes=GOOGLE_API_SCOPES)
    bq_client = bigquery.Client(project=config["project_id"], credentials=credentials)
    storage_client = storage.Client(project=config["project_id"], credentials=credentials)
    bucket = storage_client.bucket(config["bucket_name"])

    print(f"run_id={run_id}")
    for entity in select_entities(entity_name):
        schema = load_entity_schema(entity)
        source_fields = source_schema_fields(schema)
        extracted_at_field = next(
            field for field in schema["fields"] if field["name"] == "_extracted_at"
        )
        prefix = entity_gcs_prefix(config, entity)
        extract_uri = find_jsonl_uri(
            bucket,
            config["bucket_name"],
            f"{prefix}/{run_id}/extract/",
        )
        table_id = f"{config['project_id']}.{config['dataset']}.{entity.raw_table}"
        ensure_raw_table(bq_client, table_id, schema["fields"])

        if extract_uri is None:
            print(f"entity={entity.name}")
            print("loaded_rows=0")
            print(f"skipped_no_extract_files=true table={table_id}")
            continue

        staging_table_id = f"{table_id}__load_{safe_identifier(run_id)}"
        try:
            loaded_rows = load_extract_files_to_staging(
                bq_client,
                extract_uri,
                staging_table_id,
                [*source_fields, extracted_at_field],
            )
            apply_raw_table_upsert(
                bq_client,
                table_id,
                staging_table_id,
                source_fields,
            )

            print(f"entity={entity.name}")
            print(f"loaded_rows={loaded_rows}")
            print(f"applied_table={table_id}")
        finally:
            bq_client.delete_table(staging_table_id, not_found_ok=True)


def load_config() -> dict[str, Any]:
    prefix = env("PERSONAL_FINANCE_GCS_PREFIX").strip("/")
    if not prefix:
        raise ValueError("PERSONAL_FINANCE_GCS_PREFIX must not be empty")

    return {
        "project_id": env("PROJECT_ID"),
        "dataset": env("RAW_DATASET"),
        "gsheet_url": env("PERSONAL_FINANCE_GSHEET_URL"),
        "bucket_name": env("PERSONAL_FINANCE_GCS_BUCKET"),
        "prefix": prefix,
        "chunk_size": positive_int_env("PERSONAL_FINANCE_CHUNK_SIZE"),
    }


def default_env_file() -> Path | None:
    value = os.getenv("DATA_PLATFORM_ENV_FILE", "").strip()
    if value:
        return Path(value).expanduser()
    secrets_dir = os.getenv("DATA_PLATFORM_SECRETS_DIR", "").strip()
    if secrets_dir:
        return Path(secrets_dir).expanduser() / ".env"
    if PREFERRED_DEV_ENV_FILE.is_file():
        return PREFERRED_DEV_ENV_FILE
    return None


def load_environment(env_file: Path) -> None:
    env_file = env_file.expanduser()
    if not env_file.is_file():
        raise ValueError(f"env file does not exist: {env_file}")
    load_dotenv(env_file, override=False)


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


def find_jsonl_uri(bucket: storage.Bucket, bucket_name: str, prefix: str) -> str | None:
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith(".jsonl"):
            return f"gs://{bucket_name}/{prefix}*.jsonl"
    return None


def ensure_raw_table(client: bigquery.Client, table_id: str, fields: list[dict[str, Any]]) -> None:
    desired_schema = bigquery_schema(fields)
    try:
        table = client.get_table(table_id)
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=desired_schema))
        return

    desired_by_name = {field.name: field for field in desired_schema}
    existing_names = {field.name for field in table.schema}
    updated_schema = []
    schema_changed = False

    for existing_field in table.schema:
        desired_field = desired_by_name.get(existing_field.name)
        if desired_field is None:
            updated_schema.append(existing_field)
            continue
        if existing_field.field_type != desired_field.field_type:
            raise ValueError(
                f"{table_id}.{existing_field.name} has type {existing_field.field_type}; "
                f"expected {desired_field.field_type}"
            )
        if existing_field.mode == desired_field.mode:
            updated_schema.append(existing_field)
            continue
        if existing_field.mode == "REQUIRED" and desired_field.mode == "NULLABLE":
            field_resource = existing_field.to_api_repr()
            field_resource["mode"] = "NULLABLE"
            updated_schema.append(bigquery.SchemaField.from_api_repr(field_resource))
            schema_changed = True
            continue
        raise ValueError(
            f"{table_id}.{existing_field.name} has mode {existing_field.mode}; "
            f"expected {desired_field.mode}"
        )

    missing_fields = [field for field in desired_schema if field.name not in existing_names]
    missing_required_fields = [field.name for field in missing_fields if field.mode == "REQUIRED"]
    if missing_required_fields:
        raise ValueError(
            f"{table_id} is missing required columns: " + ", ".join(missing_required_fields)
        )

    if missing_fields or schema_changed:
        table.schema = [*updated_schema, *missing_fields]
        client.update_table(table, ["schema"])


def load_extract_files_to_staging(
    client: bigquery.Client,
    extract_uri: str,
    staging_table_id: str,
    fields: list[dict[str, Any]],
) -> int:
    load_job = client.load_table_from_uri(
        extract_uri,
        staging_table_id,
        job_config=bigquery.LoadJobConfig(
            schema=bigquery_schema(fields),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()
    return load_job.output_rows or 0


def apply_raw_table_upsert(
    client: bigquery.Client,
    table_id: str,
    staging_table_id: str,
    source_schema_fields: list[dict[str, Any]],
) -> None:
    source_columns = [str(field["name"]) for field in source_schema_fields]
    update_expressions = [f"target.`{column}` = source.`{column}`" for column in source_columns]
    update_expressions.extend(
        [
            "target.`_extracted_at` = source.`_extracted_at`",
            "target.`_is_deleted` = FALSE",
        ]
    )
    insert_columns = [f"`{column}`" for column in source_columns]
    insert_columns.extend(["`_extracted_at`", "`_inserted_at`", "`_is_deleted`"])
    insert_values = [f"source.`{column}`" for column in source_columns]
    insert_values.extend(["source.`_extracted_at`", "applied_at", "FALSE"])

    query = f"""
        DECLARE applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

        MERGE `{table_id}` AS target
        USING `{staging_table_id}` AS source
        ON target.`id` = source.`id`
        WHEN MATCHED THEN
          UPDATE SET
            {", ".join(update_expressions)}
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(insert_columns)})
          VALUES ({", ".join(insert_values)});
    """
    client.query(query).result()


def bigquery_schema(fields: list[dict[str, Any]]) -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField(str(field["name"]), str(field["type"]), mode=str(field["mode"]))
        for field in fields
    ]


def safe_identifier(value: str) -> str:
    safe_value = "".join(character if character.isalnum() else "_" for character in value)
    return safe_value.strip("_") or "run"


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
