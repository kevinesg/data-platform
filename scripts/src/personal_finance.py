from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

import google.auth
import gspread
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from gspread.utils import ValueRenderOption, rowcol_to_a1

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
SCHEMA_DIR = SCRIPTS_DIR / "schemas" / "personal_finance"
PREFERRED_DEV_ENV_FILE = Path.home() / "dev/secrets/data-platform/.env"
GOOGLE_API_SCOPES = (
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
)
EXTRACTION_SUCCESS_MARKER = "_SUCCESS"
TABLES = ("transactions", "paid_for_others", "transfers", "accounts")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run personal_finance extract/load steps.")
    parser.add_argument("--step", required=True, choices=["extract", "load"])
    parser.add_argument("--entity", choices=TABLES)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="replace the raw table from the staged run instead of applying an incremental merge",
    )
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
            load(run_id=args.run_id, entity_name=args.entity, full_refresh=args.full_refresh)
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
    for table in select_tables(entity_name):
        schema = json.loads((SCHEMA_DIR / f"{table}.json").read_text(encoding="utf-8"))
        source_fields = source_schema_fields(schema)
        worksheet = spreadsheet.worksheet(table)
        raw_table = f"personal_finance__{table}"
        prefix = f"{config['prefix']}/{table}"
        run_prefix = f"{prefix}/{run_id}/"
        success_marker_blob_name = f"{run_prefix}{EXTRACTION_SUCCESS_MARKER}"

        if bucket.blob(success_marker_blob_name).exists():
            print(f"entity={table}")
            print(
                f"skipped_completed_extract=true marker=gs://{config['bucket_name']}/"
                f"{success_marker_blob_name}"
            )
            continue
        if next(iter(bucket.list_blobs(prefix=run_prefix)), None) is not None:
            raise ValueError(
                f"incomplete extraction objects already exist under "
                f"gs://{config['bucket_name']}/{run_prefix}; use a new run ID"
            )

        counts = extract_sheet_chunks_to_gcs(
            worksheet=worksheet,
            bucket=bucket,
            bucket_name=config["bucket_name"],
            prefix=prefix,
            run_id=run_id,
            chunk_size=config["chunk_size"],
            source_schema_fields=source_fields,
            extracted_at=extracted_at,
        )
        bucket.blob(success_marker_blob_name).upload_from_string(
            "",
            content_type="text/plain",
            if_generation_match=0,
        )

        print(f"entity={table}")
        print(f"raw_table={raw_table}")
        print(f"source_rows={counts['source_rows']}")
        print(f"extracted_rows={counts['extracted_rows']}")
        print(f"source_id_rows={counts['source_id_rows']}")
        print(f"extract_chunk_count={counts['extract_chunk_count']}")
        print(f"source_id_chunk_count={counts['source_id_chunk_count']}")
        print(f"gcs_prefix=gs://{config['bucket_name']}/{prefix}/{run_id}/extract/")
        print(f"success_marker=gs://{config['bucket_name']}/{success_marker_blob_name}")


def load(run_id: str, entity_name: str | None = None, full_refresh: bool = False) -> None:
    config = load_config()
    credentials, _ = google.auth.default(scopes=GOOGLE_API_SCOPES)
    bq_client = bigquery.Client(project=config["project_id"], credentials=credentials)
    storage_client = storage.Client(project=config["project_id"], credentials=credentials)
    bucket = storage_client.bucket(config["bucket_name"])

    print(f"run_id={run_id}")
    for table in select_tables(entity_name):
        schema = json.loads((SCHEMA_DIR / f"{table}.json").read_text(encoding="utf-8"))
        source_fields = source_schema_fields(schema)
        extracted_at_field = next(
            field for field in schema["fields"] if field["name"] == "_extracted_at"
        )
        prefix = f"{config['prefix']}/{table}"
        success_marker_blob_name = f"{prefix}/{run_id}/{EXTRACTION_SUCCESS_MARKER}"
        if not bucket.blob(success_marker_blob_name).exists():
            raise ValueError(
                f"completed extraction marker not found: "
                f"gs://{config['bucket_name']}/{success_marker_blob_name}"
            )
        extract_uris = list_jsonl_uris(
            bucket,
            config["bucket_name"],
            f"{prefix}/{run_id}/extract/",
        )
        source_id_uris = list_jsonl_uris(
            bucket,
            config["bucket_name"],
            f"{prefix}/{run_id}/source_ids/",
        )
        if not source_id_uris:
            raise ValueError(
                f"no source id files found under "
                f"gs://{config['bucket_name']}/{prefix}/{run_id}/source_ids/"
            )
        if not extract_uris:
            raise ValueError(
                f"no extract files found under "
                f"gs://{config['bucket_name']}/{prefix}/{run_id}/extract/"
            )
        table_id = f"{config['project_id']}.{config['dataset']}.personal_finance__{table}"
        ensure_raw_table(bq_client, table_id, schema["fields"])

        safe_run_id = "".join(character if character.isalnum() else "_" for character in run_id)
        safe_run_id = safe_run_id.strip("_") or "run"
        extract_staging_table_id = f"{table_id}__extract_{safe_run_id}"
        source_ids_staging_table_id = f"{table_id}__source_ids_{safe_run_id}"
        bq_client.delete_table(extract_staging_table_id, not_found_ok=True)
        bq_client.delete_table(source_ids_staging_table_id, not_found_ok=True)
        try:
            loaded_rows = load_extract_files_to_staging(
                bq_client,
                extract_uris,
                extract_staging_table_id,
                [*source_fields, extracted_at_field],
            )
            source_id_rows = load_source_ids_to_staging(
                bq_client,
                source_id_uris,
                source_ids_staging_table_id,
            )
            apply_raw_table_transaction(
                bq_client,
                table_id,
                extract_staging_table_id,
                source_ids_staging_table_id,
                source_fields,
                full_refresh,
            )

            print(f"entity={table}")
            print(f"loaded_rows={loaded_rows}")
            print(f"source_id_rows={source_id_rows}")
            print(f"full_refresh={str(full_refresh).lower()}")
            print(f"applied_table={table_id}")
        finally:
            bq_client.delete_table(extract_staging_table_id, not_found_ok=True)
            bq_client.delete_table(source_ids_staging_table_id, not_found_ok=True)


def load_config() -> dict[str, Any]:
    prefix = env("PERSONAL_FINANCE_GCS_PREFIX").strip("/")
    if not prefix:
        raise ValueError("PERSONAL_FINANCE_GCS_PREFIX must not be empty")
    chunk_size_value = env("PERSONAL_FINANCE_CHUNK_SIZE")
    try:
        chunk_size = int(chunk_size_value)
    except ValueError as exc:
        raise ValueError("PERSONAL_FINANCE_CHUNK_SIZE must be a positive integer") from exc
    if chunk_size <= 0:
        raise ValueError("PERSONAL_FINANCE_CHUNK_SIZE must be a positive integer")

    return {
        "project_id": env("PROJECT_ID"),
        "dataset": env("RAW_DATASET"),
        "gsheet_url": env("PERSONAL_FINANCE_GSHEET_URL"),
        "bucket_name": env("PERSONAL_FINANCE_GCS_BUCKET"),
        "prefix": prefix,
        "chunk_size": chunk_size,
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


def source_schema_fields(schema: dict[str, Any]) -> list[dict[str, Any]]:
    return [field for field in schema["fields"] if field.get("source_field")]


def select_tables(entity_name: str | None = None) -> tuple[str, ...]:
    if entity_name is None:
        return TABLES
    return (entity_name,)


def extract_sheet_chunks_to_gcs(
    worksheet: gspread.Worksheet,
    bucket: storage.Bucket,
    bucket_name: str,
    prefix: str,
    run_id: str,
    chunk_size: int,
    source_schema_fields: list[dict[str, Any]],
    extracted_at: str,
) -> dict[str, int]:
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
    source_id_rows = 0
    extract_chunk_count = 0
    source_id_chunk_count = 0

    for start_row in range(2, worksheet.row_count + 1, chunk_size):
        end_row = min(start_row + chunk_size - 1, worksheet.row_count)
        rows = worksheet.get(
            f"A{start_row}:{rowcol_to_a1(end_row, len(headers))}",
            value_render_option=ValueRenderOption.formatted,
        )
        if not rows:
            break

        extract_chunk = []
        source_id_chunk = []
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

            source_id_chunk.append({"id": record["id"]})
            output_row = {}
            for field in source_schema_fields:
                name = str(field["name"])
                value = record[name]
                field_type = str(field["type"])
                mode = str(field["mode"])
                try:
                    if value == "" and mode == "NULLABLE":
                        output_row[name] = None
                    elif value == "" and mode == "REQUIRED":
                        raise ValueError("required value is empty")
                    elif field_type == "INTEGER":
                        output_row[name] = int(value)
                    elif field_type == "FLOAT":
                        output_row[name] = float(value)
                    elif field_type == "BOOLEAN":
                        normalized = value.lower()
                        if normalized in {"true", "1"}:
                            output_row[name] = True
                        elif normalized in {"false", "0"}:
                            output_row[name] = False
                        else:
                            raise ValueError("expected boolean")
                    else:
                        output_row[name] = value
                except ValueError as exc:
                    raise ValueError(f"row {source_row_number} invalid {name}: {exc}") from exc
            output_row["_extracted_at"] = extracted_at
            extract_chunk.append(output_row)

        if extract_chunk:
            extract_chunk_count += 1
            blob_name = f"{prefix}/{run_id}/extract/chunk-{extract_chunk_count:06d}.jsonl"
            upload_jsonl(bucket, blob_name, extract_chunk)
            extracted_rows += len(extract_chunk)
            print(f"wrote {len(extract_chunk)} rows to gs://{bucket_name}/{blob_name}")

        if source_id_chunk:
            source_id_chunk_count += 1
            blob_name = f"{prefix}/{run_id}/source_ids/chunk-{source_id_chunk_count:06d}.jsonl"
            upload_jsonl(bucket, blob_name, source_id_chunk)
            source_id_rows += len(source_id_chunk)

    return {
        "source_rows": source_rows,
        "extracted_rows": extracted_rows,
        "source_id_rows": source_id_rows,
        "extract_chunk_count": extract_chunk_count,
        "source_id_chunk_count": source_id_chunk_count,
    }


def list_jsonl_uris(
    bucket: storage.Bucket,
    bucket_name: str,
    prefix: str,
) -> list[str]:
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in sorted(bucket.list_blobs(prefix=prefix), key=lambda blob: blob.name)
        if blob.name.endswith(".jsonl")
    ]


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
    extract_uris: list[str],
    staging_table_id: str,
    fields: list[dict[str, Any]],
) -> int:
    load_job = client.load_table_from_uri(
        extract_uris,
        staging_table_id,
        job_config=bigquery.LoadJobConfig(
            schema=bigquery_schema(fields),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()
    return load_job.output_rows or 0


def load_source_ids_to_staging(
    client: bigquery.Client,
    source_id_uris: list[str],
    staging_table_id: str,
) -> int:
    load_job = client.load_table_from_uri(
        source_id_uris,
        staging_table_id,
        job_config=bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("id", "STRING", mode="REQUIRED")],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()
    return load_job.output_rows or 0


def apply_raw_table_transaction(
    client: bigquery.Client,
    table_id: str,
    extract_staging_table_id: str,
    source_ids_staging_table_id: str,
    source_schema_fields: list[dict[str, Any]],
    full_refresh: bool,
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

    if full_refresh:
        write_statement = f"""
        DELETE FROM `{table_id}` WHERE TRUE;

        INSERT INTO `{table_id}` ({", ".join(insert_columns)})
        SELECT {", ".join(insert_values)}
        FROM `{extract_staging_table_id}` AS source;
        """
    else:
        write_statement = f"""
        MERGE `{table_id}` AS target
        USING `{extract_staging_table_id}` AS source
        ON target.`id` = source.`id`
        WHEN MATCHED THEN
          UPDATE SET
            {", ".join(update_expressions)}
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(insert_columns)})
          VALUES ({", ".join(insert_values)});

        UPDATE `{table_id}` AS target
        SET target.`_is_deleted` = TRUE
        WHERE NOT target.`_is_deleted`
          AND NOT EXISTS (
            SELECT 1
            FROM `{source_ids_staging_table_id}` AS source_ids
            WHERE source_ids.`id` = target.`id`
          );
        """

    query = f"""
        DECLARE applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

        ASSERT (
          SELECT COUNT(*) = COUNT(DISTINCT `id`)
          FROM `{source_ids_staging_table_id}`
        ) AS 'source snapshot contains duplicate ids';

        BEGIN TRANSACTION;

        {write_statement}

        COMMIT TRANSACTION;
    """
    client.query(query).result()


def bigquery_schema(fields: list[dict[str, Any]]) -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField(str(field["name"]), str(field["type"]), mode=str(field["mode"]))
        for field in fields
    ]


def upload_jsonl(bucket: storage.Bucket, blob_name: str, rows: list[dict[str, Any]]) -> None:
    body = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)
    bucket.blob(blob_name).upload_from_string(body, content_type="application/x-ndjson")


def env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"missing required environment variable: {name}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
