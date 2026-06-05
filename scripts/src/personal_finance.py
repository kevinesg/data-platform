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
from gspread.utils import ValueRenderOption, rowcol_to_a1

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = SCRIPTS_DIR / ".env"
SCHEMA_DIR = SCRIPTS_DIR / "schemas"
DEFAULT_OUTPUT_DIR = SCRIPTS_DIR / ".local" / "personal_finance"


@dataclass(frozen=True)
class SourceEntity:
    name: str
    sheet_name: str
    raw_table: str
    schema_path: Path


SOURCE_ENTITIES = (
    SourceEntity(
        name="transactions",
        sheet_name="transactions",
        raw_table="personal_finance__transactions",
        schema_path=SCHEMA_DIR / "personal_finance__transactions.json",
    ),
    SourceEntity(
        name="paid_for_others",
        sheet_name="paid_for_others",
        raw_table="personal_finance__paid_for_others",
        schema_path=SCHEMA_DIR / "personal_finance__paid_for_others.json",
    ),
    SourceEntity(
        name="transfers",
        sheet_name="transfers",
        raw_table="personal_finance__transfers",
        schema_path=SCHEMA_DIR / "personal_finance__transfers.json",
    ),
    SourceEntity(
        name="accounts",
        sheet_name="accounts",
        raw_table="personal_finance__accounts",
        schema_path=SCHEMA_DIR / "personal_finance__accounts.json",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract personal_finance Google Sheets rows to local JSONL records."
    )
    parser.add_argument("--entity", choices=[entity.name for entity in SOURCE_ENTITIES])
    parser.add_argument("--run-id")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="local output directory for extracted JSONL chunks",
    )
    args = parser.parse_args()

    if DEFAULT_ENV_FILE.exists():
        load_dotenv(DEFAULT_ENV_FILE, override=False)

    run_id = args.run_id or dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    try:
        extract_local(run_id=run_id, output_dir=args.output_dir, entity_name=args.entity)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


def extract_local(run_id: str, output_dir: Path, entity_name: str | None = None) -> None:
    config = load_extract_config()
    extracted_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    spreadsheet = gspread.service_account(filename=config["credentials"]).open_by_url(
        config["gsheet_url"]
    )

    print(f"run_id={run_id}")
    for entity in select_entities(entity_name):
        schema = load_entity_schema(entity)
        source_fields = source_schema_fields(schema)
        worksheet = spreadsheet.worksheet(entity.sheet_name)
        entity_output_dir = output_dir / entity.name / run_id / "extract"

        source_rows, extracted_rows, chunk_count = extract_sheet_chunks_to_local(
            worksheet=worksheet,
            output_dir=entity_output_dir,
            chunk_size=config["chunk_size"],
            source_schema_fields=source_fields,
            extracted_at=extracted_at,
        )

        print(f"entity={entity.name}")
        print(f"raw_table={entity.raw_table}")
        print(f"source_rows={source_rows}")
        print(f"extracted_rows={extracted_rows}")
        print(f"chunk_count={chunk_count}")
        print(f"output_dir={entity_output_dir}")


def load_extract_config() -> dict[str, Any]:
    return {
        "credentials": env("GOOGLE_APPLICATION_CREDENTIALS"),
        "gsheet_url": env("PERSONAL_FINANCE_GSHEET_URL"),
        "chunk_size": positive_int_env("PERSONAL_FINANCE_CHUNK_SIZE"),
    }


def load_entity_schema(entity: SourceEntity) -> dict[str, Any]:
    return json.loads(entity.schema_path.read_text(encoding="utf-8"))


def source_schema_fields(schema: dict[str, Any]) -> list[dict[str, Any]]:
    return [field for field in schema["fields"] if field.get("source_field")]


def select_entities(entity_name: str | None = None) -> tuple[SourceEntity, ...]:
    if entity_name is None:
        return SOURCE_ENTITIES
    return tuple(entity for entity in SOURCE_ENTITIES if entity.name == entity_name)


def extract_sheet_chunks_to_local(
    worksheet: gspread.Worksheet,
    output_dir: Path,
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

    output_dir.mkdir(parents=True, exist_ok=True)
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
            chunk_path = output_dir / f"chunk-{chunk_count:06d}.jsonl"
            write_jsonl(chunk_path, extract_chunk)
            extracted_rows += len(extract_chunk)

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


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    body = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)
    path.write_text(body, encoding="utf-8")


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
