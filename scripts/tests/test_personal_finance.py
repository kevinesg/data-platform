import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

import personal_finance  # noqa: E402


def set_required_env(monkeypatch, chunk_size="250"):
    monkeypatch.setenv("PROJECT_ID", "kevinesg-dev")
    monkeypatch.setenv("RAW_DATASET", "raw")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/service-account.json")
    monkeypatch.setenv(
        "PERSONAL_FINANCE_GSHEET_URL",
        "https://docs.google.com/spreadsheets/d/example/edit",
    )
    monkeypatch.setenv("PERSONAL_FINANCE_GCS_BUCKET", "landing-bucket")
    monkeypatch.setenv("PERSONAL_FINANCE_GCS_PREFIX", "personal_finance/")
    monkeypatch.setenv("PERSONAL_FINANCE_CHUNK_SIZE", chunk_size)


def test_source_entities_define_expected_google_sheet_contract():
    entities = {entity.name: entity for entity in personal_finance.SOURCE_ENTITIES}

    assert list(entities) == ["transactions", "paid_for_others", "transfers", "accounts"]
    assert entities["transactions"].sheet_name == "transactions"
    assert entities["paid_for_others"].raw_table == "personal_finance__paid_for_others"
    assert entities["transfers"].gcs_prefix == "transfers"
    assert entities["accounts"].schema_path.name == "personal_finance__accounts.json"


def test_select_entities_can_limit_work_to_one_source_entity():
    selected = personal_finance.select_entities("transfers")

    assert [entity.name for entity in selected] == ["transfers"]


def test_source_schemas_reflect_current_sheet_columns():
    schemas = {
        entity.name: json.loads(entity.schema_path.read_text(encoding="utf-8"))
        for entity in personal_finance.SOURCE_ENTITIES
    }
    source_columns = {
        name: [field["name"] for field in schema["fields"] if field.get("source_field")]
        for name, schema in schemas.items()
    }

    assert "posted_date" in source_columns["transactions"]
    assert source_columns["paid_for_others"][:2] == ["id", "posted_date"]
    assert "year" not in source_columns["paid_for_others"]
    assert "month" not in source_columns["paid_for_others"]
    assert "day" not in source_columns["paid_for_others"]

    transfer_fields = {field["name"]: field for field in schemas["transfers"]["fields"]}
    assert transfer_fields["destination"]["mode"] == "NULLABLE"
    assert transfer_fields["destination_amount"]["mode"] == "NULLABLE"


def test_coerce_value_converts_supported_types():
    assert personal_finance.coerce_value("123", "INTEGER", "REQUIRED") == 123
    assert personal_finance.coerce_value("10.5", "FLOAT", "REQUIRED") == 10.5
    assert personal_finance.coerce_value("true", "BOOLEAN", "REQUIRED") is True
    assert personal_finance.coerce_value("0", "BOOLEAN", "REQUIRED") is False
    assert personal_finance.coerce_value("abc", "STRING", "REQUIRED") == "abc"
    assert personal_finance.coerce_value("", "FLOAT", "NULLABLE") is None


@pytest.mark.parametrize(
    ("value", "field_type", "mode", "message"),
    [
        ("", "STRING", "REQUIRED", "required value is empty"),
        ("not-a-number", "INTEGER", "REQUIRED", "invalid literal"),
        ("maybe", "BOOLEAN", "REQUIRED", "expected boolean"),
    ],
)
def test_coerce_value_reports_invalid_values(value, field_type, mode, message):
    with pytest.raises(ValueError, match=message):
        personal_finance.coerce_value(value, field_type, mode)


def test_coerce_record_adds_row_context_to_errors():
    schema_fields = [
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "cost", "type": "FLOAT", "mode": "REQUIRED"},
    ]
    record = {"id": "txn-1", "cost": "not-a-float"}

    with pytest.raises(ValueError, match="row 7 invalid cost"):
        personal_finance.coerce_record(record, schema_fields, source_row_number=7)


def test_load_config_reads_required_environment(monkeypatch):
    set_required_env(monkeypatch)

    config = personal_finance.load_config()

    assert config["project_id"] == "kevinesg-dev"
    assert config["dataset"] == "raw"
    assert config["credentials"] == "/tmp/service-account.json"
    assert config["gsheet_url"].startswith("https://docs.google.com/")
    assert config["bucket_name"] == "landing-bucket"
    assert config["prefix"] == "personal_finance"
    assert config["chunk_size"] == 250


def test_entity_gcs_prefix_appends_entity_path(monkeypatch):
    set_required_env(monkeypatch)
    config = personal_finance.load_config()

    assert (
        personal_finance.entity_gcs_prefix(config, personal_finance.SOURCE_ENTITIES[1])
        == "personal_finance/paid_for_others"
    )


@pytest.mark.parametrize("chunk_size", ["0", "-1", "not-an-int"])
def test_load_config_rejects_invalid_chunk_size(monkeypatch, chunk_size):
    set_required_env(monkeypatch, chunk_size=chunk_size)

    with pytest.raises(ValueError, match="PERSONAL_FINANCE_CHUNK_SIZE must be a positive integer"):
        personal_finance.load_config()


def test_load_config_rejects_empty_prefix(monkeypatch):
    set_required_env(monkeypatch)
    monkeypatch.setenv("PERSONAL_FINANCE_GCS_PREFIX", "/")

    with pytest.raises(ValueError, match="PERSONAL_FINANCE_GCS_PREFIX must not be empty"):
        personal_finance.load_config()


class FakeWorksheet:
    def __init__(self, headers, rows):
        self.headers = headers
        self.rows = rows
        self.row_count = len(rows) + 1

    def row_values(self, row_number):
        assert row_number == 1
        return self.headers

    def get(self, cell_range, value_render_option=None):
        del value_render_option
        start_row = int(cell_range.split(":", 1)[0].removeprefix("A"))
        end_cell = cell_range.rsplit(":", 1)[1]
        end_row = int("".join(character for character in end_cell if character.isdigit()))
        return self.rows[start_row - 2 : end_row - 1]


class FakeUploadedBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name

    def upload_from_string(self, body, content_type=None):
        self.bucket.uploads[self.name] = {
            "body": body,
            "content_type": content_type,
        }


class FakeBucket:
    def __init__(self):
        self.uploads = {}

    def blob(self, name):
        return FakeUploadedBlob(self, name)


def test_extract_sheet_chunks_to_gcs_uploads_jsonl_chunks():
    worksheet = FakeWorksheet(
        headers=["id", "amount", "is_active"],
        rows=[
            ["row-1", "10.5", "true"],
            ["row-2", "", "0"],
            ["", "", ""],
        ],
    )
    fields = [
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "is_active", "type": "BOOLEAN", "mode": "REQUIRED"},
    ]
    bucket = FakeBucket()

    source_rows, extracted_rows, chunk_count = personal_finance.extract_sheet_chunks_to_gcs(
        worksheet=worksheet,
        bucket=bucket,
        bucket_name="landing-bucket",
        prefix="personal_finance/transactions",
        run_id="run-1",
        chunk_size=1,
        source_schema_fields=fields,
        extracted_at="2026-06-06T00:00:00Z",
    )

    assert source_rows == 2
    assert extracted_rows == 2
    assert chunk_count == 2
    assert list(bucket.uploads) == [
        "personal_finance/transactions/run-1/extract/chunk-000001.jsonl",
        "personal_finance/transactions/run-1/extract/chunk-000002.jsonl",
    ]
    assert (
        bucket.uploads["personal_finance/transactions/run-1/extract/chunk-000001.jsonl"][
            "content_type"
        ]
        == "application/x-ndjson"
    )
    first_chunk = bucket.uploads[
        "personal_finance/transactions/run-1/extract/chunk-000001.jsonl"
    ]["body"].splitlines()
    second_chunk = bucket.uploads[
        "personal_finance/transactions/run-1/extract/chunk-000002.jsonl"
    ]["body"].splitlines()
    assert json.loads(first_chunk[0]) == {
        "id": "row-1",
        "amount": 10.5,
        "is_active": True,
        "_extracted_at": "2026-06-06T00:00:00Z",
    }
    assert json.loads(second_chunk[0]) == {
        "id": "row-2",
        "amount": None,
        "is_active": False,
        "_extracted_at": "2026-06-06T00:00:00Z",
    }


def test_extract_sheet_chunks_to_gcs_requires_schema_headers():
    worksheet = FakeWorksheet(headers=["id"], rows=[["row-1"]])
    fields = [
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
    ]

    with pytest.raises(ValueError, match="missing source columns: amount"):
        personal_finance.extract_sheet_chunks_to_gcs(
            worksheet=worksheet,
            bucket=FakeBucket(),
            bucket_name="landing-bucket",
            prefix="personal_finance/transactions",
            run_id="run-1",
            chunk_size=100,
            source_schema_fields=fields,
            extracted_at="2026-06-06T00:00:00Z",
        )
