import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

import personal_finance  # noqa: E402


def set_required_env(monkeypatch, chunk_size="250"):
    monkeypatch.setenv("PROJECT_ID", "kevinesg-dev")
    monkeypatch.setenv("RAW_DATASET", "raw_kevinesg")
    monkeypatch.setenv(
        "PERSONAL_FINANCE_GSHEET_URL",
        "https://docs.google.com/spreadsheets/d/example/edit",
    )
    monkeypatch.setenv(
        "PERSONAL_FINANCE_GCS_BUCKET",
        "kevinesg-dev-data-platform-landing-kevinesg",
    )
    monkeypatch.setenv("PERSONAL_FINANCE_GCS_PREFIX", "personal_finance/")
    monkeypatch.setenv("PERSONAL_FINANCE_CHUNK_SIZE", chunk_size)


def test_tables_define_expected_google_sheet_contract():
    assert personal_finance.TABLES == (
        "transactions",
        "paid_for_others",
        "transfers",
        "accounts",
    )
    assert [
        path.relative_to(personal_finance.SCHEMA_DIR).as_posix()
        for path in sorted(personal_finance.SCHEMA_DIR.glob("*.json"))
    ] == [
        "accounts.json",
        "paid_for_others.json",
        "transactions.json",
        "transfers.json",
    ]


def test_select_tables_can_limit_work_to_one_source_entity():
    selected = personal_finance.select_tables("transfers")

    assert selected == ("transfers",)


def test_source_schemas_reflect_current_sheet_columns():
    schemas = {
        table: json.loads((personal_finance.SCHEMA_DIR / f"{table}.json").read_text())
        for table in personal_finance.TABLES
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


def test_load_config_reads_required_environment(monkeypatch):
    set_required_env(monkeypatch)

    config = personal_finance.load_config()

    assert config["project_id"] == "kevinesg-dev"
    assert config["dataset"] == "raw_kevinesg"
    assert config["gsheet_url"].startswith("https://docs.google.com/")
    assert config["bucket_name"] == "kevinesg-dev-data-platform-landing-kevinesg"
    assert config["prefix"] == "personal_finance"
    assert config["chunk_size"] == 250


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

    def upload_from_string(self, body, content_type=None, **kwargs):
        self.bucket.uploads[self.name] = {
            "body": body,
            "content_type": content_type,
            **kwargs,
        }

    def exists(self):
        return self.name in self.bucket.uploads


class FakeBucket:
    def __init__(self):
        self.name = "landing-bucket"
        self.uploads = {}

    def blob(self, name):
        return FakeUploadedBlob(self, name)

    def list_blobs(self, prefix):
        return [FakeUploadedBlob(self, name) for name in self.uploads if name.startswith(prefix)]


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

    result = personal_finance.extract_sheet_chunks_to_gcs(
        worksheet=worksheet,
        bucket=bucket,
        bucket_name="landing-bucket",
        prefix="personal_finance/transactions",
        run_id="run-1",
        chunk_size=1,
        source_schema_fields=fields,
        extracted_at="2026-06-06T00:00:00Z",
    )

    assert result == {
        "source_rows": 2,
        "extracted_rows": 2,
        "source_id_rows": 2,
        "extract_chunk_count": 2,
        "source_id_chunk_count": 2,
    }
    assert list(bucket.uploads) == [
        "personal_finance/transactions/run-1/extract/chunk-000001.jsonl",
        "personal_finance/transactions/run-1/source_ids/chunk-000001.jsonl",
        "personal_finance/transactions/run-1/extract/chunk-000002.jsonl",
        "personal_finance/transactions/run-1/source_ids/chunk-000002.jsonl",
    ]
    assert (
        bucket.uploads["personal_finance/transactions/run-1/extract/chunk-000001.jsonl"][
            "content_type"
        ]
        == "application/x-ndjson"
    )
    first_chunk = bucket.uploads["personal_finance/transactions/run-1/extract/chunk-000001.jsonl"][
        "body"
    ].splitlines()
    second_chunk = bucket.uploads["personal_finance/transactions/run-1/extract/chunk-000002.jsonl"][
        "body"
    ].splitlines()
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
    first_source_id_chunk = bucket.uploads[
        "personal_finance/transactions/run-1/source_ids/chunk-000001.jsonl"
    ]["body"].splitlines()
    assert json.loads(first_source_id_chunk[0]) == {"id": "row-1"}


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


def test_extract_sheet_chunks_to_gcs_reports_invalid_values_with_row_context():
    worksheet = FakeWorksheet(headers=["id", "cost"], rows=[["row-1", "not-a-float"]])
    fields = [
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "cost", "type": "FLOAT", "mode": "REQUIRED"},
    ]

    with pytest.raises(ValueError, match="row 2 invalid cost"):
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


def test_list_jsonl_uris_uses_only_objects_under_the_requested_prefix():
    bucket = FakeBucket()
    bucket.blob("personal_finance/accounts/run-1/source_ids/chunk-000002.jsonl").upload_from_string(
        ""
    )
    bucket.blob("personal_finance/accounts/run-1/source_ids/chunk-000001.jsonl").upload_from_string(
        ""
    )
    bucket.blob("personal_finance/accounts/run-1/_SUCCESS").upload_from_string("")

    uris = personal_finance.list_jsonl_uris(
        bucket,
        "landing-bucket",
        "personal_finance/accounts/run-1/source_ids/",
    )

    assert uris == [
        "gs://landing-bucket/personal_finance/accounts/run-1/source_ids/chunk-000001.jsonl",
        "gs://landing-bucket/personal_finance/accounts/run-1/source_ids/chunk-000002.jsonl",
    ]


class FakeQueryJob:
    def result(self):
        return None


class FakeQueryClient:
    def __init__(self):
        self.query_text = None

    def query(self, query_text):
        self.query_text = query_text
        return FakeQueryJob()


def test_raw_table_transaction_upserts_and_marks_missing_source_ids_deleted():
    client = FakeQueryClient()

    personal_finance.apply_raw_table_transaction(
        client,
        "project.raw.personal_finance__accounts",
        "project.raw.personal_finance__accounts__extract_run_1",
        "project.raw.personal_finance__accounts__source_ids_run_1",
        [
            {"name": "id"},
            {"name": "name"},
        ],
        full_refresh=False,
    )

    query = client.query_text
    assert "BEGIN TRANSACTION;" in query
    assert "MERGE `project.raw.personal_finance__accounts`" in query
    assert "source snapshot contains duplicate ids" in query
    assert "SET target.`_is_deleted` = TRUE" in query
    assert "NOT EXISTS" in query
    assert "COMMIT TRANSACTION;" in query


def test_raw_table_transaction_can_full_refresh_from_extract_stage():
    client = FakeQueryClient()

    personal_finance.apply_raw_table_transaction(
        client,
        "project.raw.personal_finance__accounts",
        "project.raw.personal_finance__accounts__extract_run_1",
        "project.raw.personal_finance__accounts__source_ids_run_1",
        [
            {"name": "id"},
            {"name": "name"},
        ],
        full_refresh=True,
    )

    query = client.query_text
    assert "DELETE FROM `project.raw.personal_finance__accounts` WHERE TRUE" in query
    assert "INSERT INTO `project.raw.personal_finance__accounts`" in query
    assert "MERGE `project.raw.personal_finance__accounts`" not in query
    assert "SET target.`_is_deleted` = TRUE" not in query
