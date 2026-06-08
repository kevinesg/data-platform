# personal_finance

The `personal_finance` pipeline treats a Google Sheet as an external source
system. Its current source contract contains four tabs:

| Sheet tab | Raw table | Purpose |
| --- | --- | --- |
| `transactions` | `personal_finance__transactions` | Income and expense rows that affect cashflow. |
| `paid_for_others` | `personal_finance__paid_for_others` | Payments made for other people. |
| `transfers` | `personal_finance__transfers` | Internal account movements plus direct inflows and outflows. |
| `accounts` | `personal_finance__accounts` | Account attributes used for balance reporting. |

The script assumes the sheet tab, schema file, and GCS entity prefix share the
same table name:

```text
Google Sheet tab: <table_name>
Schema file: scripts/schemas/personal_finance/<table_name>.json
GCS prefix: personal_finance/<table_name>/
Raw table: personal_finance__<table_name>
```

Source exports, environment files, credentials, and warehouse data stay outside
version control.

## Prerequisites

Complete the shared dev and developer-workspace sections of
`deploy/README.md`. The resulting workspace contains:

- the shared `kevinesg-dev` project;
- a per-developer scripts service account;
- a per-developer GCS landing bucket;
- a per-developer BigQuery raw dataset;
- keyless Application Default Credentials created through service account
  impersonation; and
- an external environment file, defaulting to
  `$HOME/dev/secrets/data-platform/.env`.

The commands below assume these variables remain available from workspace
provisioning:

```bash
export PROJECT_ID=kevinesg-dev
export DEVELOPER_ID=kevinesg
export SCRIPTS_SERVICE_ACCOUNT_EMAIL="data-platform-scripts-${DEVELOPER_ID}@${PROJECT_ID}.iam.gserviceaccount.com"
export RAW_DATASET="raw_${DEVELOPER_ID}"
export LANDING_BUCKET="${PROJECT_ID}-data-platform-landing-${DEVELOPER_ID}"
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$DATA_PLATFORM_SECRETS_DIR/.env}"
```

`DEVELOPER_ID` uses the assigned stable identifier. Either configuration
variable can select another external directory or file before these commands
run.

## Google Sheets Access

The Google Sheets and Google Drive services are shared project settings. They
are enabled once by a platform maintainer, not by each developer. The maintainer
runs this guarded bootstrap from the `data-platform-bootstrap-dev` gcloud
configuration:

```bash
gcloud config configurations activate data-platform-bootstrap-dev

enable_missing_source_services() {
  local required_source_services=(
    sheets.googleapis.com
    drive.googleapis.com
  )
  local enabled_source_services
  local missing_source_services=()

  enabled_source_services="$(
    gcloud services list \
      --enabled \
      --project="$PROJECT_ID" \
      --format='value(config.name)'
  )" || return 1

  for required_service in "${required_source_services[@]}"; do
    if ! printf '%s\n' "$enabled_source_services" |
      grep -Fxq "$required_service"; then
      missing_source_services+=("$required_service")
    fi
  done

  if ((${#missing_source_services[@]})); then
    gcloud services enable \
      "${missing_source_services[@]}" \
      --project="$PROJECT_ID"
  else
    echo "Google Sheets and Google Drive services are enabled."
  fi
}

enable_missing_source_services

gcloud services list \
  --enabled \
  --project="$PROJECT_ID" \
  --filter='config.name~"^(drive|sheets)\.googleapis\.com$"' \
  --format='value(config.name)' \
  --sort-by='config.name'

gcloud config configurations activate "data-platform-dev-${DEVELOPER_ID}"
```

Both services must appear, and the developer configuration must be active,
before extraction runs.

The development workbook must contain approved source data and must not be a
production workbook unless its owner has explicitly approved that use. For a
team, use a sanitized workbook per developer or another controlled dev source
that preserves the required schema and edge cases.

Share the selected workbook with the per-developer service account:

1. Open the workbook in Google Sheets.
2. Select **Share**.
3. Add the value of `SCRIPTS_SERVICE_ACCOUNT_EMAIL`.
4. Grant `Viewer` access.
5. Confirm that the workbook contains `transactions`, `paid_for_others`,
   `transfers`, and `accounts`.
6. Confirm that each tab's headers match the `source_field` entries in the
   corresponding `scripts/schemas/personal_finance/<table_name>.json` file.

## Environment Configuration

Open `$DATA_PLATFORM_ENV_FILE` and set the complete pipeline contract:

```dotenv
ENVIRONMENT=dev
DEVELOPER_ID=kevinesg
PROJECT_ID=kevinesg-dev
RAW_DATASET=raw_kevinesg

PERSONAL_FINANCE_GSHEET_URL=https://docs.google.com/spreadsheets/d/.../edit
PERSONAL_FINANCE_GCS_BUCKET=kevinesg-dev-data-platform-landing-kevinesg
PERSONAL_FINANCE_GCS_PREFIX=personal_finance
PERSONAL_FINANCE_CHUNK_SIZE=5000
PERSONAL_FINANCE_JSONL_RETENTION_DAYS=7
```

The developer identifier in the dataset and bucket names must match
`DEVELOPER_ID`. Do not add `GOOGLE_APPLICATION_CREDENTIALS`; local
authentication is provided by ADC.

## Configuration Validation

Run from the `scripts/` directory:

```bash
uv sync --locked

uv run python validate_config.py \
  --env-file "$DATA_PLATFORM_ENV_FILE"

uv run python src/personal_finance.py --help
```

Configuration validation must report the selected developer workspace:

```text
scripts config OK
environment=dev
developer_id=kevinesg
project_id=kevinesg-dev
raw_dataset=raw_kevinesg
```

Resolve configuration, ADC, IAM, source sharing, dataset, and bucket errors
before running extraction.

## Dev Extract Validation

Begin with the smallest stable entity so source access, headers, coercion, ADC,
and GCS permissions are checked before warehouse loading:

```bash
export RUN_ID="dev-accounts-$(date -u +%Y%m%dT%H%M%SZ)"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step extract \
  --entity accounts \
  --run-id "$RUN_ID"
```

The command reads the configured tab in chunks, filters and coerces fields
through `schemas/personal_finance/<table_name>.json`, and writes run-scoped row and
source-ID JSONL files under:

```text
gs://<developer-bucket>/personal_finance/accounts/<run-id>/extract/
gs://<developer-bucket>/personal_finance/accounts/<run-id>/source_ids/
```

After all chunks succeed, extraction writes:

```text
gs://<developer-bucket>/personal_finance/accounts/<run-id>/_SUCCESS
```

The empty `_SUCCESS` object is the completion marker for that entity and run.
Load refuses a run without it so a failed partial extraction cannot be
interpreted as a complete source snapshot. Extraction also refuses a run ID
whose prefix contains partial objects without `_SUCCESS`; use a new run ID
instead. Reusing a completed run ID skips that entity without replacing its
snapshot.

Verify object metadata without printing source records:

```bash
gcloud storage ls --long \
  "gs://$LANDING_BUCKET/personal_finance/accounts/$RUN_ID/**"
```

Do not continue to loading until the extract succeeds and the expected objects
are present.

## Dev Raw Load Validation

Load the exact staged run:

```bash
uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step load \
  --entity accounts \
  --run-id "$RUN_ID"
```

After verifying `_SUCCESS`, the load step reads the run-scoped files through
BigQuery staging tables. It rejects duplicate source IDs, then applies row
upserts, reactivations, and source deletion markers in one BigQuery transaction.
Reusing a run ID retries the same staged input.

Inspect the table schema and row count without printing source values:

```bash
bq show \
  --project_id="$PROJECT_ID" \
  --schema \
  --format=prettyjson \
  "$PROJECT_ID:$RAW_DATASET.personal_finance__accounts"

bq query \
  --project_id="$PROJECT_ID" \
  --location=US \
  --use_legacy_sql=false \
  "SELECT
      COUNT(*) AS row_count,
      COUNTIF(_is_deleted) AS deleted_row_count
   FROM \`$PROJECT_ID.$RAW_DATASET.personal_finance__accounts\`"
```

Retry the same load and repeat the count query:

```bash
uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step load \
  --entity accounts \
  --run-id "$RUN_ID"
```

The retry must succeed without increasing the row count.

## Full Refresh

Full refresh replaces one raw table from a completed staged run instead of
merging into the existing table:

```bash
export RUN_ID="dev-accounts-full-refresh-$(date -u +%Y%m%dT%H%M%SZ)"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step extract \
  --entity accounts \
  --run-id "$RUN_ID"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step load \
  --entity accounts \
  --run-id "$RUN_ID" \
  --full-refresh
```

Use this for controlled rebuilds of raw tables after schema or source-contract
changes. A future Airflow task can call the same CLI command and pass
`--full-refresh` for a manual or parameterized rebuild path. Do not use full
refresh as the default scheduled path unless the source volume and downstream
runtime have been sized for it.

## Cleanup Retention

Cleanup removes completed staged runs after
`PERSONAL_FINANCE_JSONL_RETENTION_DAYS`. It only deletes run prefixes that have
an `_SUCCESS` marker older than the retention cutoff. Incomplete run prefixes
are left in place for investigation.

Clean up one known run:

```bash
uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step cleanup \
  --entity accounts \
  --run-id "$RUN_ID"
```

Clean up all completed runs for all personal finance tables:

```bash
uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step cleanup
```

For a fresh validation run, cleanup reports `deleted_files=0` and
`skipped_recent_runs=1` for the selected entity. For an older completed run, it
deletes the run's staged JSONL files and `_SUCCESS` marker. A cleaned run is no
longer load-retryable; create a new extract run before loading again.

## Full Dev Validation

After the single-entity checkpoint succeeds, run all four entities with one new
run ID:

```bash
export RUN_ID="dev-full-$(date -u +%Y%m%dT%H%M%SZ)"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step extract \
  --run-id "$RUN_ID"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step load \
  --run-id "$RUN_ID"
```

Verify that all raw tables exist:

```bash
bq ls \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"
```

Record the command results and any remediation in the development review before
continuing to source deletion validation.

## Dev Source Deletion Validation

Deletion detection compares the complete source-ID snapshot with active raw
rows in BigQuery. IDs absent from a completed snapshot are retained in raw
storage with `_is_deleted = TRUE`; they are not physically deleted. Load rejects
an empty source snapshot. If a source can intentionally become empty later,
handle that as a separate source-contract change.

Before changing the source, verify the new snapshot and transactional load path
against the unchanged `accounts` tab:

```bash
export RUN_ID="dev-deletion-baseline-$(date -u +%Y%m%dT%H%M%SZ)"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step extract \
  --entity accounts \
  --run-id "$RUN_ID"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step load \
  --entity accounts \
  --run-id "$RUN_ID"

bq query \
  --project_id="$PROJECT_ID" \
  --location=US \
  --use_legacy_sql=false \
  "SELECT
      COUNT(*) AS row_count,
      COUNTIF(_is_deleted) AS deleted_row_count
   FROM \`$PROJECT_ID.$RAW_DATASET.personal_finance__accounts\`"
```

The load must complete and the counts must remain consistent with the unchanged
source before deletion behavior is tested.

Use an approved disposable row in one development workbook tab. Preserve the
complete row in the workbook's version history or another approved temporary
location before removing it. Set the row ID without placing source data in the
repository:

```bash
export DELETION_TEST_ENTITY=accounts
export DELETION_TEST_ID='<approved-source-id>'
```

Confirm that the baseline row is active:

```bash
bq query \
  --project_id="$PROJECT_ID" \
  --location=US \
  --use_legacy_sql=false \
  --parameter="deletion_test_id:STRING:$DELETION_TEST_ID" \
  "SELECT id, _is_deleted
   FROM \`$PROJECT_ID.$RAW_DATASET.personal_finance__accounts\`
   WHERE id = @deletion_test_id"
```

Remove exactly that row from the `accounts` tab, then extract and load a new
completed snapshot:

```bash
export RUN_ID="dev-delete-accounts-$(date -u +%Y%m%dT%H%M%SZ)"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step extract \
  --entity "$DELETION_TEST_ENTITY" \
  --run-id "$RUN_ID"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step load \
  --entity "$DELETION_TEST_ENTITY" \
  --run-id "$RUN_ID"
```

Repeat the parameterized query. The row must remain present with
`_is_deleted = TRUE`, while other source rows remain active.

Restore the complete source row before ending validation. Extract and load with
another new run ID:

```bash
export RUN_ID="dev-restore-accounts-$(date -u +%Y%m%dT%H%M%SZ)"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step extract \
  --entity "$DELETION_TEST_ENTITY" \
  --run-id "$RUN_ID"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step load \
  --entity "$DELETION_TEST_ENTITY" \
  --run-id "$RUN_ID"
```

The parameterized query must return `_is_deleted = FALSE`. Compare the entity
row count with the restored source and resolve any discrepancy before
committing the deletion-detection update.

## Adding A Table

Adding a new personal finance table requires the source tab and repository
contract to be added together:

1. Add a Google Sheets tab named with the stable table name, for example
   `budgets`.
2. Add `scripts/schemas/personal_finance/budgets.json`.
3. Set the schema `"name"` to `personal_finance__budgets`.
4. Mark source columns with `"source_field": true`.
5. Include required raw metadata fields: `_extracted_at`, `_inserted_at`, and
   `_is_deleted`.
6. Add the table name to `TABLES` in `scripts/src/personal_finance.py`.
7. Run extract and load for only that table in dev:

```bash
export RUN_ID="dev-budgets-$(date -u +%Y%m%dT%H%M%SZ)"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step extract \
  --entity budgets \
  --run-id "$RUN_ID"

uv run python src/personal_finance.py \
  --env-file "$DATA_PLATFORM_ENV_FILE" \
  --step load \
  --entity budgets \
  --run-id "$RUN_ID"
```

Do not add a table only in code or only in the workbook. The workbook tab,
schema file, raw table contract, and dev validation must move together.
