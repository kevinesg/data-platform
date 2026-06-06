# personal_finance

The `personal_finance` pipeline treats a Google Sheet as an external source
system. Its current source contract contains four tabs:

| Sheet tab | Raw table | Purpose |
| --- | --- | --- |
| `transactions` | `personal_finance__transactions` | Income and expense rows that affect cashflow. |
| `paid_for_others` | `personal_finance__paid_for_others` | Payments made for other people. |
| `transfers` | `personal_finance__transfers` | Internal account movements plus direct inflows and outflows. |
| `accounts` | `personal_finance__accounts` | Account attributes used for balance reporting. |

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
   corresponding `scripts/schemas/personal_finance__*.json` file.

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
uv sync

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
through `schemas/personal_finance__*.json`, and writes run-scoped JSONL files
under:

```text
gs://<developer-bucket>/personal_finance/accounts/<run-id>/extract/
```

Verify object metadata without printing source records:

```bash
gcloud storage ls --long \
  "gs://$LANDING_BUCKET/personal_finance/accounts/$RUN_ID/extract/"
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

The load step reads the staged files, loads them through a run-scoped BigQuery
staging table, and upserts raw rows by source `id`. Reusing a run ID retries the
same staged input.

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
implementing source deletion detection. Deletion testing needs a documented
source-change procedure and rollback because it intentionally changes source
state.
