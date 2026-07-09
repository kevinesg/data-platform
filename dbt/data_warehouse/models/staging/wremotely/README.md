# wremotely Staging Models

This directory owns dbt staging models for the raw wremotely tables loaded by
the private `wremotely-etl` runtime.

The raw tables use a shared staged-envelope contract:

```text
contract_version
stage_run_id
source_step
source_run_id
source_artifact
source_artifact_sha256
source_record_index
payload
```

Staging keeps that grain and unpacks frequently used `payload` fields into
typed columns. It does not decide whether a job is active, publishable, closed,
or safe to serve. Those decisions belong in intermediate and mart models after
the raw facts are parseable.

## Environment variables

Use the normal dbt environment from `dbt/README.md`, plus the wremotely raw
dataset:

```bash
export WREMOTELY_RAW_DATASET="wremotely_raw_dev"
```

For QA and prod, use the environment-specific raw dataset created for
`wremotely-etl`, for example `wremotely_raw_qa` or `wremotely_raw_prod`.

If `WREMOTELY_RAW_DATASET` is unset, dbt falls back to `RAW_DATASET` so CI can
parse the project without a live wremotely warehouse.

## Grant dbt read access to the raw tables

Run this as a platform maintainer after the `wremotely-etl` raw dataset exists.
It is a mutating warehouse permission change.

```bash
test -n "$PROJECT_ID"
test -n "$WREMOTELY_RAW_DATASET"
test -n "$DBT_SERVICE_ACCOUNT_EMAIL"

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataViewer\`
   ON SCHEMA \`$PROJECT_ID\`.$WREMOTELY_RAW_DATASET
   TO \"serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL\""
```

Verify the raw tables are visible without printing row payloads:

```bash
for TABLE in \
  wremotely__discovery_source_responses \
  wremotely__discovery_candidates \
  wremotely__source_crawl_pages \
  wremotely__source_crawl_job_urls \
  wremotely__job_url_selection_results \
  wremotely__selected_job_urls \
  wremotely__extraction_page_results \
  wremotely__job_facts \
  wremotely__classification_classifications \
  wremotely__country_eligibility_extractions \
  wremotely__recheck_lifecycle_results; do
  bq query \
    --project_id="$PROJECT_ID" \
    --location="$BIGQUERY_LOCATION" \
    --use_legacy_sql=false \
    "SELECT '$TABLE' AS table_name, COUNT(*) AS row_count
     FROM \`$PROJECT_ID.$WREMOTELY_RAW_DATASET.$TABLE\`"
done
```

## Validate staging models

From the `dbt/` component directory:

```bash
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$DATA_PLATFORM_SECRETS_DIR/.env}"

test -f "$DATA_PLATFORM_ENV_FILE"
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a

export DATA_PLATFORM_DBT_PROFILES_DIR="${DATA_PLATFORM_DBT_PROFILES_DIR:-$DATA_PLATFORM_SECRETS_DIR/dbt}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$DATA_PLATFORM_DBT_PROFILES_DIR}"
export WREMOTELY_RAW_DATASET="${WREMOTELY_RAW_DATASET:-wremotely_raw_dev}"
test -s "$DBT_GOOGLE_APPLICATION_CREDENTIALS"

uv run dbt parse \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR"

uv run dbt build \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select path:models/staging/wremotely
```

Use the external profile and service-account key documented in `dbt/README.md`.
Do not commit `profiles.yml`, service-account JSON, generated `target/`, or
warehouse exports.
