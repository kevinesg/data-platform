# scripts

The `scripts` component owns extract/load commands and source-specific pipeline
code.

This component is terminal-testable without Airflow. Airflow can orchestrate
script commands later, but extraction and loading logic belongs here.

Source-specific contracts and commands live near the relevant pipeline
implementation instead of accumulating in this component README.

## Command Flow

Use this README after reading the root `README.md` and completing the applicable
shared setup in `deploy/README.md`. For an existing dev environment, that
usually means workstation tools are installed and the platform maintainer has
provided the scripts workspace values.

Follow this file in order for scripts component setup:

1. **Scripts Cloud Workspace** provisions or repairs the scripts service
   account, landing bucket, raw dataset, and impersonation grant.
2. **Scripts Local Workstation** configures local credentials, the external dev
   environment file, the scripts runtime, and component access checks.
3. **Pipeline Docs** continue source-specific setup and validation after the
   scripts component checks pass.

## End-To-End Dev Setup

This section sets up the scripts component from workspace provisioning through
local verification. Platform project creation, billing, shared service
enablement, and workstation tool installation are covered by
`deploy/README.md`.

The service check in this section keeps the component runbook self-contained.
In an already configured dev project, it reports that the required services are
enabled. If a required service is missing, only a platform maintainer applies
the mutating enable command.

Run commands from the repository root unless a block changes directories.

### Scripts Cloud Workspace

Run this subsection as a platform maintainer.

```bash
export PROJECT_ID=kevinesg-dev
export BIGQUERY_LOCATION=US
export DEVELOPER_ID=kevinesg
export DEVELOPER_EMAIL=kevinesg.dev@gmail.com
export RAW_DATASET="raw_${DEVELOPER_ID}"
export LANDING_BUCKET="${PROJECT_ID}-data-platform-landing-${DEVELOPER_ID}"
export SCRIPTS_SERVICE_ACCOUNT_NAME="data-platform-scripts-${DEVELOPER_ID}"
export SCRIPTS_SERVICE_ACCOUNT_EMAIL="${SCRIPTS_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export PLATFORM_BOOTSTRAP_CONFIGURATION=data-platform-bootstrap-dev

gcloud config configurations activate "$PLATFORM_BOOTSTRAP_CONFIGURATION"
gcloud config unset auth/impersonate_service_account
gcloud config set project "$PROJECT_ID"
gcloud config list
```

If a command prints `This command is using service account impersonation`, stop
and rerun the block above before continuing. `DEVELOPER_ID` is a stable
lowercase identifier containing 3-8 letters or digits.

Verify or enable the shared services needed by scripts:

```bash
enable_missing_scripts_services() {
  local required_scripts_services=(
    bigquery.googleapis.com
    storage.googleapis.com
    iam.googleapis.com
    iamcredentials.googleapis.com
    serviceusage.googleapis.com
  )
  local enabled_scripts_services
  local missing_scripts_services=()

  enabled_scripts_services="$(
    gcloud services list \
      --enabled \
      --project="$PROJECT_ID" \
      --format='value(config.name)'
  )" || return 1

  for required_service in "${required_scripts_services[@]}"; do
    if ! printf '%s\n' "$enabled_scripts_services" |
      grep -Fxq "$required_service"; then
      missing_scripts_services+=("$required_service")
    fi
  done

  if ((${#missing_scripts_services[@]})); then
    gcloud services enable \
      "${missing_scripts_services[@]}" \
      --project="$PROJECT_ID"
  else
    echo "All required scripts services are enabled."
  fi
}

enable_missing_scripts_services
```

Create or verify the scripts service account:

```bash
if gcloud iam service-accounts describe \
  "$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Service account already exists: $SCRIPTS_SERVICE_ACCOUNT_EMAIL"
else
  gcloud iam service-accounts create "$SCRIPTS_SERVICE_ACCOUNT_NAME" \
    --project="$PROJECT_ID" \
    --display-name="Data Platform Scripts Dev ${DEVELOPER_ID}"
fi

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/bigquery.jobUser"
```

Create or verify the landing bucket:

```bash
if gcloud storage buckets describe \
  "gs://$LANDING_BUCKET" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Landing bucket already exists: gs://$LANDING_BUCKET"
else
  gcloud storage buckets create "gs://$LANDING_BUCKET" \
    --project="$PROJECT_ID" \
    --location="$BIGQUERY_LOCATION" \
    --uniform-bucket-level-access \
    --public-access-prevention
fi

gcloud storage buckets add-iam-policy-binding "gs://$LANDING_BUCKET" \
  --member="serviceAccount:$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/storage.objectAdmin"

gcloud storage buckets describe "gs://$LANDING_BUCKET" \
  --project="$PROJECT_ID" \
  --format='yaml(name,location,uniform_bucket_level_access,public_access_prevention)'
```

A separate bucket per developer is intentional because Cloud Storage object
listing cannot be restricted to one object-name prefix.

Create or verify the raw dataset:

```bash
if bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"; then
  echo "Raw dataset already exists: $PROJECT_ID:$RAW_DATASET"
else
  echo "Create the raw dataset only when the bq show output says Not found."
  read -r -p "Create raw dataset $PROJECT_ID:$RAW_DATASET? [y/N] " CREATE_RAW_DATASET
  if test "$CREATE_RAW_DATASET" = y; then
    bq --location="$BIGQUERY_LOCATION" mk \
      --dataset \
      "$PROJECT_ID:$RAW_DATASET"
  fi
fi

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataEditor\`
   ON SCHEMA \`$PROJECT_ID\`.$RAW_DATASET
   TO \"serviceAccount:$SCRIPTS_SERVICE_ACCOUNT_EMAIL\""

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"
```

Grant local impersonation permission:

```bash
gcloud iam service-accounts add-iam-policy-binding \
  "$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" \
  --member="user:$DEVELOPER_EMAIL" \
  --role="roles/iam.serviceAccountTokenCreator"
```

Do not create JSON service-account keys for local development.

### Scripts Local Workstation

Run this subsection on the development workstation.

```bash
export PROJECT_ID=kevinesg-dev
export DEVELOPER_ID=kevinesg
export DEVELOPER_EMAIL=kevinesg.dev@gmail.com
export RAW_DATASET="raw_${DEVELOPER_ID}"
export LANDING_BUCKET="${PROJECT_ID}-data-platform-landing-${DEVELOPER_ID}"
export SCRIPTS_SERVICE_ACCOUNT_NAME="data-platform-scripts-${DEVELOPER_ID}"
export SCRIPTS_SERVICE_ACCOUNT_EMAIL="${SCRIPTS_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export DEVELOPER_CONFIGURATION="data-platform-dev-${DEVELOPER_ID}"

if gcloud config configurations describe \
  "$DEVELOPER_CONFIGURATION" >/dev/null 2>&1; then
  gcloud config configurations activate "$DEVELOPER_CONFIGURATION"
else
  gcloud config configurations create "$DEVELOPER_CONFIGURATION"
fi

gcloud auth login "$DEVELOPER_EMAIL"
gcloud config set account "$DEVELOPER_EMAIL"
gcloud config set project "$PROJECT_ID"
gcloud config unset auth/impersonate_service_account

gcloud auth application-default login \
  --impersonate-service-account="$SCRIPTS_SERVICE_ACCOUNT_EMAIL"

gcloud config set \
  auth/impersonate_service_account \
  "$SCRIPTS_SERVICE_ACCOUNT_EMAIL"
```

Do not follow impersonated ADC setup with
`gcloud auth application-default set-quota-project`. That command updates user
ADC files and rejects an impersonated-service-account ADC file.

Create or verify the external dev environment file:

```bash
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$DATA_PLATFORM_SECRETS_DIR/.env}"
export DATA_PLATFORM_ENV_PARENT="$(dirname "$DATA_PLATFORM_ENV_FILE")"

mkdir -p "$DATA_PLATFORM_ENV_PARENT"
chmod 700 "$DATA_PLATFORM_ENV_PARENT"

if test -f "$DATA_PLATFORM_ENV_FILE"; then
  echo "Environment file already exists: $DATA_PLATFORM_ENV_FILE"
else
  cp scripts/.env.example "$DATA_PLATFORM_ENV_FILE"
  chmod 600 "$DATA_PLATFORM_ENV_FILE"
fi
```

Add or verify the scripts values in the external dev environment file. If
another component already created the file, keep the existing values and merge
the scripts values from `scripts/.env.example`.

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

Replace every example value. The file must not be copied into the repository,
committed, pasted into tickets, or shared between developers.

Install the local scripts runtime:

```bash
cd scripts
uv sync --locked
```

Verify scripts access:

```bash
gcloud config list
gcloud storage ls "gs://$LANDING_BUCKET"
bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"
gcloud auth application-default print-access-token >/dev/null &&
  echo "Application Default Credentials are available."

uv run python validate_config.py --env-file "$DATA_PLATFORM_ENV_FILE"
```

The pipeline documentation owns source access and extract/load validation.

## Runtime Contract

Scripts expose explicit terminal commands for each major extract/load step. Each
command is runnable locally first, then callable by Airflow later through the
same stable interface.

Keep task boundaries retryable:

- extract from a source into a durable staging location.
- load staged data into warehouse raw tables.
- clean up completed staging files according to a documented retention policy.

For production-scale sources, do not assume source data, staged files, or
warehouse tables fit in local memory. Prefer chunked reads, durable staged files,
warehouse-side comparisons, and idempotent run identifiers.

Runtime configuration comes from environment variables or an environment file
stored outside the repository. Do not commit `.env` files, credentials, source
exports, or warehouse data.

## Local Commands

Run scripts component commands from the `scripts/` directory after the
end-to-end dev setup has passed:

```bash
cd scripts

export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$DATA_PLATFORM_SECRETS_DIR/.env}"
```

Use Python 3.12 for this component. Keep dependencies component-local so scripts,
dbt, and Airflow can evolve independently.

The external environment file path shown above is the repository default, not a
code requirement. Another external path can be selected before running commands
by setting `DATA_PLATFORM_SECRETS_DIR` or `DATA_PLATFORM_ENV_FILE`. Pipeline
documentation defines the source-specific values placed in that file. Deployed
runtimes receive configuration and workload credentials from the deployment
platform.

## Validation

Run formatting, linting, and tests from `scripts/` as those checks are added:

```bash
uv run python validate_config.py --env-file "$DATA_PLATFORM_ENV_FILE"
uv run ruff check .
uv run pytest
```

This component starts with project scaffolding only. Source-specific runtime
dependencies, commands, schemas, and tests are added with the pipeline features
that need them.

## Schemas

Source schema files live under `schemas/`. Each schema defines the source fields
accepted by extraction, the raw warehouse table contract, metadata fields, and
the source primary key.

Schema files stay close to the scripts code because extract/load commands use
them to filter, coerce, and load source records. Business-specific
transformation rules belong in dbt, not in raw schemas.

## Design Notes

Keep extract/load logic outside Airflow. Airflow owns orchestration, scheduling,
retries, and task dependencies; this component owns source interaction, staging,
warehouse load behavior, and source-specific validation.

Add dependencies only when a pipeline needs them. Helper functions are useful
when they represent a meaningful workflow step or remove repeated complexity.
Small one-line wrappers around library calls are usually unnecessary.

## Pipeline Docs

Pipeline-specific contracts and commands live near the relevant pipeline
implementation. A single pipeline note can live at `pipelines/<source>.md`; use
`pipelines/<source>/` only when that pipeline needs multiple docs or supporting
non-runtime files. The importable runtime source tree is not the home for
operational notes.

Current pipeline setup and validation continue in `pipelines/personal_finance.md`
after the scripts end-to-end dev setup has passed.
