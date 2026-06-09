# dbt

The `dbt` component owns warehouse transformations, tests, and dbt project
configuration.

Transformation logic belongs here instead of Airflow or extract/load scripts.
Targets, schemas, and model organization are designed for production-scale
growth across many domains and thousands of models.

Pipeline-specific model behavior lives near the relevant dbt models, tests, or
domain docs instead of accumulating in this component README.

## Command Flow

Use this README after reading the root `README.md` and completing the applicable
shared setup in `deploy/README.md`. For an existing dev environment, that
usually means workstation tools are installed and the platform maintainer has
provided the dbt workspace values.

Follow this file in order for dbt component setup:

1. **Local Runtime Setup** verifies the dbt CLI can run from this component.
2. **First-Time Repository Initialization** is used only while creating the dbt
   project skeleton in an empty repo. Existing checkouts use
   **Existing-Checkout Setup** instead.
3. **dbt Cloud Workspace** provisions or repairs the dbt service account, raw
   read grant, and dbt target datasets.
4. **dbt Local Workstation** creates the external service-account key and
   environment file, configures the external dbt profile, and runs `dbt debug`.

## Project Layout

The first dbt setup step is the local runtime only. Install and verify the dbt
CLI before creating a dbt project directory.

The dbt project files are created with `dbt init`. Do not hand-create the
generated project skeleton before the CLI works locally.

Profiles, sources, models, seeds, tests, and dbt-specific cloud resources are
added after project initialization, in the commits that need them.

There are two setup paths:

- First-time repository initialization creates `dbt/data_warehouse/` with
  `dbt init`, then removes dbt's starter tutorial files before committing.
- Existing-checkout setup starts from the committed `dbt/data_warehouse/`
  project and installs the locked local runtime.

## Local Runtime Setup

Check for `uv` before working in this component. Install it only when the
command is missing on the workstation.

```bash
if command -v uv >/dev/null; then
  uv --version
else
  curl -LsSf https://astral.sh/uv/install.sh | sh
  uv --version
fi
```

Install from the committed lockfile:

```bash
cd dbt
uv sync --locked
```

Run `uv lock` only in a dependency-change commit where `pyproject.toml` is
intentionally updated.

Run the first local verification from the component directory:

```bash
cd dbt

uv run dbt --version
```

## First-Time Repository Initialization

This path applies only while the repository is being initialized and
`dbt/data_warehouse/` does not exist yet.

Initialize the project:

```bash
cd dbt

uv run dbt init
```

Use these prompt responses:

```text
Enter a name for your project (letters, digits, underscore): data_warehouse
The profile data_warehouse already exists in ~/.dbt/profiles.yml. Continue and overwrite it? [y/N]: n
```

Answer `n` when dbt asks to overwrite an existing global profile. This project
does not use `~/.dbt/profiles.yml` as the committed or preferred local profile
location.

Clean up the generated starter project before committing:

- Delete `data_warehouse/models/example/` and the tutorial files inside it,
  including `my_first_dbt_model.sql`, `my_second_dbt_model.sql`, and
  `schema.yml` when dbt generates them.
- Remove the generated `models.data_warehouse.example` configuration from
  `data_warehouse/dbt_project.yml`.
- Replace the generated `data_warehouse/README.md` starter text with this
  project's dbt ownership notes.
- Keep the standard dbt directories: `analyses/`, `macros/`, `models/`,
  `seeds/`, `snapshots/`, and `tests/`.
- Add `.gitkeep` files only for empty standard directories that Git must track.
- Keep `data_warehouse/.gitignore` for dbt-generated `target/`,
  `dbt_packages/`, and `logs/`.
- Leave generated local runtime files untracked, including `dbt/.venv/`,
  `dbt/logs/`, `data_warehouse/target/`, and `data_warehouse/dbt_packages/`.

The actual local `profiles.yml` belongs outside the repository under the
project secrets directory once profile setup is needed.

## Existing-Checkout Setup

When `dbt/data_warehouse/` already exists in the repository, the project has
already been initialized. Set up the local runtime from the lockfile and verify
the CLI:

```bash
cd dbt
uv sync --locked
uv run dbt --version
```

After this local runtime check passes, continue with **End-To-End Dev Setup**
below for the dbt service account, datasets, external environment file,
service-account key, profile file, and `dbt debug`.

## Profile Contract

The committed `data_warehouse/profiles.yml.example` is a non-secret template.
The working `profiles.yml` lives outside the repository with the rest of the
project's local dev configuration.

Local dbt development uses the dbt service-account JSON file configured through
`DBT_GOOGLE_APPLICATION_CREDENTIALS`.

## End-To-End Dev Setup

This section sets up the dbt component from workspace provisioning through
local `dbt debug`. Platform project creation, billing, shared service
enablement, and workstation tool installation are covered by
`deploy/README.md`.

The service check in this section keeps the component runbook self-contained.
In an already configured dev project, it reports that the required services are
enabled. If a required service is missing, only a platform maintainer applies
the mutating enable command.

Run commands from the repository root unless a block changes directories.

### dbt Cloud Workspace

Run this subsection as a platform maintainer.

Every time this subsection is resumed from the middle, rerun the first block
below before running a resource block. The resource blocks create datasets and
grant IAM, so they run from the authenticated platform-maintainer configuration.
The dbt service account intentionally cannot grant access to itself.

```bash
export PROJECT_ID=kevinesg-dev
export BIGQUERY_LOCATION=US
export DEVELOPER_ID=kevinesg
export DEVELOPER_EMAIL=kevinesg.dev@gmail.com
export RAW_DATASET="raw_${DEVELOPER_ID}"
export DBT_DATASET="dbt_${DEVELOPER_ID}"
export DBT_STAGING_DATASET="${DBT_DATASET}_staging"
export DBT_INTERMEDIATE_DATASET="${DBT_DATASET}_intermediate"
export DBT_PERSONAL_FINANCE_SEED_DATASET="${DBT_DATASET}_seed_personal_finance"
export DBT_PERSONAL_FINANCE_MART_DATASET="${DBT_DATASET}_mart_personal_finance"
export DBT_SERVICE_ACCOUNT_NAME="data-platform-dbt-${DEVELOPER_ID}"
export DBT_SERVICE_ACCOUNT_EMAIL="${DBT_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export PLATFORM_BOOTSTRAP_CONFIGURATION=data-platform-bootstrap-dev

gcloud config configurations activate "$PLATFORM_BOOTSTRAP_CONFIGURATION"
gcloud config set project "$PROJECT_ID"
gcloud config list
```

`DEVELOPER_ID` is a stable lowercase identifier containing 3-8 letters or
digits.

Verify or enable the shared services needed by dbt:

```bash
enable_missing_dbt_services() {
  local required_dbt_services=(
    bigquery.googleapis.com
    iam.googleapis.com
    serviceusage.googleapis.com
  )
  local enabled_dbt_services
  local missing_dbt_services=()

  enabled_dbt_services="$(
    gcloud services list \
      --enabled \
      --project="$PROJECT_ID" \
      --format='value(config.name)'
  )" || return 1

  for required_service in "${required_dbt_services[@]}"; do
    if ! printf '%s\n' "$enabled_dbt_services" |
      grep -Fxq "$required_service"; then
      missing_dbt_services+=("$required_service")
    fi
  done

  if ((${#missing_dbt_services[@]})); then
    gcloud services enable \
      "${missing_dbt_services[@]}" \
      --project="$PROJECT_ID"
  else
    echo "All required dbt services are enabled."
  fi
}

enable_missing_dbt_services
```

Create or verify the dbt service account:

```bash
if gcloud iam service-accounts describe \
  "$DBT_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Service account already exists: $DBT_SERVICE_ACCOUNT_EMAIL"
else
  gcloud iam service-accounts create "$DBT_SERVICE_ACCOUNT_NAME" \
    --project="$PROJECT_ID" \
    --display-name="Data Platform dbt Dev ${DEVELOPER_ID}"
fi

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/bigquery.user"
```

The dbt BigQuery adapter creates or ensures target schemas during `dbt run`.
`roles/bigquery.jobUser` can run query jobs but does not include
`bigquery.datasets.create`; `roles/bigquery.user` is the predefined project role
needed for local dbt runs. Dataset-level grants below still define which raw and
dbt datasets the service account can read or write.

Create or verify the raw dataset boundary used by dbt sources:

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
  "GRANT \`roles/bigquery.dataViewer\`
   ON SCHEMA \`$PROJECT_ID\`.$RAW_DATASET
   TO \"serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL\""

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"
```

Create or verify the dbt target dataset:

```bash
if bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_DATASET"; then
  echo "dbt target dataset already exists: $PROJECT_ID:$DBT_DATASET"
else
  echo "Create the dbt target dataset only when the bq show output says Not found."
  read -r -p "Create dbt target dataset $PROJECT_ID:$DBT_DATASET? [y/N] " CREATE_DBT_DATASET
  if test "$CREATE_DBT_DATASET" = y; then
    bq --location="$BIGQUERY_LOCATION" mk \
      --dataset \
      "$PROJECT_ID:$DBT_DATASET"
  fi
fi

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataEditor\`
   ON SCHEMA \`$PROJECT_ID\`.$DBT_DATASET
   TO \"serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL\""

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_DATASET"
```

Create or verify the dbt staging dataset:

This block is a platform-maintainer resource block. It must run from the
authenticated bootstrap configuration.

```bash
if bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_STAGING_DATASET"; then
  echo "dbt staging dataset already exists: $PROJECT_ID:$DBT_STAGING_DATASET"
else
  echo "Create the dbt staging dataset only when the bq show output says Not found."
  read -r -p "Create dbt staging dataset $PROJECT_ID:$DBT_STAGING_DATASET? [y/N] " CREATE_DBT_STAGING_DATASET
  if test "$CREATE_DBT_STAGING_DATASET" = y; then
    bq --location="$BIGQUERY_LOCATION" mk \
      --dataset \
      "$PROJECT_ID:$DBT_STAGING_DATASET"
  fi
fi

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataEditor\`
   ON SCHEMA \`$PROJECT_ID\`.$DBT_STAGING_DATASET
   TO \"serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL\""

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_STAGING_DATASET"
```

dbt creates BigQuery datasets from the target dataset plus a model layer suffix.
The staging models use `+schema: staging`, so the default BigQuery dataset name
is `dbt_<developer>_staging`.

Create or verify the additional dbt write datasets used by the current project:

This block is a platform-maintainer resource block. It must run from the
authenticated bootstrap configuration.

```bash
for DBT_WRITE_DATASET in \
  "$DBT_INTERMEDIATE_DATASET" \
  "$DBT_PERSONAL_FINANCE_SEED_DATASET" \
  "$DBT_PERSONAL_FINANCE_MART_DATASET"; do
  if bq show \
    --project_id="$PROJECT_ID" \
    "$PROJECT_ID:$DBT_WRITE_DATASET"; then
    echo "dbt write dataset already exists: $PROJECT_ID:$DBT_WRITE_DATASET"
  else
    echo "Create the dbt write dataset only when the bq show output says Not found."
    read -r -p "Create dbt write dataset $PROJECT_ID:$DBT_WRITE_DATASET? [y/N] " CREATE_DBT_WRITE_DATASET
    if test "$CREATE_DBT_WRITE_DATASET" = y; then
      bq --location="$BIGQUERY_LOCATION" mk \
        --dataset \
        "$PROJECT_ID:$DBT_WRITE_DATASET"
    fi
  fi

  bq query \
    --project_id="$PROJECT_ID" \
    --location="$BIGQUERY_LOCATION" \
    --use_legacy_sql=false \
    "GRANT \`roles/bigquery.dataEditor\`
     ON SCHEMA \`$PROJECT_ID\`.$DBT_WRITE_DATASET
     TO \"serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL\""

  bq show \
    --project_id="$PROJECT_ID" \
    "$PROJECT_ID:$DBT_WRITE_DATASET"
done
```

Intermediate models use `+schema: intermediate`, so the default BigQuery dataset
name is `dbt_<developer>_intermediate`. Personal finance classification seeds
use `+schema: seed_personal_finance`, so the default BigQuery dataset name is
`dbt_<developer>_seed_personal_finance`. Personal finance marts use
`+schema: mart_personal_finance`, so the default BigQuery dataset name is
`dbt_<developer>_mart_personal_finance`.

### dbt Local Workstation

Run this subsection on the development workstation.

```bash
export PROJECT_ID=kevinesg-dev
export BIGQUERY_LOCATION=US
export DEVELOPER_ID=kevinesg
export DEVELOPER_EMAIL=kevinesg.dev@gmail.com
export RAW_DATASET="raw_${DEVELOPER_ID}"
export DBT_DATASET="dbt_${DEVELOPER_ID}"
export DBT_STAGING_DATASET="${DBT_DATASET}_staging"
export DBT_INTERMEDIATE_DATASET="${DBT_DATASET}_intermediate"
export DBT_PERSONAL_FINANCE_SEED_DATASET="${DBT_DATASET}_seed_personal_finance"
export DBT_PERSONAL_FINANCE_MART_DATASET="${DBT_DATASET}_mart_personal_finance"
export DBT_SERVICE_ACCOUNT_NAME="data-platform-dbt-${DEVELOPER_ID}"
export DBT_SERVICE_ACCOUNT_EMAIL="${DBT_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export DEVELOPER_CONFIGURATION="data-platform-dev-${DEVELOPER_ID}"
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DBT_GOOGLE_APPLICATION_CREDENTIALS="$DATA_PLATFORM_SECRETS_DIR/dbt-service-account.json"

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
```

The unset command only clears legacy local CLI state from the previous
impersonated-ADC setup. dbt runtime authentication uses the JSON key configured
below.

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
  cp dbt/.env.example "$DATA_PLATFORM_ENV_FILE"
  chmod 600 "$DATA_PLATFORM_ENV_FILE"
fi
```

Create the dbt service-account key only when the external file does not already
exist:

```bash
if test -f "$DBT_GOOGLE_APPLICATION_CREDENTIALS"; then
  echo "dbt service-account key already exists."
else
  gcloud iam service-accounts keys create \
    "$DBT_GOOGLE_APPLICATION_CREDENTIALS" \
    --iam-account="$DBT_SERVICE_ACCOUNT_EMAIL" \
    --project="$PROJECT_ID"
  chmod 600 "$DBT_GOOGLE_APPLICATION_CREDENTIALS"
fi

test -s "$DBT_GOOGLE_APPLICATION_CREDENTIALS"
grep -Fq "$DBT_SERVICE_ACCOUNT_EMAIL" \
  "$DBT_GOOGLE_APPLICATION_CREDENTIALS"
```

The key is a long-lived credential. Keep it outside the repository and runtime
image, and delete/recreate it immediately if it is exposed.

Add or verify the dbt values in the external dev environment file. If another
component already created the file, keep the existing values and merge the dbt
values from `dbt/.env.example`.

```dotenv
PROJECT_ID=kevinesg-dev
RAW_DATASET=raw_kevinesg
DBT_TARGET=dev
DBT_DATASET=dbt_kevinesg
DBT_SERVICE_ACCOUNT_EMAIL=data-platform-dbt-kevinesg@kevinesg-dev.iam.gserviceaccount.com
DBT_GOOGLE_APPLICATION_CREDENTIALS=/home/kevinesg/dev/secrets/data-platform/dbt-service-account.json
DBT_THREADS=4
BIGQUERY_LOCATION=US
```

Install the local dbt runtime:

```bash
cd dbt
uv sync --locked
uv run dbt --version
```

Create or verify the external dbt profile:

```bash
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DATA_PLATFORM_DBT_PROFILES_DIR="${DATA_PLATFORM_DBT_PROFILES_DIR:-$DATA_PLATFORM_SECRETS_DIR/dbt}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$DATA_PLATFORM_DBT_PROFILES_DIR}"

mkdir -p "$DBT_PROFILES_DIR"
chmod 700 "$DBT_PROFILES_DIR"

cp data_warehouse/profiles.yml.example "$DBT_PROFILES_DIR/profiles.yml"
chmod 600 "$DBT_PROFILES_DIR/profiles.yml"
```

The committed profile is environment-driven, so replacing the external copy is
the supported way to apply authentication contract changes.

Load the external environment file before running dbt commands:

```bash
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a
```

Verify the profile and connection:

```bash
gcloud config list
bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"
bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_DATASET"
bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_STAGING_DATASET"
bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_INTERMEDIATE_DATASET"
bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_PERSONAL_FINANCE_SEED_DATASET"
bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$DBT_PERSONAL_FINANCE_MART_DATASET"
test -s "$DBT_GOOGLE_APPLICATION_CREDENTIALS"

uv run dbt debug --project-dir data_warehouse --profiles-dir "$DBT_PROFILES_DIR"
```

After source definitions are added, verify dbt can parse and list them:

```bash
uv run dbt ls \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --resource-type source
```

After staging models are added, verify dbt can list and run only the staging
path:

```bash
uv run dbt ls \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --resource-type model \
  --select path:models/staging/personal_finance

uv run dbt run \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select path:models/staging/personal_finance
```

After staging model tests are added, run only the staging tests:

```bash
uv run dbt test \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select path:models/staging/personal_finance
```

After personal finance seeds and downstream models are added, materialize seeds
before running models that depend on them. `dbt run` does not run seed files.

```bash
uv run dbt ls \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --resource-type seed

uv run dbt seed \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select personal_finance__transaction_type_classification
```

After personal finance intermediate models are added, run and test only that
layer:

```bash
uv run dbt run \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select path:models/intermediate/personal_finance

uv run dbt test \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select personal_finance__transaction_type_classification path:models/intermediate/personal_finance path:tests/personal_finance
```

After personal finance marts are added, seed first, then run the selected mart
path with its upstream models:

```bash
uv run dbt seed \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select personal_finance__transaction_type_classification

uv run dbt run \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select +path:models/marts/personal_finance
```
