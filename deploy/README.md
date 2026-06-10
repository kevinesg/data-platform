# Environment Setup

This runbook covers shared platform bootstrap and workstation prerequisites.
Start with the root `README.md`, then use this file for common setup before
following the relevant component README.

This directory is named `deploy` because it owns deployment-facing contracts:
environment bootstrap, host layout, runtime image manifests, and recovery
runbooks. Keep component development commands in component READMEs. If the
platform later adds infrastructure-as-code or broader operations tooling, split
that into a separate `infra/` or `ops/` directory instead of turning `deploy/`
into a catch-all folder.

## Choose The Applicable Path

The setup responsibilities are intentionally separate:

- Developer setup for an existing dev environment consists of workstation
  tools, assigned component workspace values, and the relevant component README.
  It does not include project creation, billing changes, shared-service
  enablement, or shared identity and access management (IAM) changes.
- A platform maintainer uses the bootstrap sections only when creating an
  environment, recovering it, or repairing a verified configuration gap.
- A platform maintainer provisions component workspaces from the component
  README that owns that runtime.

Every shared-resource section starts with a read-only check. Apply the mutating
command only when the check confirms that the resource or setting is missing.

The normal command flow is:

1. Install and verify workstation tools from **Workstation Tools**.
2. Run **Platform Bootstrap** only for a new shared environment, environment
   recovery, or a verified shared-resource gap.
3. Move to the relevant component README for component workspace provisioning,
   local credentials, runtime setup, and validation.

## Environment Topology

| Environment | Google Cloud project | Ownership |
| --- | --- | --- |
| `dev` | `kevinesg-dev` | Shared development and integration project. |
| `qa` | `kevinesg-qa` | Centrally managed release-validation project. |
| `prod` | `kevinesg-prod` | Centrally managed production project. |

The shared dev project contains separate component service accounts, a raw
BigQuery dataset, a dbt target dataset, and a Cloud Storage landing bucket for
each developer. Additional team or temporary sandbox projects are justified
only when project-level IAM, enabled services, quotas, costs, or infrastructure
changes need a stronger boundary.

QA and prod are provisioned once and do not use developer-specific resources.

## Workstation Tools

Install tools from their official documentation:

- [Git](https://git-scm.com/downloads)
- [GitHub CLI](https://cli.github.com/)
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [Docker Engine](https://docs.docker.com/engine/install/) or
  [Docker Desktop](https://docs.docker.com/desktop/)

The Google Cloud CLI installation must include `gcloud` and `bq`.

```bash
git --version
gh --version
gcloud --version
bq version
uv --version
docker version
docker compose version
```

Authenticate GitHub CLI once per workstation:

```bash
gh auth login
gh auth status
```

The [`gh auth login` reference](https://cli.github.com/manual/gh_auth_login)
documents browser, SSH, HTTPS, token, and headless options.

## Bootstrap Environment Values

Set these values before running shared bootstrap command blocks in this runbook.
The component READMEs repeat their own required values so each component setup
can be followed independently.

For shared dev:

```bash
export ENVIRONMENT=dev
export PROJECT_ID=kevinesg-dev
export PROJECT_NAME="Data Platform Dev"
export BIGQUERY_LOCATION=US
```

For QA:

```bash
export ENVIRONMENT=qa
export PROJECT_ID=kevinesg-qa
export PROJECT_NAME="Data Platform QA"
export BIGQUERY_LOCATION=US
```

## Platform Bootstrap

This section is for an authorized platform maintainer. Team members joining an
existing dev environment skip to the relevant component README after receiving
their assigned workspace values.

### Bootstrap CLI Configuration

A gcloud configuration is a local group of CLI properties. Its name does not
grant Google Cloud permissions. `data-platform-bootstrap-$ENVIRONMENT`
describes its purpose; the authenticated account still needs the required IAM
roles.

```bash
export PLATFORM_BOOTSTRAP_CONFIGURATION="data-platform-bootstrap-$ENVIRONMENT"

test -n "$ENVIRONMENT"
test -n "$PROJECT_ID"
test -n "$PROJECT_NAME"
test -n "$BIGQUERY_LOCATION"

if gcloud config configurations describe \
  "$PLATFORM_BOOTSTRAP_CONFIGURATION" >/dev/null 2>&1; then
  gcloud config configurations activate "$PLATFORM_BOOTSTRAP_CONFIGURATION"
else
  gcloud config configurations create "$PLATFORM_BOOTSTRAP_CONFIGURATION"
fi

gcloud auth login

export GOOGLE_CLOUD_BOOTSTRAP_ACCOUNT="$(
  gcloud auth list --filter=status:ACTIVE --format='value(account)'
)"

gcloud config set account "$GOOGLE_CLOUD_BOOTSTRAP_ACCOUNT"
gcloud config set project "$PROJECT_ID"
gcloud config list
```

Before each mutating bootstrap command, confirm that the active account and
project are correct.

### Project State

Check the project:

```bash
gcloud projects describe "$PROJECT_ID" \
  --format='value(projectId,lifecycleState)'
```

An `ACTIVE` result means the project already exists; do not create it again.
Only when the command returns `NOT_FOUND` does a platform maintainer run:

```bash
gcloud projects create "$PROJECT_ID" \
  --name="$PROJECT_NAME" \
  --labels="environment=$ENVIRONMENT,system=data-platform"
```

Any error other than `NOT_FOUND` must be investigated instead of treated as a
missing project. Project creation requires the appropriate organization-level
permission. See Google's [project creation
documentation](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

### Billing State

Check whether billing is already linked:

```bash
gcloud billing projects describe "$PROJECT_ID" \
  --format='value(projectId,billingEnabled,billingAccountName)'
```

When `billingEnabled` is `True`, do not relink billing. When it is `False`, an
authorized platform maintainer selects an open billing account and links it:

```bash
gcloud billing accounts list --filter='open=true'

read -r -p "Billing account ID to link: " BILLING_ACCOUNT_ID
test -n "$BILLING_ACCOUNT_ID"

gcloud billing projects link "$PROJECT_ID" \
  --billing-account="$BILLING_ACCOUNT_ID"

gcloud billing projects describe "$PROJECT_ID" \
  --format='value(projectId,billingEnabled,billingAccountName)'
```

If no billing account is available, use the
[Cloud Billing account
workflow](https://cloud.google.com/billing/docs/how-to/create-billing-account).
Payment-profile setup is completed in the Google Cloud console.

### Shared Service State

The platform maintainer enables shared services once per project. Developer
workstation setup only verifies the existing state.

The following guard enables only missing services:

```bash
enable_missing_platform_services() {
  local required_platform_services=(
    bigquery.googleapis.com
    storage.googleapis.com
    iam.googleapis.com
    serviceusage.googleapis.com
  )
  local enabled_platform_services
  local missing_platform_services=()

  enabled_platform_services="$(
    gcloud services list \
      --enabled \
      --project="$PROJECT_ID" \
      --format='value(config.name)'
  )" || return 1

  for required_service in "${required_platform_services[@]}"; do
    if ! printf '%s\n' "$enabled_platform_services" |
      grep -Fxq "$required_service"; then
      missing_platform_services+=("$required_service")
    fi
  done

  if ((${#missing_platform_services[@]})); then
    gcloud services enable \
      "${missing_platform_services[@]}" \
      --project="$PROJECT_ID"
  else
    echo "All required shared services are enabled."
  fi
}

enable_missing_platform_services
```

Verify the exact services without the deprecated substring-filter behavior:

```bash
gcloud services list \
  --enabled \
  --project="$PROJECT_ID" \
  --filter='config.name~"^(bigquery|iam|serviceusage|storage)\.googleapis\.com$"' \
  --format='value(config.name)' \
  --sort-by='config.name'
```

`gcloud services enable` is state-idempotent: an enabled service stays enabled.
However, each invocation can create a new long-running operation, so different
operation IDs do not mean the service was enabled twice. Use the read-only list
check instead of repeatedly calling `enable`. Additional dependency services,
such as `bigquerystorage.googleapis.com`, can appear in the enabled-service list.

## Component Setup

After shared platform bootstrap, component-specific setup lives with the
component that owns the runtime. Each component README is written as an
end-to-end path for that component so operators do not have to combine commands
from unrelated runtime sections.

- `scripts/README.md` owns the scripts service account, landing bucket, raw
  dataset, external service-account key, environment file setup, and scripts
  verification.
- `dbt/README.md` owns the dbt service account, dbt target dataset, raw read
  grant, external service-account key, external dbt profile, and `dbt debug`.
- `airflow/README.md` owns the local orchestration runtime, image variables,
  Compose setup, and DAG validation.
- `metabase/README.md` owns analytics service setup when that component is
  implemented.

Shared project creation, billing, shared API enablement, and workstation tool
installation remain in this runbook because they are cross-component platform
setup.

## QA And Prod

QA and prod project bootstrap is performed once by platform maintainers.
Environment configuration belongs on the matching deployment platform or an
authorized administration host, not on a development workstation.

Each environment uses dedicated service-account JSON files stored outside the
repository on its deployment platform or authorized administration host. Mount
keys read-only into runtime containers, restrict filesystem access, and rotate
or revoke keys immediately after suspected exposure. QA and prod keys must not
be stored on development workstations.

## Deployment Image Manifest

Deployed environments use an external non-secret `images.env` file beside the
external secret `.env` file. The image manifest pins each runtime component to
an immutable GHCR tag produced by `.github/workflows/publish-images.yml`.

```text
$HOME/secrets/data-platform/qa/images.env
$HOME/secrets/data-platform/prod/images.env
```

Create the first copy from `deploy/images.env.example` after the publishing
workflow has produced fresh image tags for the rebuilt repository. Replace every
`sha-change-me` value with a real `sha-<commit-sha>` tag.

`images.env` is disposable deployment state, not a secret. Recreate it from the
published image set during clean rebuilds. Do not commit environment-specific
copies.

## QA Deployment Path

QA deployment uses:

```text
QA repo clone:     $HOME/qa/data-platform
QA secrets file:   $HOME/secrets/data-platform/qa/.env
QA image manifest: $HOME/secrets/data-platform/qa/images.env
QA runner dir:     $HOME/actions-runners/data-platform/qa
QA runner label:   data-platform-qa
```

The `deploy-qa` workflow defaults to those paths. Add GitHub environment
variables only when the host uses different absolute paths:

```text
QA_REPO_DIR
QA_DATA_PLATFORM_ENV_FILE
QA_IMAGE_ENV_FILE
```

### QA GCP Workspace

Run this section once as a platform maintainer after the QA GCP project exists
and billing is enabled. If the project does not exist yet, use **Platform
Bootstrap** first with `PROJECT_ID=kevinesg-qa` and `PROJECT_NAME="Data Platform
QA"`. Use an authenticated machine with permission to enable APIs, create
service accounts, create buckets/datasets, grant IAM, and create service-account
keys.

```bash
export ENVIRONMENT=qa
export PROJECT_ID=kevinesg-qa
export BIGQUERY_LOCATION=US
export RAW_DATASET=raw
export DBT_DEFAULT_DATASET=analytics
export DBT_DATASETS="analytics staging intermediate seed_personal_finance mart_personal_finance"
export LANDING_BUCKET="$PROJECT_ID-data-platform-landing"
export SCRIPTS_SERVICE_ACCOUNT_NAME="data-platform-scripts-$ENVIRONMENT"
export DBT_SERVICE_ACCOUNT_NAME="data-platform-dbt-$ENVIRONMENT"
export SCRIPTS_SERVICE_ACCOUNT_EMAIL="$SCRIPTS_SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
export DBT_SERVICE_ACCOUNT_EMAIL="$DBT_SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
export SECRETS_DIR="$HOME/secrets/data-platform/$ENVIRONMENT"
export SCRIPTS_GOOGLE_APPLICATION_CREDENTIALS="$SECRETS_DIR/scripts-service-account.json"
export DBT_GOOGLE_APPLICATION_CREDENTIALS="$SECRETS_DIR/dbt-service-account.json"

gcloud config set project "$PROJECT_ID"
gcloud config list
```

Enable missing services:

```bash
enable_missing_qa_services() {
  local required_services=(
    bigquery.googleapis.com
    drive.googleapis.com
    iam.googleapis.com
    serviceusage.googleapis.com
    sheets.googleapis.com
    storage.googleapis.com
  )
  local enabled_services
  local missing_services=()

  enabled_services="$(
    gcloud services list \
      --enabled \
      --project="$PROJECT_ID" \
      --format='value(config.name)'
  )" || return 1

  for required_service in "${required_services[@]}"; do
    if ! printf '%s\n' "$enabled_services" |
      grep -Fxq "$required_service"; then
      missing_services+=("$required_service")
    fi
  done

  if ((${#missing_services[@]})); then
    gcloud services enable \
      "${missing_services[@]}" \
      --project="$PROJECT_ID"
  else
    echo "All required QA services are enabled."
  fi
}

enable_missing_qa_services
```

Create or verify service accounts:

```bash
if gcloud iam service-accounts describe \
  "$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Service account already exists: $SCRIPTS_SERVICE_ACCOUNT_EMAIL"
else
  gcloud iam service-accounts create "$SCRIPTS_SERVICE_ACCOUNT_NAME" \
    --project="$PROJECT_ID" \
    --display-name="Data Platform Scripts QA"
fi

if gcloud iam service-accounts describe \
  "$DBT_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Service account already exists: $DBT_SERVICE_ACCOUNT_EMAIL"
else
  gcloud iam service-accounts create "$DBT_SERVICE_ACCOUNT_NAME" \
    --project="$PROJECT_ID" \
    --display-name="Data Platform dbt QA"
fi

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL" \
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
```

Create or verify BigQuery datasets and grants:

```bash
if bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET" >/dev/null 2>&1; then
  echo "Raw dataset already exists: $PROJECT_ID:$RAW_DATASET"
else
  bq --location="$BIGQUERY_LOCATION" mk \
    --dataset \
    "$PROJECT_ID:$RAW_DATASET"
fi

for DBT_DATASET_NAME in $DBT_DATASETS; do
  if bq show \
    --project_id="$PROJECT_ID" \
    "$PROJECT_ID:$DBT_DATASET_NAME" >/dev/null 2>&1; then
    echo "dbt dataset already exists: $PROJECT_ID:$DBT_DATASET_NAME"
  else
    bq --location="$BIGQUERY_LOCATION" mk \
      --dataset \
      "$PROJECT_ID:$DBT_DATASET_NAME"
  fi
done

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataEditor\`
   ON SCHEMA \`$PROJECT_ID\`.$RAW_DATASET
   TO \"serviceAccount:$SCRIPTS_SERVICE_ACCOUNT_EMAIL\""

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataViewer\`
   ON SCHEMA \`$PROJECT_ID\`.$RAW_DATASET
   TO \"serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL\""

for DBT_DATASET_NAME in $DBT_DATASETS; do
  bq query \
    --project_id="$PROJECT_ID" \
    --location="$BIGQUERY_LOCATION" \
    --use_legacy_sql=false \
    "GRANT \`roles/bigquery.dataEditor\`
     ON SCHEMA \`$PROJECT_ID\`.$DBT_DATASET_NAME
     TO \"serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL\""
done
```

Create service-account keys only when the external files do not already exist:

```bash
mkdir -p "$SECRETS_DIR"
chmod 700 "$SECRETS_DIR"

if test -f "$SCRIPTS_GOOGLE_APPLICATION_CREDENTIALS"; then
  echo "Scripts service-account key already exists."
else
  gcloud iam service-accounts keys create \
    "$SCRIPTS_GOOGLE_APPLICATION_CREDENTIALS" \
    --iam-account="$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
    --project="$PROJECT_ID"
fi

if test -f "$DBT_GOOGLE_APPLICATION_CREDENTIALS"; then
  echo "dbt service-account key already exists."
else
  gcloud iam service-accounts keys create \
    "$DBT_GOOGLE_APPLICATION_CREDENTIALS" \
    --iam-account="$DBT_SERVICE_ACCOUNT_EMAIL" \
    --project="$PROJECT_ID"
fi

chmod 600 "$SCRIPTS_GOOGLE_APPLICATION_CREDENTIALS" "$DBT_GOOGLE_APPLICATION_CREDENTIALS"
```

Share the personal finance Google Sheet with the QA scripts service account:

```text
data-platform-scripts-qa@kevinesg-qa.iam.gserviceaccount.com
```

### QA Host Setup

Run this on the deployment host:

```bash
mkdir -p "$HOME/qa"
mkdir -p "$HOME/secrets/data-platform/qa"
mkdir -p "$HOME/actions-runners/data-platform/qa"
chmod 700 "$HOME/secrets/data-platform/qa"

git clone git@github.com:kevinesg/data-platform.git "$HOME/qa/data-platform"
cd "$HOME/qa/data-platform"
git switch main

cp deploy/env.example "$HOME/secrets/data-platform/qa/.env"
cp deploy/images.env.example "$HOME/secrets/data-platform/qa/images.env"
chmod 600 "$HOME/secrets/data-platform/qa/.env" "$HOME/secrets/data-platform/qa/images.env"
```

Edit `$HOME/secrets/data-platform/qa/.env` and set QA values. At minimum,
replace passwords/secrets, `PROJECT_ID`, `PERSONAL_FINANCE_GSHEET_URL`,
`PERSONAL_FINANCE_GCS_BUCKET`, `AIRFLOW_UID`, `DOCKER_GID`, and the absolute
service-account key paths. Keep `PERSONAL_FINANCE_GCS_PREFIX=personal_finance`.

Useful host values:

```bash
id -u
stat -c '%g' /var/run/docker.sock
python -c "import secrets; print(secrets.token_urlsafe(48))"
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
realpath "$HOME/secrets/data-platform/qa/scripts-service-account.json"
realpath "$HOME/secrets/data-platform/qa/dbt-service-account.json"
```

Register the QA self-hosted runner from GitHub: repository **Settings** >
**Actions** > **Runners** > **New self-hosted runner**. Use the Linux commands
shown by GitHub from this directory:

```bash
cd "$HOME/actions-runners/data-platform/qa"
```

During runner configuration:

```text
runner group: Default
runner name: a stable repo/environment/host name, for example data-platform-qa-homeserver
additional labels: data-platform-qa
work folder: _work
```

The runner name identifies the installed runner instance in the GitHub UI and
logs. Use a name that stays clear when the same host later runs runners for
other repositories or environments. The `data-platform-qa` label is what
`deploy-qa` uses for job routing.

Install and start the runner service:

```bash
cd "$HOME/actions-runners/data-platform/qa"
sudo ./svc.sh install
sudo ./svc.sh start
sudo ./svc.sh status
```

### QA Deploy

Run GitHub Actions > `deploy-qa` with `git_ref` set to `main`.

The workflow:

1. Updates the persistent QA checkout.
2. Selects the latest published scripts, dbt, and Airflow images that match the
   deployed source history.
3. Writes those refs to `$HOME/secrets/data-platform/qa/images.env`.
4. Runs `dbt compile` in the deployed dbt image with QA credentials.
5. Pulls runtime images and recreates the QA Airflow stack.
6. Runs Airflow DAG import smoke checks.

The `dbt compile` step uses the profile baked into the selected dbt image. That
profile is copied from `dbt/data_warehouse/profiles.yml.example` at image build
time, so the image must include a target matching `DBT_TARGET=qa`.

### QA Verification

Run on the deployment host after `deploy-qa` succeeds:

```bash
export QA_REPO_DIR="$HOME/qa/data-platform"
export QA_ENV_FILE="$HOME/secrets/data-platform/qa/.env"
export QA_IMAGE_ENV_FILE="$HOME/secrets/data-platform/qa/images.env"

cd "$QA_REPO_DIR/airflow"
set -a
. "$QA_IMAGE_ENV_FILE"
set +a

DATA_PLATFORM_ENV_FILE="$QA_ENV_FILE" \
  docker compose --env-file "$QA_ENV_FILE" -f docker-compose.yml ps

DATA_PLATFORM_ENV_FILE="$QA_ENV_FILE" \
  docker compose --env-file "$QA_ENV_FILE" -f docker-compose.yml exec -T scheduler airflow dags list

DATA_PLATFORM_ENV_FILE="$QA_ENV_FILE" \
  docker compose --env-file "$QA_ENV_FILE" -f docker-compose.yml exec -T scheduler airflow dags list-import-errors
```

Trigger the personal finance DAG manually from the Airflow UI, or from the
scheduler container when an end-to-end QA run is needed:

```bash
DATA_PLATFORM_ENV_FILE="$QA_ENV_FILE" \
  docker compose --env-file "$QA_ENV_FILE" -f docker-compose.yml exec -T scheduler airflow dags trigger etl__personal_finance
```

After editing `$HOME/secrets/data-platform/qa/.env`, rerun `deploy-qa` or
recreate the stack without deleting volumes:

```bash
cd "$QA_REPO_DIR/airflow"
set -a
. "$QA_IMAGE_ENV_FILE"
set +a

DATA_PLATFORM_ENV_FILE="$QA_ENV_FILE" \
  docker compose --env-file "$QA_ENV_FILE" -f docker-compose.yml up -d --force-recreate --remove-orphans
```

Do not run `docker compose down -v` for ordinary QA configuration changes. That
removes Airflow metadata and Postgres state.
