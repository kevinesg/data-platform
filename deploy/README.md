# Environment Setup

This runbook covers platform bootstrap, developer workspace provisioning, and
workstation setup. It supports both a new platform environment and an existing
environment that already has projects, billing, and services configured.

## Choose The Applicable Path

The setup responsibilities are intentionally separate:

- Developer setup for an existing dev environment consists of workstation
  tools, a provisioned developer workspace, and local authentication. It does
  not include project creation, billing changes, shared-service enablement, or
  shared identity and access management (IAM) changes.
- A platform maintainer uses the bootstrap sections only when creating an
  environment, recovering it, or repairing a verified configuration gap.
- A platform maintainer provisions one developer workspace per team member.

Every shared-resource section starts with a read-only check. Apply the mutating
command only when the check confirms that the resource or setting is missing.

## Environment Topology

| Environment | Google Cloud project | Ownership |
| --- | --- | --- |
| `dev` | `kevinesg-dev` | Shared development and integration project. |
| `qa` | `kevinesg-qa` | Centrally managed release-validation project. |
| `prod` | `kevinesg-prod` | Centrally managed production project. |

The shared dev project contains a separate service account, BigQuery dataset,
and Cloud Storage landing bucket for each developer. Additional team or
temporary sandbox projects are justified only when project-level IAM, enabled
services, quotas, costs, or infrastructure changes need a stronger boundary.

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

## Shared Dev Contract

The remaining commands use these values:

```bash
export ENVIRONMENT=dev
export PROJECT_ID=kevinesg-dev
export PROJECT_NAME="Data Platform Dev"
export BIGQUERY_LOCATION=US
```

## Platform Bootstrap

This section is for an authorized platform maintainer. Team members joining an
existing dev environment skip to **Configure A Development Workstation** after
receiving their assigned workspace values.

### Bootstrap CLI Configuration

A gcloud configuration is a local group of CLI properties. Its name does not
grant Google Cloud permissions. `data-platform-bootstrap-dev` describes its
purpose; the authenticated account still needs the required IAM roles.

```bash
export PLATFORM_BOOTSTRAP_CONFIGURATION=data-platform-bootstrap-dev

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
Only when the command returns `NOT_FOUND` should a platform maintainer run:

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
    iamcredentials.googleapis.com
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
  --filter='config.name~"^(bigquery|iam|iamcredentials|serviceusage|storage)\.googleapis\.com$"' \
  --format='value(config.name)' \
  --sort-by='config.name'
```

`gcloud services enable` is state-idempotent: an enabled service stays enabled.
However, each invocation can create a new long-running operation, so different
operation IDs do not mean the service was enabled twice. Use the read-only list
check instead of repeatedly calling `enable`. Additional dependency services,
such as `bigquerystorage.googleapis.com`, can appear in the enabled-service list.

## Developer Workspace Provisioning

This section is run once per developer by a platform maintainer. A team member
does not grant their own project IAM or create shared cloud resources.

Run this section from the bootstrap gcloud configuration, not from a developer
configuration that impersonates a component service account. If a command prints
`This command is using service account impersonation`, stop and fix the active
configuration before continuing.

```bash
export PLATFORM_BOOTSTRAP_CONFIGURATION=data-platform-bootstrap-dev

gcloud config configurations activate "$PLATFORM_BOOTSTRAP_CONFIGURATION"
gcloud config unset auth/impersonate_service_account
gcloud config set project "$PROJECT_ID"
gcloud config list
```

`DEVELOPER_ID` is a stable lowercase identifier containing 3-8 letters or
digits. The eight-character limit keeps the descriptive service-account ID
within Google Cloud's 30-character limit.

```bash
export DEVELOPER_ID=kevinesg
export DEVELOPER_EMAIL=kevinesg.dev@gmail.com
export SCRIPTS_SERVICE_ACCOUNT_NAME="data-platform-scripts-${DEVELOPER_ID}"
export SCRIPTS_SERVICE_ACCOUNT_EMAIL="${SCRIPTS_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export RAW_DATASET="raw_${DEVELOPER_ID}"
export LANDING_BUCKET="${PROJECT_ID}-data-platform-landing-${DEVELOPER_ID}"
```

### Service Account

Check before creating:

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
```

Grant or repair the project-level BigQuery job-runner role:

```bash
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/bigquery.jobUser"
```

The IAM policy stores a unique role/member binding; rerunning this command does
not create a second effective grant, but it should still be treated as a
maintainer repair command rather than a team-member onboarding command.

### Landing Bucket

Check before creating:

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
```

Grant or repair object access and verify the bucket contract:

```bash
gcloud storage buckets add-iam-policy-binding "gs://$LANDING_BUCKET" \
  --member="serviceAccount:$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/storage.objectAdmin"

gcloud storage buckets describe "gs://$LANDING_BUCKET" \
  --project="$PROJECT_ID" \
  --format='yaml(name,location,uniform_bucket_level_access,public_access_prevention)'
```

A separate bucket per developer is intentional because Cloud Storage object
listing cannot be restricted to one object-name prefix.

### Raw Dataset

Check before creating:

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
```

Grant or repair data-editor access and verify the dataset:

```bash
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

### Impersonation Permission

Grant or repair the developer's permission to obtain short-lived credentials
for their service account:

```bash
gcloud iam service-accounts add-iam-policy-binding \
  "$SCRIPTS_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" \
  --member="user:$DEVELOPER_EMAIL" \
  --role="roles/iam.serviceAccountTokenCreator"
```

Do not create a JSON service-account key for local development.

## Configure A Development Workstation

This section is run by the developer after the platform maintainer confirms that
the workspace exists.

Set the assigned workspace values:

```bash
export PROJECT_ID=kevinesg-dev
export DEVELOPER_ID=kevinesg
export DEVELOPER_EMAIL=kevinesg.dev@gmail.com
export SCRIPTS_SERVICE_ACCOUNT_NAME="data-platform-scripts-${DEVELOPER_ID}"
export SCRIPTS_SERVICE_ACCOUNT_EMAIL="${SCRIPTS_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export RAW_DATASET="raw_${DEVELOPER_ID}"
export LANDING_BUCKET="${PROJECT_ID}-data-platform-landing-${DEVELOPER_ID}"
```

### Developer CLI Configuration

The gcloud configuration separates project and impersonation settings from
unrelated projects. Like the bootstrap configuration, its name grants no IAM
permissions.

```bash
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
gcloud config list
```

### Application Default Credentials

Application Default Credentials are separate from gcloud CLI credentials.
Create impersonated credentials once during workstation setup, or rerun the
command when changing the target service account:

```bash
gcloud auth application-default login \
  --impersonate-service-account="$SCRIPTS_SERVICE_ACCOUNT_EMAIL"

gcloud config set \
  auth/impersonate_service_account \
  "$SCRIPTS_SERVICE_ACCOUNT_EMAIL"
```

Do not follow this with `gcloud auth application-default set-quota-project`.
That command updates user ADC files and rejects an impersonated-service-account
ADC file. For an impersonated service account, Google uses the project that owns
the service account as the quota project.

Verify that ADC can produce a token without displaying it:

```bash
gcloud auth application-default print-access-token >/dev/null &&
  echo "Application Default Credentials are available."
```

Service account impersonation starts with an authenticated human identity and
exchanges it for a short-lived access token representing the selected service
account. The human must have explicit permission to perform that exchange. No
service-account private key is downloaded.

Local development uses impersonation so application permissions match the
developer scripts identity. Approved human access to QA or prod can also use
narrowly scoped impersonation. Scheduled QA/prod workloads use attached service
accounts on Google Cloud or Workload Identity Federation outside Google Cloud;
they do not depend on a human login or local ADC file.

### External Dev Environment File

The default path on this workstation is
`$HOME/dev/secrets/data-platform/.env`. Either variable can select another
external location:

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

Edit the file and replace every example value. The scripts discover
`DATA_PLATFORM_ENV_FILE`, then `DATA_PLATFORM_SECRETS_DIR`, then the preferred
default path.

The environment file can contain source URLs and other sensitive configuration.
It must not be copied into the repository, committed, pasted into tickets, or
shared between developers.

## Developer Workspace Verification

Run these read-only checks after workstation setup:

```bash
gcloud config list
gcloud storage ls "gs://$LANDING_BUCKET"
bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"
gcloud auth application-default print-access-token >/dev/null &&
  echo "Application Default Credentials are available."
```

The pipeline documentation owns source access and extract/load validation.

## QA And Prod

QA and prod project bootstrap is performed once by platform maintainers.
Environment configuration belongs on the matching deployment platform or an
authorized administration host, not on a development workstation.

Workloads on Google Cloud use an attached environment-specific service account.
Workloads outside Google Cloud and CI/CD use Workload Identity Federation when
supported. Persistent JSON keys are a fallback only when the selected runtime
cannot use a keyless mechanism.
