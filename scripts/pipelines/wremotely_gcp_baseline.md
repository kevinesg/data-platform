# wremotely GCP Workload Baseline

This runbook captures a bounded, read-only wremotely workload baseline before
an on-premises migration design is selected. It measures current BigQuery
storage and attributed jobs, the wremotely Cloud Storage prefix, publication
traffic visible through Cloud Monitoring, and optional dbt run artifacts. It
does not change pipeline data, deploy infrastructure, estimate invoice cost, or
prove that a proposed replacement can restore production state.

The collector is intentionally not an Airflow task. It is a one-time or
operator-initiated architecture measurement, not a normal extract/load step.
Store its JSON output outside the repository and do not commit or attach it to a
public issue.

## Measurement Contract

The collector:

- accepts only a `wremotely` Cloud Storage prefix and a publication topic whose
  ID contains `wremotely`;
- attributes BigQuery tables through the exact `wremotely__`,
  `stg_wremotely__`, and `int_wremotely__` table prefixes or the domain-owned
  `*_wremotely` dbt datasets, which also contain aliased marts such as
  `serving_jobs`;
- attributes completed BigQuery jobs through those table prefixes or the
  `wremotely` query identifier and excludes its own labeled metadata queries;
- bounds the shared BigQuery metadata queries with a maximum bytes-billed
  value;
- fails without writing a partial report if the Cloud Storage prefix contains
  more than the declared object limit;
- emits fixed Cloud Storage age and storage-class buckets, not object names or
  run identifiers;
- sums one-minute Pub/Sub distributions into hourly points before transfer,
  preserving counts while bounding the Monitoring response;
- limits lookback to 42 days because Cloud Monitoring does not retain these
  Pub/Sub metrics as long as BigQuery job metadata; and
- includes dbt wall-clock duration only when explicit `run_results.json`
  artifacts are supplied.

The report gives usage units, not currency. Shared-project billing does not
provide a defensible product-only invoice allocation, and the metadata queries
used to create the report have their own minimum query billing behavior. Treat
BigQuery storage history as delayed, Cloud Storage request cost as excluded,
and an empty Pub/Sub result as ambiguous between no traffic and a monitoring
gap. BigQuery query duration is not a substitute for dbt wall-clock duration.
The measurement semantics follow Google's current documentation for
[BigQuery table storage](https://cloud.google.com/bigquery/docs/information-schema-table-storage),
[BigQuery storage history](https://cloud.google.com/bigquery/docs/information-schema-table-storage-usage),
[BigQuery jobs](https://cloud.google.com/bigquery/docs/information-schema-jobs),
and [Pub/Sub monitoring](https://cloud.google.com/pubsub/docs/monitoring).

## Prerequisites And Authentication

Run the setup commands as an authorized platform maintainer. They require the
Google Cloud CLI, `uv`, a checkout of this repository, and an operator identity
allowed to create service accounts, enable services, and administer the listed
IAM bindings. The dedicated identity avoids broadening the Airflow, dbt, or
scripts runtime service accounts.

Run the complete setup and capture in dev first. After that output passes the
validation below, repeat it for prod to collect the architecture baseline. Set
one target environment explicitly:

```bash
export ENVIRONMENT=dev

case "$ENVIRONMENT" in
  dev)
    export PROJECT_ID=kevinesg-dev
    export DATA_PLATFORM_REPO_DIR="$HOME/dev/github/data-platform"
    export DATA_PLATFORM_ENV_FILE="$HOME/dev/secrets/data-platform/.env"
    ;;
  prod)
    export PROJECT_ID=kevinesg-prod
    export DATA_PLATFORM_REPO_DIR="$HOME/prod/data-platform"
    export DATA_PLATFORM_ENV_FILE="$HOME/secrets/data-platform/prod/.env"
    ;;
  *)
    echo "ENVIRONMENT must be dev or prod" >&2
    return 1 2>/dev/null || exit 1
    ;;
esac

export OPERATOR_EMAIL="$(gcloud config get-value account)"
export BASELINE_SERVICE_ACCOUNT_NAME=wremotely-gcp-baseline
export BASELINE_SERVICE_ACCOUNT_EMAIL="${BASELINE_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

test "$PROJECT_ID" = "kevinesg-$ENVIRONMENT"
printf '%s\n' "$OPERATOR_EMAIL" | grep -Fq @
test -d "$DATA_PLATFORM_REPO_DIR/scripts"
test -r "$DATA_PLATFORM_ENV_FILE"
```

Create an isolated gcloud configuration before any project inspection or
mutation. This prevents a stale active project in the operator's normal gcloud
configuration from influencing the setup.

```bash
export BASELINE_GCLOUD_CONFIG_DIR="$HOME/.config/gcloud-wremotely-baseline-$ENVIRONMENT"
mkdir -p "$BASELINE_GCLOUD_CONFIG_DIR"
chmod 700 "$BASELINE_GCLOUD_CONFIG_DIR"
export CLOUDSDK_CONFIG="$BASELINE_GCLOUD_CONFIG_DIR"

gcloud auth login "$OPERATOR_EMAIL"
gcloud config set project "$PROJECT_ID"

test "$(gcloud config get-value project)" = "$PROJECT_ID"
test "$(gcloud config get-value account)" = "$OPERATOR_EMAIL"
gcloud config list
```

Verify the required APIs and enable only those reported missing:

```bash
required_baseline_services=(
  bigquery.googleapis.com
  iamcredentials.googleapis.com
  monitoring.googleapis.com
  storage.googleapis.com
)

enabled_baseline_services="$(
  gcloud services list \
    --enabled \
    --project="$PROJECT_ID" \
    --format='value(config.name)'
)"

missing_baseline_services=()
for required_service in "${required_baseline_services[@]}"; do
  if ! printf '%s\n' "$enabled_baseline_services" |
    grep -Fxq "$required_service"; then
    missing_baseline_services+=("$required_service")
  fi
done

if ((${#missing_baseline_services[@]})); then
  printf 'Missing services:\n%s\n' "${missing_baseline_services[*]}"
  gcloud services enable \
    "${missing_baseline_services[@]}" \
    --project="$PROJECT_ID"
else
  echo "All baseline services are enabled."
fi
```

Create the dedicated service account only when it is absent:

```bash
if gcloud iam service-accounts describe \
  "$BASELINE_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Service account already exists: $BASELINE_SERVICE_ACCOUNT_EMAIL"
else
  gcloud iam service-accounts create "$BASELINE_SERVICE_ACCOUNT_NAME" \
    --project="$PROJECT_ID" \
    --display-name="wremotely GCP baseline reader"
fi
```

Grant only the read and metadata permissions required by the collector. The
BigQuery job-user role is required because querying `INFORMATION_SCHEMA`
creates bounded metadata query jobs.

```bash
baseline_project_roles=(
  roles/bigquery.jobUser
  roles/bigquery.metadataViewer
  roles/bigquery.resourceViewer
  roles/monitoring.viewer
)

for baseline_role in "${baseline_project_roles[@]}"; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$BASELINE_SERVICE_ACCOUNT_EMAIL" \
    --role="$baseline_role"
done

gcloud iam service-accounts add-iam-policy-binding \
  "$BASELINE_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" \
  --member="user:$OPERATOR_EMAIL" \
  --role=roles/iam.serviceAccountTokenCreator
```

Load the existing external environment configuration to identify the exact
wremotely bucket. Do not copy this file into the checkout or print unrelated
secret values.

```bash
test -r "$DATA_PLATFORM_ENV_FILE"
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a

test "$PROJECT_ID" = "kevinesg-$ENVIRONMENT"
test "$WREMOTELY_GCS_PREFIX" = wremotely
test -n "$WREMOTELY_GCS_BUCKET"
test -n "$WREMOTELY_PUBLICATION_TOPIC"
test -n "$WREMOTELY_BIGQUERY_LOCATION"
```

Grant bucket-level object viewing. Cloud Storage IAM cannot restrict object
listing to a name prefix, so the code-level `wremotely/` prefix check and
listing bound remain required controls.

```bash
gcloud storage buckets add-iam-policy-binding \
  "gs://$WREMOTELY_GCS_BUCKET" \
  --member="serviceAccount:$BASELINE_SERVICE_ACCOUNT_EMAIL" \
  --role=roles/storage.objectViewer
```

Authenticate the Python client libraries with short-lived service-account
impersonation. This writes local Application Default Credentials (ADC), not a
service-account key. Google documents this flow under
[service-account impersonation](https://cloud.google.com/docs/authentication/use-service-account-impersonation).
The isolated gcloud configuration created above also isolates this ADC from the
normal workstation credentials. The collector requests the `cloud-platform`
OAuth scope because BigQuery `INFORMATION_SCHEMA` reads require creation of a
bounded query job; the dedicated service account's IAM roles remain the
least-privilege authorization boundary.

```bash
gcloud auth application-default login \
  --impersonate-service-account="$BASELINE_SERVICE_ACCOUNT_EMAIL"

gcloud auth application-default print-access-token >/dev/null
echo "Impersonated ADC is available."
```

## Capture And Validate

Install the locked scripts environment and create a private output directory:

```bash
cd "$DATA_PLATFORM_REPO_DIR/scripts"
uv sync --locked

umask 077
export BASELINE_OUTPUT_DIR="$HOME/wremotely-gcp-baselines/$ENVIRONMENT"
mkdir -p "$BASELINE_OUTPUT_DIR"
export BASELINE_CAPTURED_AT="$(date -u +%Y%m%dT%H%M%SZ)"
export BASELINE_OUTPUT="$BASELINE_OUTPUT_DIR/${BASELINE_CAPTURED_AT}.json"
```

Capture 30 days of comparable activity. Omit `--dbt-run-results` unless the
path is an actual retained wremotely project artifact from the measured
environment. Supplying several flags includes several explicit samples.

For the production-shaped dev validation, verify the retained split-project
artifact before supplying it:

```bash
export WREMOTELY_DBT_RUN_RESULTS="$DATA_PLATFORM_REPO_DIR/dbt/wremotely/target/run_results.json"

test -r "$WREMOTELY_DBT_RUN_RESULTS"
jq -e '
  (.results | length) > 0 and
  all(.results[]; (
    (.unique_id | type) == "string" and
    (.unique_id | split(".")[1]) == "wremotely"
  ))
' "$WREMOTELY_DBT_RUN_RESULTS"
```

```bash
uv run python src/wremotely_gcp_baseline.py \
  --gcp-project "$PROJECT_ID" \
  --bigquery-location "$WREMOTELY_BIGQUERY_LOCATION" \
  --gcs-bucket "$WREMOTELY_GCS_BUCKET" \
  --gcs-prefix "$WREMOTELY_GCS_PREFIX" \
  --publication-topic "$WREMOTELY_PUBLICATION_TOPIC" \
  --lookback-days 30 \
  --max-gcs-objects 250000 \
  --bigquery-max-bytes-billed 300000000 \
  --dbt-run-results "$WREMOTELY_DBT_RUN_RESULTS" \
  --output "$BASELINE_OUTPUT"
```

The 300 MB per-query ceiling is deliberately just above the observed
227,540,992-byte requirement of the 30-day dev jobs metadata query. A larger
environment can still fail closed and report the minimum required ceiling. Do
not raise it above the collector's 1 GB hard limit without revisiting the query
attribution design.

Validate the scope, bounds, and report completeness before using the result in
an architecture decision:

```bash
jq -e '
  .contract_version == 1 and
  .scope.product == "wremotely" and
  .scope.lookback_days == 30 and
  .scope.gcs_prefix == "wremotely" and
  .scope.bigquery_table_prefixes == [
    "wremotely__",
    "stg_wremotely__",
    "int_wremotely__"
  ] and
  .scope.bigquery_dataset_suffixes == ["_wremotely"] and
  .bigquery.current_storage.table_count > 0 and
  .gcs.listing_complete == true and
  .gcs.object_count <= .gcs.max_objects and
  .dbt.status == "collected" and
  .dbt.sample_count > 0 and
  ((tostring | contains("personal_finance")) | not)
' "$BASELINE_OUTPUT"

jq '{
  scope,
  current_storage: {
    tables: .bigquery.current_storage.table_count,
    rows: .bigquery.current_storage.total_rows,
    logical_gib: (.bigquery.current_storage.total_logical_bytes / 1073741824),
    current_physical_gib: (
      .bigquery.current_storage.current_physical_bytes / 1073741824
    ),
    time_travel_gib: (
      .bigquery.current_storage.time_travel_physical_bytes / 1073741824
    ),
    total_physical_gib: (
      .bigquery.current_storage.total_physical_bytes / 1073741824
    ),
    by_dataset: .bigquery.current_storage.by_dataset
  },
  storage_history: {
    first: .bigquery.storage_timeline[0],
    last: .bigquery.storage_timeline[-1],
    logical_growth_percent: (
      if (.bigquery.storage_timeline | length) > 1 and
        .bigquery.storage_timeline[0].average_billable_logical_bytes > 0
      then
        ((
          .bigquery.storage_timeline[-1].average_billable_logical_bytes /
          .bigquery.storage_timeline[0].average_billable_logical_bytes
        ) - 1) * 100
      else null end
    )
  },
  jobs: {
    count: .bigquery.jobs.job_count,
    failed: .bigquery.jobs.failed_job_count,
    failure_percent: (
      if .bigquery.jobs.job_count > 0 then
        (.bigquery.jobs.failed_job_count / .bigquery.jobs.job_count) * 100
      else null end
    ),
    processed_tib: (.bigquery.jobs.total_bytes_processed / 1099511627776),
    billed_tib: (.bigquery.jobs.total_bytes_billed / 1099511627776),
    slot_hours: (.bigquery.jobs.total_slot_ms / 3600000),
    summed_duration_hours: (.bigquery.jobs.total_duration_ms / 3600000)
  },
  gcs: {
    objects: .gcs.object_count,
    gib: (.gcs.total_bytes / 1073741824),
    by_age_bucket: .gcs.by_age_bucket,
    by_storage_class: .gcs.by_storage_class
  },
  pubsub: {
    messages: .pubsub.published_message_count,
    bytes: .pubsub.approximate_published_message_bytes,
    warning: .pubsub.metric_gap_warning
  },
  dbt,
  limitations
}' "$BASELINE_OUTPUT"
```

The dev capture is the implementation gate. After it passes, start a new shell,
select `ENVIRONMENT=prod` in the environment block above, and repeat every setup,
capture, and validation section against the production project. Do not reuse the
dev service account or output file for production.

If `pubsub.metric_gap_warning` is non-null despite known publications in the
window, do not report zero traffic. Resolve the monitoring gap or record the
measurement as unavailable. If the GCS object bound is exceeded, raise the
explicit limit only after confirming the intended bucket and prefix; the
collector will not write a partial report.

Use the measured storage, object count and age profile, BigQuery job volume and
slot use, publication count, and dbt wall-clock samples to size an on-premises
prototype and its restore test. The baseline does not itself choose a database,
object store, scheduler, or backup design.

## Rotation, Revocation, And Recovery

Revoke the local ADC after the capture:

```bash
gcloud auth application-default revoke
gcloud auth revoke "$OPERATOR_EMAIL"
unset CLOUDSDK_CONFIG
```

If the identity is retained for a later comparison, remove the operator's
impersonation grant between captures:

```bash
gcloud iam service-accounts remove-iam-policy-binding \
  "$BASELINE_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" \
  --member="user:$OPERATOR_EMAIL" \
  --role=roles/iam.serviceAccountTokenCreator
```

For a one-time baseline, also remove the bucket and project bindings, then
delete the dedicated identity:

```bash
gcloud storage buckets remove-iam-policy-binding \
  "gs://$WREMOTELY_GCS_BUCKET" \
  --member="serviceAccount:$BASELINE_SERVICE_ACCOUNT_EMAIL" \
  --role=roles/storage.objectViewer

for baseline_role in \
  roles/bigquery.jobUser \
  roles/bigquery.metadataViewer \
  roles/bigquery.resourceViewer \
  roles/monitoring.viewer; do
  gcloud projects remove-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$BASELINE_SERVICE_ACCOUNT_EMAIL" \
    --role="$baseline_role"
done

gcloud iam service-accounts delete \
  "$BASELINE_SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID"
```

The JSON report is reproducible for a declared time window, but some source
views are delayed. Retain the report only as long as the migration decision
needs it. If it is deleted or suspected incomplete, create fresh impersonated
ADC and rerun the same command with a new output path; do not edit a prior
report by hand.

## Validation

Run the implementation checks from `scripts/`:

```bash
uv run ruff check src/wremotely_gcp_baseline.py tests/test_wremotely_gcp_baseline.py
uv run pytest -q tests/test_wremotely_gcp_baseline.py
uv run python src/wremotely_gcp_baseline.py --help
```
