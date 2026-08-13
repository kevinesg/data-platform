# wremotely Airflow DAGs

`etl__wremotely` ingests newly crawled jobs,
`maintenance__wremotely_artifacts` removes verified-safe private ETL artifacts,
`maintenance__wremotely_lifecycle` rechecks one stable active-job bucket, and
`repair__wremotely_job_urls` performs bounded exact-URL repairs.
`repair__wremotely_classifications` replays one completed historical extraction
through raw classification load without crawling it again, while
`repair__wremotely_warehouse_classifications` rebuilds current classifications
from exact-lineage raw warehouse facts. Normal producers load raw data and
trigger `publish__wremotely_serving`, which serializes the tested dbt build,
publication hold, current serving snapshot, and publication signal. Airflow
owns dependency order, retries, and timeouts only; the private runtime owns
extract/load behavior and dbt owns transformation and blocking tests.

## Runtime inputs

All paths below must be outside this repository and readable by the host user
running Docker:

- `WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS`: service-account JSON for the
  private extract/load runtime.
- `DBT_GOOGLE_APPLICATION_CREDENTIALS`: service-account JSON for dbt.
- `WREMOTELY_DBT_JOB_CREATION_TIMEOUT_SECONDS`: maximum time dbt waits for one
  BigQuery job-creation request in the serving build. Configure `60` seconds
  in dev, QA, and prod; the serving DAG passes it only to its dbt container.
- `WREMOTELY_DBT_JOB_EXECUTION_TIMEOUT_SECONDS`: maximum time dbt waits for
  one submitted BigQuery job in the serving build. Configure `900` seconds in
  dev, QA, and prod; the serving DAG passes it to the dbt container without
  changing other Airflow dbt tasks.
- `WREMOTELY_ETL_ARTIFACTS_DIR`: durable local artifact directory mounted into
  private runtime containers. It must be writable by the private runtime
  container user.
- `WREMOTELY_HANDOFF_DATASET`: private BigQuery dataset for durable
  current-state handoff and serving tables, for
  example `handoff_<developer>` in dev and `handoff` in QA/prod.
- `WREMOTELY_PUBLICATION_TOPIC`: private Pub/Sub topic that receives only a
  committed `READY` publication ID, for example
  `wremotely-serving-publications-<developer>` in shared dev and
  `wremotely-serving-publications` in environment-isolated QA/prod projects.
- `WREMOTELY_PUBLICATION_HOLD_POLICY`: private policy file for the
  pre-publication hold step. Keep the file outside Git.
- `GROQ_API_KEY`: required only when `WREMOTELY_LOCAL_LLM_RUNTIME=groq`.
  Keep separate keys in the external QA and prod environment files and never
  put a live key in Git, an image, or a DAG command.
- `WREMOTELY_LIFECYCLE_SCHEDULE`: required only when `ENVIRONMENT=prod`;
  configure `0 6,18 * * *` so lifecycle starts six hours after each main ELT
  schedule while seven half-day runs still cover the active catalog in 3.5
  days. Keep dev/QA lifecycle runs manual.
- `WREMOTELY_ARTIFACT_CLEANUP_SCHEDULE`: required only when
  `ENVIRONMENT=prod`; configure `0 3 * * *` so cleanup runs daily between the
  00:00 ingestion and 06:00 lifecycle starts and far from the 18:17 metadata
  backup. Keep dev/QA cleanup manual.

The private runtime image is configured with `DATA_PLATFORM_WREMOTELY_ETL_IMAGE`.
Dev may keep this value in its external development environment file. QA and
prod keep it in the external deployment `images.env` manifest using the exact
immutable form
`ghcr.io/kevinesg/wremotely-etl:sha-<full-40-character-commit-sha>`.
`deploy-qa` resolves the private `qa-candidate` pointer, verifies its immutable
revision, and records that immutable image in the QA manifest; `deploy-prod`
promotes the same manifest entry. The private GHCR package must grant
`kevinesg/data-platform` read access under **Manage Actions access** so the
deployment workflows can verify and pull it without making the package public.

Timestamped wremotely run IDs use the DagRun logical date when one exists, so
scheduled replay identities remain tied to their data interval. Airflow 3 manual
DagRuns may have no logical date; those runs use `run_after` instead. Scheduled
timestamps keep the existing `YYYYMMDDTHHMMSSZ` format, while manual timestamps
retain microseconds as `YYYYMMDDTHHMMSSffffffZ` so separately triggered runs do
not share an artifact identity. Lifecycle bucket selection uses the same
resolved timestamp. Do not substitute task start time or wall-clock retry time,
because retries and task clears must keep one artifact identity and bucket.

### Durable logical refresh requests

The normal ingestion DAG reads one optional Airflow Variable named
`wremotely_refresh_request` before selecting its first EL task. An absent
variable keeps the scheduled path unchanged. A request is a JSON object with:

```json
{
  "refresh_id": "ats-parser-20260805",
  "from_step": "extract",
  "input_run_id": "20260803T001500Z-wremotely"
}
```

`from_step` must be one of `crawl`, `select`, `extract`, `job_facts`, `classify`,
`evaluate`, or `stage`. The DAG
skips earlier tasks, reuses their retained artifacts from `input_run_id`, and
passes `--full-refresh` only to the declared step and its descendants. An
`input_run_id` is required for every boundary after `crawl`; it is the complete
base run whose compatible artifacts are being reused, not a parser version or
Git SHA. Use `crawl` when no retained input is needed.

The refresh run uses the stable identity
`refresh-<refresh_id>-wremotely`, so retries and a later scheduled retry of a
failed request reuse the same artifact generation. `publish_handoff` follows a
`crawl` boundary, while `upload` and `load` are transport descendants of a
`stage` boundary; they are not valid logical refresh boundaries because their
CLI contract has no separate retained-input run ID. The DAG acknowledges and
deletes the Variable only after the serialized serving publication succeeds. A
failed run, or a request changed while a run is active, leaves the request in
place for operator review and a later retry.

To queue one reviewed refresh, use the Airflow UI Variables screen or the
equivalent CLI inside the deployed Airflow container after the environment
setup in this runbook is complete:

```bash
airflow variables set wremotely_refresh_request \
  '{"refresh_id":"ats-parser-20260805","from_step":"extract","input_run_id":"20260803T001500Z-wremotely"}'
```

Do not use this request for the lifecycle maintenance DAG. Do not map it to
dbt's command-level `--full-refresh`; the protected serving model must retain
prior rows so closure and suppression tombstones remain publishable.

The reviewed approved-source registry is bundled at
`/app/source_registry/approved_sources.jsonl` in that immutable private image.
The private runtime records its actual checksum in completed artifacts. Do not
maintain a second deployed host copy or checksum variable; the immutable image
reference binds the registry bytes to the reviewed private source commit. The
operator-specific publication-hold policy remains external and read-only.

## Handoff dataset and publication topic

The wremotely DAG uses a private BigQuery handoff dataset for current-state
handoff and serving publication tables. This dataset is not raw
warehouse history or a dbt target. It is the private exchange boundary between
pipeline steps and the serving publication worker; no table is public.

Use a descriptive dataset name such as `handoff_<developer>` for local dev and
`handoff` for QA and prod. The environment-specific GCP projects already separate
QA and prod, so deployed dataset names do not need a developer suffix. Avoid
`temp` because these tables are replaceable but still persistent handoff state;
the name can be confused with BigQuery temporary tables or data that may be
deleted at any moment. Avoid the shorthand `ops` in resource names unless a
broader operations dataset has a documented ownership contract.

The intended table behavior is:

- crawl and other upstream steps write local artifacts first;
- crawl checkpoints completed source rows in `.crawl-work` and publishes one
  canonical crawl artifact only after the run completes;
- a completed step batch-loads its current output to the handoff dataset with
  `WRITE_TRUNCATE`;
- downstream steps may start from the current handoff table instead of an exact
  upstream artifact run ID;
- publication hold merges final per-job decisions only after its local
  checkpoint completes;
- final serving tables retain current job state and advance `_updated_at` only
  for newly ingested, changed, closed, suppressed, or reactivated jobs;
- durable raw tables that dbt reads remain separate and are updated only by the
  core load step.

This keeps long-running discovery and crawl work from blocking manual runs that
only need the latest already-published handoff table. It also keeps handoff
replacement single-writer: only `publish_handoff` replaces the handoff table
after `crawl` has published a complete canonical crawl artifact.

### Migrate serving publication tables to current state

This migration does not require rerunning crawl, select, extract, job facts,
classification, lifecycle recheck, or dbt. Deploy the corrected private ETL
image and DAG first. The latest run's completed publication-hold artifact uses
the former candidate checksum contract, so back it up before clearing tasks:

```bash
export WREMOTELY_BASE_RUN_ID="<logical-date-as-YYYYMMDDTHHMMSSZ>-wremotely"
export COMPLETED_PUBLICATION_HOLD_RUN_DIR="$WREMOTELY_ETL_ARTIFACTS_DIR/$WREMOTELY_BASE_RUN_ID-publication-hold"
export COMPLETED_PUBLICATION_HOLD_DIR="$COMPLETED_PUBLICATION_HOLD_RUN_DIR/publication_hold"
export COMPLETED_PUBLICATION_HOLD_BACKUP="${COMPLETED_PUBLICATION_HOLD_DIR}.pre-final-verdict-migration"
export COMPLETED_SERVING_SNAPSHOT_RUN_DIR="$WREMOTELY_ETL_ARTIFACTS_DIR/$WREMOTELY_BASE_RUN_ID-serving-snapshot"
export COMPLETED_SERVING_SNAPSHOT_DIR="$COMPLETED_SERVING_SNAPSHOT_RUN_DIR/publish_serving_snapshot"
export COMPLETED_SERVING_SNAPSHOT_BACKUP="${COMPLETED_SERVING_SNAPSHOT_DIR}.pre-current-state-migration"

test -d "$COMPLETED_PUBLICATION_HOLD_DIR"
test ! -e "$COMPLETED_PUBLICATION_HOLD_BACKUP"
test -d "$COMPLETED_SERVING_SNAPSHOT_DIR"
test ! -e "$COMPLETED_SERVING_SNAPSHOT_BACKUP"
sudo mv -- "$COMPLETED_PUBLICATION_HOLD_DIR" "$COMPLETED_PUBLICATION_HOLD_BACKUP"
sudo mv -- "$COMPLETED_SERVING_SNAPSHOT_DIR" "$COMPLETED_SERVING_SNAPSHOT_BACKUP"
```

Use the Airflow run's logical date for `WREMOTELY_BASE_RUN_ID`, not the current
wall-clock time. Airflow's rootless Docker container can create the run
directory under a remapped host owner, so the host-side rename requires
administrator permission even when the artifacts root belongs to the operator.
Keep both backups until end-to-end worker validation succeeds. The serving
snapshot artifact is contract-versioned so a normal replay cannot silently
accept completion evidence from an incompatible publication contract.
Then clear only `publication_hold`, `publish_serving_snapshot`, and
`signal_publication` in that run, with upstream tasks unselected. The durable
legacy BigQuery verdicts exclude all previously evaluated jobs, so the task
does not call the model; it writes a new zero-candidate run artifact and
initializes `wremotely__publication_holds`. The existing dbt marts initialize
the unversioned serving tables.

After `publish_serving_snapshot` succeeds, verify the new tables before
granting the serving worker access:

```bash
for TABLE_NAME in \
  wremotely__publication_holds \
  wremotely__serving_jobs \
  wremotely__serving_companies \
  wremotely__serving_job_country_eligibility \
  wremotely__serving_publication; do
  bq show \
    --project_id="$PROJECT_ID" \
    --format=prettyjson \
    "$PROJECT_ID:$WREMOTELY_HANDOFF_DATASET.$TABLE_NAME" >/dev/null
done
```

Apply the PostgreSQL migration and validate the worker against the current
publication before removing legacy BigQuery tables. Once that validation
succeeds, remove the old snapshot/history tables; they are not runtime inputs:

```bash
for TABLE_NAME in \
  wremotely__publication_holds_current \
  wremotely__serving_jobs_versions \
  wremotely__serving_jobs_versions_v5 \
  wremotely__serving_companies_versions \
  wremotely__serving_job_country_eligibility_versions \
  wremotely__serving_publications; do
  if bq show \
    --project_id="$PROJECT_ID" \
    "$PROJECT_ID:$WREMOTELY_HANDOFF_DATASET.$TABLE_NAME" >/dev/null 2>&1; then
    bq rm \
      --project_id="$PROJECT_ID" \
      --force \
      --table \
      "$PROJECT_ID:$WREMOTELY_HANDOFF_DATASET.$TABLE_NAME"
  fi
done
```

The approved source registry remains reviewed and versioned in the private
source repository, then ships in the immutable private runtime image. Do not
upload the approved registry to BigQuery as the source of truth or create an
independent deployed host copy.

Create or verify the runtime's BigQuery access and handoff dataset as a platform
maintainer before enabling a DAG or private runtime command that references
them. The ETL service account needs project-level job creation, dataset-level
read/write access on the raw dataset, and dataset-level read/write access on the
handoff dataset. The same account reads raw history during `select`, writes raw
tables during `load`, and reads tested dbt candidate relations after
`dbt_build`. `roles/bigquery.dataEditor` is intentional on raw and handoff;
`roles/bigquery.dataViewer` is sufficient for ETL reads from the dbt mart, and
the dbt service account receives `roles/bigquery.dataEditor` on that mart.
Tables created later inherit these dataset grants, so bootstrap does not need
per-table IAM. The setup uses `gcloud` and `bq`, installed with Google Cloud
CLI.

```bash
export PROJECT_ID="${PROJECT_ID:-kevinesg-dev}"
export ENVIRONMENT="${ENVIRONMENT:-dev}"
export BIGQUERY_LOCATION="${BIGQUERY_LOCATION:-US}"
export DEVELOPER_ID="${DEVELOPER_ID:-kevinesg}"
export PLATFORM_BOOTSTRAP_CONFIGURATION="${PLATFORM_BOOTSTRAP_CONFIGURATION:-data-platform-bootstrap-dev}"
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$DATA_PLATFORM_SECRETS_DIR/.env}"

test -s "$DATA_PLATFORM_ENV_FILE"
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a

if test -z "${WREMOTELY_HANDOFF_DATASET:-}"; then
  if test "$ENVIRONMENT" = dev; then
    export WREMOTELY_HANDOFF_DATASET="handoff_${DEVELOPER_ID}"
  else
    export WREMOTELY_HANDOFF_DATASET="handoff"
  fi
fi

if test -z "${WREMOTELY_PUBLICATION_TOPIC:-}"; then
  if test "$ENVIRONMENT" = dev; then
    export WREMOTELY_PUBLICATION_TOPIC="wremotely-serving-publications-${DEVELOPER_ID}"
  else
    export WREMOTELY_PUBLICATION_TOPIC="wremotely-serving-publications"
  fi
fi

export WREMOTELY_ETL_SERVICE_ACCOUNT_EMAIL="$(
  python -c 'import json, os; print(json.load(open(os.environ["WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS"]))["client_email"])'
)"
export DBT_SERVICE_ACCOUNT_EMAIL="$(
  python -c 'import json, os; print(json.load(open(os.environ["DBT_GOOGLE_APPLICATION_CREDENTIALS"]))["client_email"])'
)"

if test "$ENVIRONMENT" = qa || test "$ENVIRONMENT" = prod; then
  export WREMOTELY_DBT_MART_DATASET="mart_wremotely"
else
  export WREMOTELY_DBT_MART_DATASET="${DBT_DATASET}_mart_wremotely"
fi

gcloud config configurations activate "$PLATFORM_BOOTSTRAP_CONFIGURATION"
gcloud config set project "$PROJECT_ID"
gcloud config list

if gcloud services list \
  --enabled \
  --project="$PROJECT_ID" \
  --filter='config.name=pubsub.googleapis.com' \
  --format='value(config.name)' | grep -Fxq pubsub.googleapis.com; then
  echo "Pub/Sub API is enabled."
else
  gcloud services enable pubsub.googleapis.com --project="$PROJECT_ID"
fi

if gcloud pubsub topics describe "$WREMOTELY_PUBLICATION_TOPIC" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Publication topic already exists: $WREMOTELY_PUBLICATION_TOPIC"
else
  gcloud pubsub topics create "$WREMOTELY_PUBLICATION_TOPIC" \
    --project="$PROJECT_ID"
fi

gcloud pubsub topics add-iam-policy-binding "$WREMOTELY_PUBLICATION_TOPIC" \
  --project="$PROJECT_ID" \
  --member="serviceAccount:$WREMOTELY_ETL_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/pubsub.publisher"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$WREMOTELY_ETL_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/bigquery.jobUser"

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataEditor\`
   ON SCHEMA \`$PROJECT_ID\`.$RAW_DATASET
   TO \"serviceAccount:$WREMOTELY_ETL_SERVICE_ACCOUNT_EMAIL\""

if bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$WREMOTELY_DBT_MART_DATASET" >/dev/null 2>&1; then
  echo "wremotely mart dataset already exists: $PROJECT_ID:$WREMOTELY_DBT_MART_DATASET"
else
  bq --location="$BIGQUERY_LOCATION" mk \
    --dataset \
    "$PROJECT_ID:$WREMOTELY_DBT_MART_DATASET"
fi

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataEditor\`
   ON SCHEMA \`$PROJECT_ID\`.$WREMOTELY_DBT_MART_DATASET
   TO \"serviceAccount:$DBT_SERVICE_ACCOUNT_EMAIL\""

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataViewer\`
   ON SCHEMA \`$PROJECT_ID\`.$WREMOTELY_DBT_MART_DATASET
   TO \"serviceAccount:$WREMOTELY_ETL_SERVICE_ACCOUNT_EMAIL\""

if bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$WREMOTELY_HANDOFF_DATASET"; then
  echo "wremotely handoff dataset already exists: $PROJECT_ID:$WREMOTELY_HANDOFF_DATASET"
else
  echo "Create the handoff dataset only when the bq show output says Not found."
  read -r -p "Create handoff dataset $PROJECT_ID:$WREMOTELY_HANDOFF_DATASET? [y/N] " CREATE_WREMOTELY_HANDOFF_DATASET
  if test "$CREATE_WREMOTELY_HANDOFF_DATASET" = y; then
    bq --location="$BIGQUERY_LOCATION" mk \
      --dataset \
      "$PROJECT_ID:$WREMOTELY_HANDOFF_DATASET"
  fi
fi

bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataEditor\`
   ON SCHEMA \`$PROJECT_ID\`.$WREMOTELY_HANDOFF_DATASET
   TO \"serviceAccount:$WREMOTELY_ETL_SERVICE_ACCOUNT_EMAIL\""

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$RAW_DATASET"

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$WREMOTELY_HANDOFF_DATASET"

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$WREMOTELY_DBT_MART_DATASET"

gcloud pubsub topics describe "$WREMOTELY_PUBLICATION_TOPIC" \
  --project="$PROJECT_ID"

gcloud pubsub topics get-iam-policy "$WREMOTELY_PUBLICATION_TOPIC" \
  --project="$PROJECT_ID" \
  --flatten='bindings[].members' \
  --filter="bindings.role=roles/pubsub.publisher AND bindings.members=serviceAccount:$WREMOTELY_ETL_SERVICE_ACCOUNT_EMAIL" \
  --format='table(bindings.role,bindings.members)'
```

After creating or verifying the dataset and topic, add or update both values in
the external Airflow environment file. Use `handoff_<developer>` and
`wremotely-serving-publications-<developer>` in shared dev; QA/prod use
`handoff` and `wremotely-serving-publications` because their projects already
isolate the environments.

```bash
python -c 'import os
from pathlib import Path

env_file = Path(os.environ["DATA_PLATFORM_ENV_FILE"])
lines = env_file.read_text().splitlines()
required_values = {
    "WREMOTELY_HANDOFF_DATASET": os.environ["WREMOTELY_HANDOFF_DATASET"],
    "WREMOTELY_PUBLICATION_TOPIC": os.environ["WREMOTELY_PUBLICATION_TOPIC"],
    "WREMOTELY_PLATFORM_WORKER_COUNT": "2",
    "WREMOTELY_RECHECK_WORKER_COUNT": "16",
}
for name, value in required_values.items():
    updated = False
    for index, line in enumerate(lines):
        if line.startswith(f"{name}="):
            if name in {"WREMOTELY_HANDOFF_DATASET", "WREMOTELY_PUBLICATION_TOPIC"}:
                lines[index] = f"{name}={value}"
            updated = True
            break
    if not updated:
        lines.append(f"{name}={value}")
env_file.write_text("\n".join(lines) + "\n")
'
```

## Task order

The ingestion DAG loads new-job data before triggering the serialized
publication DAG:

```text
read_refresh_request
  -> choose_refresh_start
  -> crawl
  -> publish_handoff
  -> select
  -> extract
  -> job_facts
  -> classify
  -> evaluate
  -> stage
  -> upload
  -> load
  -> trigger_publication
  -> acknowledge_refresh_request
```

`choose_refresh_start` branches to one dedicated start gate per core EL task.
The selected gate and its descendants use `none_failed_min_one_success` so
skipped upstream tasks do not block a downstream-only rebuild; the linear edges
still preserve normal execution order.

The independent lifecycle DAG runs:

```text
prepare_recheck
  -> recheck
  -> stage_recheck
  -> upload_recheck
  -> load_recheck
  -> trigger_publication
```

The trigger-only publication DAG runs at most one active DAG run:

```text
dbt_build
  -> publication_hold
  -> publish_serving_snapshot
  -> signal_publication
```

The manual repair DAG starts from the current source-crawl handoff table and
runs `select -> extract -> job_facts -> classify -> evaluate -> stage -> upload
-> load -> trigger_publication`. Its trigger form requires 1-100 unique absolute
job URLs. Each URL must exist in the current source-crawl handoff table; the
selector fails rather than broadening the repair when a requested URL is absent.

The manual historical-classification repair DAG runs:

```text
job_facts
  -> classify
  -> evaluate
  -> stage
  -> upload
  -> load
```

It intentionally stops before dbt and publication. Operators process completed
extraction runs oldest to newest, then trigger the serialized publication DAG
once after the final load. Every task has an eight-hour execution timeout and
the complete DAG has a 24-hour timeout.

The manual warehouse-classification repair DAG runs:

```text
prepare
  -> replay
  -> stage
  -> upload
  -> load
```

It reads and repairs only the current environment's own raw warehouse facts.
It never copies dev tables into QA or prod. It also stops before dbt and
publication so operators can verify the raw load before triggering the
serialized publication DAG. Every task has an eight-hour execution timeout and
the complete DAG has a 24-hour timeout.

The normal scheduled path does not acquire new sources. It starts from the
approved source snapshot, selects unseen job URLs, loads raw BigQuery tables,
and only then builds the dbt serving snapshot.
The intended production ingestion cadence is every 12 hours (`0 */12 * * *`).
Lifecycle runs every 12 hours at 06:00 and 18:00 UTC (`0 6,18 * * *`), six hours
after each main ELT schedule, with seven stable buckets and 16 internal workers.
Artifact cleanup runs daily at 03:00 UTC (`0 3 * * *`) and retains three
complete days of local and GCS artifacts.
Each scheduled run owns one complete bucket, so seven successful runs cover the
active catalog in 3.5 days. The offset reduces routine contention but does not
guarantee separation when a producer exceeds six hours; the one-slot network
and warehouse pools remain the concurrency controls. Bucket size grows with the
active catalog; monitor actual run duration, network-pool queue delay, retries,
and completion before the next lifecycle interval. Cleanup and lifecycle are
manual in dev and QA; repair and publication are unscheduled in every
environment.

These DAGs include every implemented step required to create and refresh the
BigQuery serving publication. They intentionally do not run search-provider
`discover`, offline crawl merging, or classifier benchmarks. Those are
source-acquisition or evaluation workflows with different budgets and
cadences. The producer DAGs publish the exact committed `READY` publication ID
to Pub/Sub. The VPS publication worker consumes that signal and applies the
bounded serving snapshot to PostgreSQL.

Artifact cleanup is an independent maintenance DAG so ingestion or publication
failure does not suppress retention and cleanup failure does not block
publication. Its one task invokes the private runtime with:

```text
cleanup
  --cleanup-min-age-days 3
  --cleanup-gcs
  --cleanup-apply
```

The task uses the environment's mounted `WREMOTELY_ETL_ARTIFACTS_DIR`, exact
GCP project, bucket, and wremotely prefix. It shares the one-slot
`wremotely_warehouse` pool with raw loads and publication mutations. The
private runtime scans every repository-owned run output and interrupted work
directory. It deletes exact manifest-listed GCS generations only for
checksum-verified loaded stage runs, deletes each stage `_SUCCESS` marker last,
and removes eligible local run directories only after all eligible GCS
deletions succeed. It retains recent, uploaded-not-loaded, staged-not-loaded,
and primary local-only artifacts. It never deletes BigQuery rows, source
registry inputs, other GCS prefixes or buckets, Airflow logs, Docker logs, or
personal-finance files.

### Artifact cleanup rollout and recovery

The service account needs the bucket-scoped object read/delete permission
documented by the private ETL warehouse setup. Before unpausing cleanup in an
environment, run the private ETL README's cleanup report against that
environment's mounted artifact root, project, bucket, and prefix with
`--cleanup-min-age-days 3 --cleanup-gcs` and without `--cleanup-apply`. Inspect
`cleanup.json`, `cleanup_candidates.jsonl`, and
`gcs_cleanup_candidates.jsonl`; investigate any unexpected prefix, bucket,
eligibility reason, or run identity before enabling deletion.

After the report is accepted, trigger `maintenance__wremotely_artifacts`
manually once in dev or QA. Confirm its `cleanup.json` reports only expected
eligible/deleted counts and that retained reasons include every recent,
uploaded-not-loaded, staged-not-loaded, and primary local-only run. Prod uses
the same immutable private image and exact command, with the schedule enabled
only after this smoke check.

Pause `maintenance__wremotely_artifacts` to disable future deletion; pausing
does not remove state. If GCS deletion fails, the private runtime leaves local
upload/load evidence and the cleanup run incomplete, so clear or retry the same
task after correcting credentials, IAM, bucket, prefix, or connectivity. If a
cleanup task completed, rerunning the same DAG run verifies and reuses its
completed report; trigger a new DAG run to scan a fresh artifact snapshot.
Deleted local replay files are not reconstructed by Airflow. Recovery uses the
durable BigQuery data or surviving GCS objects where available, so policy
changes must be tested through dry-run before the schedule is re-enabled.

`evaluate` and `stage` consume the completed selection, extraction, job-facts,
and classification artifacts. They do not require the same DAG run's crawl
artifact because `select` reads the current crawl handoff table. This permits a
manual run to reuse an already-published crawl handoff without fabricating a
matching local crawl directory or replaying crawl.

Discovery and source-crawl artifacts are operational handoff inputs, not dbt
serving sources, so the dbt graph does not require raw discovery/crawl tables.
`load` does guarantee that the lifecycle-recheck raw source exists with the
standard empty envelope schema before dbt runs; an environment with no recheck
rows is valid and must not fail dbt because the relation is absent.

`crawl` is a single Airflow task because the private runtime checkpoints
completed source-registry rows in a durable `.crawl-work` directory. It can run
multiple internal crawl workers, controlled by
`WREMOTELY_SOURCE_CRAWL_WORKER_COUNT`. Recognized platforms run one stream per
tenant and no more than `WREMOTELY_PLATFORM_WORKER_COUNT` tenants concurrently;
ordinary domains keep one stream per domain. The dev defaults are `6` global
crawl workers and `2` tenants per platform. If the task fails or the
worker stops, clearing and rerunning `crawl` resumes after the last committed
source rows instead of starting from the top of the approved source snapshot.
Active worker rows that had not committed yet may run again. The private
runtime does not run two rows from the same tenant concurrently. It removes
equivalent Workday tenant/site rows before scheduling and stops Workday API
pagination once the configured per-page URL budget is satisfied.

`crawl` has an explicit 18-hour execution timeout because a full reviewed
registry can exceed the two-hour default used by smaller tasks. Docker tasks
use forced container removal on failure or timeout so an orphaned runtime
cannot keep writing a checkpoint while an Airflow retry starts.
Every wremotely task has a bounded execution timeout. Smaller Docker tasks and
dbt use their shared two-hour limits, measured network and repair work has a
larger explicit limit, and publication-trigger waits are bounded at 12 hours.
The DAG-level timeout remains the final bound for each complete workflow.

`publication_hold` reads `serving_jobs` from the generated wremotely mart
dataset (`${DBT_DATASET}_mart_wremotely` in dev and `mart_wremotely` in
QA/prod).
Before invoking the configured inference runtime, it keeps only candidates
whose trimmed, uppercased structured title contains `DATA ENGINEER`,
`ANALYTICS ENGINEER`, `SQL DEVELOPER`, or `ETL ENGINEER`. Its checkpoint is
then narrowed by the policy's remote-only and direct target-country requirements.
Validated country eligibility takes precedence over possible visa-support text,
and jobs filtered from this operator-specific evaluation remain serveable for
countries where they are eligible. Verdicts are final by job ID; later content,
policy, prompt, runtime, or model changes do not reevaluate an existing verdict.

The lifecycle DAG reads the current serving handoff plus raw selected-job
metadata and lifecycle history. The private selector removes `is_deleted=true`
rows before assigning active `job_id` values to seven stable hash buckets.
Airflow derives one bucket index from the resolved DagRun timestamp and
processes the complete bucket with no scheduled row cap. Scheduled runs use
their 12-hour logical date; manual runs use `run_after`. Seven successful
scheduled runs therefore cover every currently active serving job over 3.5 days
without one growing all-at-once fetch burst. It completes successfully with an
empty bucket.
Explicit closed-page evidence sets `is_deleted`; terminal HTTP outcomes require
two consecutive rechecks. The workflow loads lifecycle events before triggering
the serialized publication DAG, and dbt retains rows with `is_deleted = true`
and advances `_updated_at` instead of removing them.
Classification reconciliation uses the same serving tombstone for a previously
served row that no longer meets publication requirements; the warehouse status
model keeps that reason distinct from lifecycle closure. A source-declared
`validThrough` boundary is also an explicit suppression reason. Date-only
values remain valid through that UTC date; values with a time expire at their
exact timestamp.
`WREMOTELY_RECHECK_WORKER_COUNT` controls total internal concurrency while
`WREMOTELY_PLATFORM_WORKER_COUNT` caps concurrent tenants per recognized ATS;
the runtime still serializes each tenant or ordinary domain.

The one-slot `wremotely_network` pool prevents crawl, extraction, repair, and
lifecycle containers from fetching concurrently across DAGs. The one-slot
`wremotely_warehouse` pool prevents producer raw loads from overlapping dbt
builds or serving publication writes. The trigger-only publication DAG has
`max_active_runs=1`, so producer timing cannot interleave two complete
dbt/publication chains. Producer trigger tasks wait deferrably and fail when the
linked publication run fails. Keep the trigger-only publication DAG unpaused
before running a producer DAG. These controls protect cross-DAG boundaries; the
private runtime remains responsible for safe internal concurrency and checkpoints.

The externally mounted policy file owns the complete model prompt and its
operator-specific structured configuration, including target-country,
visa/residency, compensation, work-arrangement, and stack settings. The private
runtime renders only job context/text placeholders and applies generic schema,
evidence, and fail-closed decision mechanics. The DAG must not duplicate those
personal policy values in its command.

Publication-hold rows include a bounded model factual summary, validation
warnings, and deterministic decision justification/factors. The private
handoff row keeps those audit fields without storing raw model responses or
chain-of-thought.

Publication hold evaluates only matching jobs without an existing verdict.
Verdicts are final by job ID and remain in `wremotely__publication_holds`;
later policy, prompt, model, or content changes do not reprocess them.
`publish_serving_snapshot` anti-joins `held` and `review_hold` decisions, then
transactionally merges newer `_updated_at` rows into
`wremotely__serving_jobs`, `wremotely__serving_companies`, and
`wremotely__serving_job_country_eligibility`. It updates the singleton `READY`
control row in `wremotely__serving_publication`. The pipeline performs no
physical serving-row deletes; lifecycle removal is represented by `is_deleted`.
The task also passes the approved-source snapshot bundled in the same immutable
private image. The runtime records its actual checksum, counts distinct enabled
approved source IDs for company-career and ATS-company sources, then includes
those bounded totals in the same publication identity and control-row
transaction. Airflow does not inspect registry rows or derive the counts.

`signal_publication` reads the completed local snapshot artifact, verifies that
its exact publication ID still has one `READY` control row in BigQuery, and
publishes only that UTF-8 publication ID as the Pub/Sub message data. It runs in
the scripts image with read-only mounts for the ETL credential and artifact
directory. Airflow retries or manual task clears may publish a duplicate; this
is intentional, and the serving worker must use its PostgreSQL publication
ledger to make duplicate IDs no-ops. If signaling fails, clear only
`signal_publication`; do not rebuild the snapshot.

The VPS worker consumes an environment-specific pull subscription with its own
least-privilege subscriber identity. Pub/Sub does not retain topic messages for
a subscription that does not yet exist.

## Historical classification reconciliation

Use this procedure only after a reviewed classifier change must be applied to
completed extraction artifacts. It is a controlled repair, not a scheduled
workflow. The deployed private ETL image must contain the reviewed classifier
and the `classification_replay` staging contract. The deployed data-platform
images must contain this DAG and the serving-tombstone reconciliation model.

Before triggering the repair, use the deployment host and external production
environment documented in `deploy/README.md`. Verify the runtime file and list
completed extraction runs without modifying artifacts:

```bash
test -s "$DATA_PLATFORM_ENV_FILE"
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a

test -d "$WREMOTELY_ETL_ARTIFACTS_DIR"
find "$WREMOTELY_ETL_ARTIFACTS_DIR" \
  -mindepth 3 \
  -maxdepth 3 \
  -path '*/extract/_SUCCESS' \
  -printf '%h\n' |
sed 's#/extract$##; s#.*/##' |
sort
```

In the Airflow UI:

1. Pause `etl__wremotely`, `maintenance__wremotely_lifecycle`, and
   `repair__wremotely_job_urls`. Wait for every active wremotely producer or
   publication run to finish before loading historical observations.
2. Leave `repair__wremotely_classifications` available for manual triggers.
3. Trigger one repair run for each completed extraction ID, strictly oldest to
   newest. Use the same reviewed `replay_label` for every run, such as
   `classification-reconciliation-v1`, and wait for all six tasks to succeed
   before starting the next extraction ID.
4. After the final replay load succeeds, trigger
   `publish__wremotely_serving` once with a new `publication_run_id`. Do not run
   dbt with `--full-refresh`; the incremental serving model needs its prior rows
   to emit explicit suppression tombstones. The model overrides command-level
   full-refresh requests as a defense in depth; a missing serving target is a
   recovery condition, not permission to publish a state-less rebuild.
5. Wait for `dbt_build`, `publication_hold`, `publish_serving_snapshot`, and
   `signal_publication` to succeed, then verify the VPS worker applies that exact
   publication ID before resuming producers.

Use these read-only BigQuery checks after `dbt_build` and before resuming the
normal DAGs:

```sql
SELECT
    publication_status
    , publication_status_reason
    , COUNT(*) AS candidate_count
FROM `<project>.intermediate.int_wremotely__job_publication_status`
GROUP BY publication_status, publication_status_reason
ORDER BY publication_status, candidate_count DESC;

SELECT
    COUNTIF(NOT is_deleted) AS active_job_count
    , COUNTIF(is_deleted) AS tombstone_job_count
FROM `<project>.mart_wremotely.serving_jobs`;
```

Reusing the same extraction ID and replay label is a verification replay: the
private runtime validates completed local/GCS/load artifacts and skips matching
work. A changed classifier must use a new replay label. Do not clear an older
repair into a newer raw history or process extraction runs out of order.

## Warehouse classification reconciliation

Use this procedure when retained local extraction artifacts do not cover the
complete current warehouse population. This is an unscheduled repair. It
rebuilds classification from exact-lineage latest raw job facts in the selected
environment, stages only classification and country-eligibility observations,
and appends them through the normal immutable upload/load contract.

Prerequisites:

1. Merge and deploy the reviewed private ETL image containing
   `prepare-classification-replay-from-warehouse`,
   `replay-classification`, and the
   `warehouse_classification_replay` stage contract.
2. Merge and deploy the reviewed data-platform dbt image containing the
   country-eligibility models and blocking tests validated for the replay.
3. Merge and deploy the data-platform Airflow image containing
   `repair__wremotely_warehouse_classifications`.
4. Start with dev. Do not use dev success as permission to copy dev raw, dbt,
   handoff, serving, or publication tables into prod.
5. Choose one stable classifier revision label, such as `classification-v13`.
   Use that same label in dev and prod only when both environments run the same
   immutable private ETL image.

In the Airflow UI for dev:

1. Pause `etl__wremotely`, `maintenance__wremotely_lifecycle`,
   `repair__wremotely_job_urls`, and
   `repair__wremotely_classifications`. Wait for every active wremotely
   producer, cleanup, repair, or publication run to finish.
2. Trigger `repair__wremotely_warehouse_classifications` with the chosen
   `replay_label`. Wait for `prepare`, `replay`, `stage`, `upload`, and `load`
   to succeed.
3. Trigger `publish__wremotely_serving` once with a new
   `publication_run_id`. Do not run dbt with `--full-refresh`; the incremental
   serving model needs its prior rows to emit explicit suppression tombstones.
   The model overrides command-level full-refresh requests as a defense in
   depth. If its prior target is missing or damaged, stop and recover that state
   before publication.
4. Wait for all publication tasks to succeed. Verify that the serving worker
   applies that exact publication ID and that the read-only checks below return
   no contract failures before resuming the paused DAGs.

Run these read-only checks against the same environment after `dbt_build`:

```sql
SELECT
    COUNT(*) AS latest_job_fact_count
    , COUNTIF(c.candidate_id IS NULL) AS missing_latest_classification_count
    , COUNTIF(
        c.latest_classification_source_content_sha256
            IS DISTINCT FROM j.latest_job_fact_source_content_sha256
        OR c.latest_classification_normalized_text_sha256
            IS DISTINCT FROM j.latest_job_fact_normalized_text_sha256
        OR c.latest_classification_jsonld_sha256
            IS DISTINCT FROM j.latest_job_fact_jsonld_sha256
    ) AS classification_lineage_mismatch_count
FROM `<project>.intermediate.int_wremotely__latest_job_facts` AS j
LEFT JOIN `<project>.intermediate.int_wremotely__latest_classifications` AS c
    USING (candidate_id);

SELECT
    validated_country_eligibility_scope
    , COUNT(*) AS candidate_count
FROM `<project>.intermediate.int_wremotely__candidate_country_eligibility`
GROUP BY validated_country_eligibility_scope
ORDER BY validated_country_eligibility_scope;

SELECT
    latest_remote_scope
    , COUNT(*) AS candidate_count
FROM `<project>.intermediate.int_wremotely__latest_classifications`
GROUP BY latest_remote_scope
ORDER BY latest_remote_scope;

SELECT
    publication_status
    , publication_status_reason
    , COUNT(*) AS candidate_count
FROM `<project>.intermediate.int_wremotely__job_publication_status`
GROUP BY publication_status, publication_status_reason
ORDER BY publication_status, candidate_count DESC;
```

The first query must report zero missing latest classifications and zero
lineage mismatches. Review the complete scope, work-arrangement, and publication
distributions against the dev validation record before promoting the immutable
images.

After dev succeeds, repeat the same pause, trigger, publication, validation, and
resume sequence in prod. The prod DAG reads prod raw facts and generates a prod
publication; it does not consume dev artifacts. Reusing the same replay label
in the same environment verifies and skips matching local, GCS, and load
artifacts. A classifier change requires a new label.

## Successful task clear and replay behavior

Idempotency is defined against the same declared run ID, configuration, and
input artifacts. It does not mean an old DAG run can safely replace newer
current-state handoff tables, nor does it provide disaster recovery after
someone manually deletes verified external data.

- `crawl`, `select`, `extract`, `job_facts`, `classify`, `evaluate`,
  `prepare-classification-replay-from-warehouse`, `replay-classification`,
  `prepare_recheck`, `recheck`, `stage`, and `stage_recheck` verify their
  completed local artifacts and return without repeating successful work.
  Incomplete crawl/extract/recheck work resumes from committed checkpoints;
  only uncommitted external reads may repeat.
- `upload` and `upload_recheck` verify immutable GCS object names, sizes, and
  checksums. A missing object after a completed upload is an error rather than
  an implicit recreation.
- `load` and `load_recheck` use run/source checksums to verify append-only raw
  rows and reject conflicting rows. A completed local load artifact returns
  without submitting another load job.
- `publish_handoff` and `publication_hold` reapply the same completed rows to
  replaceable current-state tables. Repeating the current run has the same
  result, but clearing one of these tasks in an older DAG run after a newer run
  can roll current state backward. Do not clear historical current-state
  publisher tasks unless that rollback is intentional.
- `dbt_build` rebuilds deterministic tables from warehouse state visible when
  it runs. It is repeatable while raw inputs are unchanged, but an old task
  cleared after newer raw loads consumes the newer warehouse state. It is not a
  run-pinned historical reconstruction. dbt unit tests must pass against dev
  before the immutable image is published but are excluded from this
  production-data build; all selected data tests remain blocking. The serving
  build has a 30-minute Airflow execution timeout per attempt. BigQuery job
  creation and execution
  use `WREMOTELY_DBT_JOB_CREATION_TIMEOUT_SECONDS` and
  `WREMOTELY_DBT_JOB_EXECUTION_TIMEOUT_SECONDS`. Exceeding a bound fails the
  attempt, stops and removes the task container, and preserves the last
  complete downstream serving publication. If Docker has already removed the
  container, timeout cleanup treats that absence as an idempotent no-op so the
  original Airflow timeout remains the reported failure. A failed dbt build
  also preserves its temporary target directory under
  `$WREMOTELY_ETL_ARTIFACTS_DIR/dbt-failures/`; this includes the
  `run_results.json` and manifest needed by native `dbt retry`. The prior
  successful baseline artifact is never replaced by a failed or partial run.
  The runner keeps only the five newest failed target directories so repeated
  failures cannot grow the artifact volume without bound.
  Retrying is an explicit operator action after the cause is corrected; the
  serving DAG does not silently retry from an old target directory.
- `publish_serving_snapshot` is content-addressed. Replaying the same completed
  run verifies the bundled approved-source checksum recorded by the private
  runtime and the source-coverage aggregate before returning its publication
  ID. Recreating the same serving and source-coverage snapshot through a new run
  resolves to the same immutable publication ID.
- `signal_publication` is intentionally at-least-once. Every successful clear
  may receive a new Pub/Sub message ID for the same publication ID. The serving
  worker must acknowledge only after its PostgreSQL transaction commits and
  use the publication ledger to make duplicate publication IDs no-ops.

For a current run, clear only the failed task and the downstream tasks that
need to continue. For a historical run, prefer a new manual DAG run or an
explicit recovery procedure instead of clearing `publish_handoff`,
`publication_hold`, or `dbt_build`.

Replacing the host policy file creates a new inode and can remove its container
ACL. After every replacement, reset ordinary permissions, grant only read
access to private runtime UID `10001`, and verify the effective ACL:

```bash
chmod 600 "$WREMOTELY_PUBLICATION_HOLD_POLICY"
setfacl -m u:10001:r-- "$WREMOTELY_PUBLICATION_HOLD_POLICY"

stat -c 'mode=%a owner=%U group=%G' "$WREMOTELY_PUBLICATION_HOLD_POLICY"
getfacl -cp "$WREMOTELY_PUBLICATION_HOLD_POLICY"
```

After the named ACL is added, `stat` commonly reports mode `640` because the
group mode bits represent the ACL mask. The required effective entries are
`user::rw-`, `user:10001:r--`, `group::---`, and `other::---`; the owning group
still has no access.

An in-progress checkpoint created by an older evaluator or pre-dbt input
contract cannot be resumed because its configuration and candidate identity
contract differ.
Stop the running `publication_hold` task first, then quarantine only that task
run's old work directory before rebuilding the image and clearing the task:

```bash
export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$HOME/dev/secrets/data-platform/.env}"
test -s "$DATA_PLATFORM_ENV_FILE"
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a

export PUBLICATION_HOLD_RUN_ID="<logical-date-as-YYYYMMDDTHHMMSSZ>-wremotely-publication-hold"
export PUBLICATION_HOLD_WORK_DIR="$WREMOTELY_ETL_ARTIFACTS_DIR/$PUBLICATION_HOLD_RUN_ID/.publication-hold-work"
export PUBLICATION_HOLD_WORK_BACKUP="${PUBLICATION_HOLD_WORK_DIR}.pre-deterministic-v3"

test -d "$PUBLICATION_HOLD_WORK_DIR"
test ! -e "$PUBLICATION_HOLD_WORK_BACKUP"
find "$PUBLICATION_HOLD_WORK_DIR" -maxdepth 1 -type f -printf '%f\n'
sudo mv -- "$PUBLICATION_HOLD_WORK_DIR" "$PUBLICATION_HOLD_WORK_BACKUP"
```

Use the logical date shown by the Airflow DAG run, not the wall-clock restart
time. Keep the quarantined directory until the replacement task succeeds. A
new DAG run needs no quarantine because it has a new publication-hold run ID.

To preserve expensive completed upstream work while migrating an existing DAG
run from the pre-dbt hold graph, keep that same Airflow run. Quarantine its
partial publication-hold checkpoint, reload the updated images/DAG, then clear
only `evaluate` with the current graph's downstream option enabled. Do not
clear `crawl`, `publish_handoff`, `select`, `extract`, `job_facts`, or
`classify`. The updated `evaluate` and `stage` tasks reuse the completed
selection and later artifacts without requiring that run's crawl artifact.

```bash
export WREMOTELY_BASE_RUN_ID="<logical-date-as-YYYYMMDDTHHMMSSZ>-wremotely"
export PUBLICATION_HOLD_RUN_ID="${WREMOTELY_BASE_RUN_ID}-publication-hold"
export PUBLICATION_HOLD_WORK_DIR="$WREMOTELY_ETL_ARTIFACTS_DIR/$PUBLICATION_HOLD_RUN_ID/.publication-hold-work"
export PUBLICATION_HOLD_WORK_BACKUP="${PUBLICATION_HOLD_WORK_DIR}.pre-post-dbt-v5"

test -d "$PUBLICATION_HOLD_WORK_DIR"
test ! -e "$PUBLICATION_HOLD_WORK_BACKUP"
sudo mv -- "$PUBLICATION_HOLD_WORK_DIR" "$PUBLICATION_HOLD_WORK_BACKUP"
```

Keep the backup until `publish_serving_snapshot` succeeds. In the Airflow UI,
open the same DAG run, select `evaluate`, choose **Clear**, enable
**Downstream**, and leave **Upstream** disabled. Confirm that the resulting
selection contains only `evaluate`, `stage`, `upload`, `load`, `dbt_build`,
`publication_hold`, and `publish_serving_snapshot` before applying the clear.

`publish_handoff` replaces
`<project>.<WREMOTELY_HANDOFF_DATASET>.wremotely__source_crawl_job_urls_current`
after `crawl` completes and publishes the canonical crawl artifact. `select`
reads that current handoff table, not the crawl run's local artifact path. In
dev, if the handoff table already exists and you intentionally want to skip a
slow crawl for a manual smoke run, mark both `crawl` and `publish_handoff`
successful before clearing/running `select`.

Normal `select` reads durable known-URL history from the raw BigQuery dataset
only; it does not scan old local extraction directories. Local extraction
history is available only through the private runtime's explicit
`--skip-known-url-lookup` development/bootstrap mode. This prevents old local
permissions or never-loaded partial runs from affecting a normal Airflow run.
Known-URL history is outcome-aware: successful extraction and terminal failures
are suppressed, while network, HTTP 408/425/429/5xx, run-local circuit-breaker,
and unavailable-robots failures remain eligible for a later DAG run. Presence in
the selected-URL table alone does not suppress a URL.

## Configuration notes

- `WREMOTELY_SOURCE_CRAWL_WORKER_COUNT` controls internal source-crawl
  concurrency inside the single `crawl` task.
- `WREMOTELY_EXTRACT_WORKER_COUNT` controls cross-domain extraction
  concurrency.
- `WREMOTELY_PLATFORM_WORKER_COUNT` limits concurrent tenants within one
  recognized platform for both crawl and extraction. The default is `2`, while
  each tenant or ordinary source domain remains serialized.
- `WREMOTELY_RECHECK_WORKER_COUNT` controls total internal lifecycle
  concurrency and must be between `1` and `32`. The default is `16`; source
  tenant/domain serialization still applies.
- Scheduled lifecycle runs always use seven buckets, minimum age zero, and a
  complete-bucket limit of zero. A manual dev trigger may set the DAG parameter
  `recheck_limit` to `1..1000` for a bounded orchestration smoke; production
  scheduled runs leave it at `0`.
- `WREMOTELY_PUBLICATION_TOPIC` selects the private environment-specific topic.
  The publisher service account receives `roles/pubsub.publisher` on this topic
  only; it does not need project-wide Pub/Sub administration or subscriber
  permissions.
- `WREMOTELY_DOCKER_NETWORK_MODE=host` lets a container reach a local
  host-bound inference endpoint on Linux. Use another Docker network mode only
  if the configured endpoint is reachable from child containers.
- `WREMOTELY_LOCAL_LLM_*` configures the inference runtime used only by the
  pre-publication hold step. `groq` requires `GROQ_API_KEY`; the DAG passes that
  key through Airflow's UI-hidden private environment only to
  `publication_hold`, not to dbt, snapshot publication, or signal tasks. Keep
  runtime settings and the key in the external environment file.

### Groq publication-hold setup

Create a dedicated project and one key per environment in the official
[Groq API Keys console](https://console.groq.com/keys). Groq keys do not have
configurable scopes, so separate projects and keys are the available
least-privilege boundary. Before enabling an environment, review Groq's current
[data handling documentation](https://console.groq.com/docs/your-data) and
enable Zero Data Retention in Data Controls.

Store the QA and prod keys only in these external files, each with mode `600`:

```text
/home/kevinesg/secrets/data-platform/qa/.env
/home/kevinesg/secrets/data-platform/prod/.env
```

Configure each file with its own key:

```dotenv
GROQ_API_KEY=<environment-specific-secret>
WREMOTELY_LOCAL_LLM_RUNTIME=groq
WREMOTELY_LOCAL_LLM_MODEL=meta-llama/llama-4-scout-17b-16e-instruct
WREMOTELY_LOCAL_LLM_ENDPOINT=https://api.groq.com/openai/v1
WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS=60
```

Follow the private runtime README's Groq model-access verification before
deployment. Then validate the platform environment:

```bash
python validate_config.py --env-file "$DATA_PLATFORM_ENV_FILE"
```

Validation fails when the key is missing or the Groq endpoint is not the
official HTTPS endpoint. Check the current
[rate limits](https://console.groq.com/docs/rate-limits) before a full run.

Rotate a key by creating and verifying its replacement, updating only the
matching environment file, redeploying that environment, and then deleting the
old key in the Groq console. Revoke a possibly exposed key immediately and
follow Groq's current
[security guidance](https://console.groq.com/docs/production-readiness/security-onboarding).

## Publication signal recovery and revocation

The Pub/Sub publish call returns a server-assigned message ID before the Airflow
task succeeds. Inspect the `signal_publication` log for `publication_id`,
`pubsub_topic`, and `pubsub_message_id`. A retry can produce another message ID
for the same publication ID and is safe by contract.

Verify the control row independently when diagnosing a signal failure:

```bash
bq query \
  --project_id="$PROJECT_ID" \
  --location="$BIGQUERY_LOCATION" \
  --use_legacy_sql=false \
  --parameter="publication_id:STRING:<publication-id>" \
  "SELECT publication_id, publication_state, published_at
   FROM \`$PROJECT_ID.$WREMOTELY_HANDOFF_DATASET.wremotely__serving_publication\`
   WHERE publication_id = @publication_id"
```

To revoke the pipeline publisher without deleting the topic or existing
publications:

```bash
gcloud pubsub topics remove-iam-policy-binding "$WREMOTELY_PUBLICATION_TOPIC" \
  --project="$PROJECT_ID" \
  --member="serviceAccount:$WREMOTELY_ETL_SERVICE_ACCOUNT_EMAIL" \
  --role="roles/pubsub.publisher"
```

Restore publishing by rerunning the topic-level
`add-iam-policy-binding` command in the setup section, then clear only the
failed `signal_publication` task. Credential key rotation continues to use the
external ETL credential path; recreate Airflow containers after replacing that
file so subsequent Docker tasks use the intended key.

## Recover a timed-out crawl container

The current DAG force-removes a Docker task container when Airflow times it out.
An older parsed DAG using `auto_remove="success"` can leave the container
running after the task has failed. Do not clear or retry that task until the old
container is gone; overlapping crawl containers can write the same checkpoint.

Load the canonical Airflow environment, list only containers created from the
configured private runtime image, inspect the selected container, then stop and
remove it:

```bash
set -euo pipefail

export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$HOME/dev/secrets/data-platform/.env}"
test -s "$DATA_PLATFORM_ENV_FILE"
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a

docker ps -a \
  --filter "ancestor=$DATA_PLATFORM_WREMOTELY_ETL_IMAGE" \
  --format '{{.ID}}\t{{.Names}}\t{{.Status}}\t{{.CreatedAt}}'

export STALE_WREMOTELY_ETL_CONTAINER_ID="<container-id>"
docker inspect \
  --format '{{.Id}} {{.Config.Image}} {{.State.Status}}' \
  "$STALE_WREMOTELY_ETL_CONTAINER_ID"
docker stop --time 30 "$STALE_WREMOTELY_ETL_CONTAINER_ID"
docker rm "$STALE_WREMOTELY_ETL_CONTAINER_ID"

docker ps -a \
  --filter "ancestor=$DATA_PLATFORM_WREMOTELY_ETL_IMAGE" \
  --format '{{.ID}}\t{{.Names}}\t{{.Status}}\t{{.CreatedAt}}'
```

Stopping the container can replay only its currently uncommitted source row;
completed source rows remain in the durable checkpoint. If the private runtime
crawler version changed, do not clear the old task against the rebuilt image:
finish with the old container or trigger a new DAG run with a new run ID.

For local dev, set the global crawl and per-platform worker counts in the
external Airflow environment file before reloading Airflow:

```bash
export WREMOTELY_SOURCE_CRAWL_WORKER_COUNT="${WREMOTELY_SOURCE_CRAWL_WORKER_COUNT:-6}"
export WREMOTELY_PLATFORM_WORKER_COUNT="${WREMOTELY_PLATFORM_WORKER_COUNT:-2}"

python -c 'import os
from pathlib import Path

env_file = Path(os.environ["DATA_PLATFORM_ENV_FILE"])
updates = {
    "WREMOTELY_SOURCE_CRAWL_WORKER_COUNT": os.environ["WREMOTELY_SOURCE_CRAWL_WORKER_COUNT"],
    "WREMOTELY_PLATFORM_WORKER_COUNT": os.environ["WREMOTELY_PLATFORM_WORKER_COUNT"],
}
lines = env_file.read_text().splitlines()
for name, value in updates.items():
    for index, line in enumerate(lines):
        if line.startswith(f"{name}="):
            lines[index] = f"{name}={value}"
            break
    else:
        lines.append(f"{name}={value}")
env_file.write_text("\n".join(lines) + "\n")
'
```

## Validation

Validate the Airflow external environment file before starting or redeploying
Airflow:

```bash
cd airflow
python validate_config.py --env-file "$DATA_PLATFORM_ENV_FILE"
```

After Airflow starts, verify DAG parsing:

```bash
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml exec scheduler airflow dags list-import-errors
```

For QA and prod, use the deployed Compose file set instead of
`docker-compose.dev.yml`.

## Local image and Airflow reload

After changing the private runtime code, rebuild the local private runtime image
used by `DATA_PLATFORM_WREMOTELY_ETL_IMAGE`:

```bash
cd /var/home/kevinesg/dev/github/wremotely/etl

docker build --pull=false --tag wremotely-etl:dev .

docker run --rm wremotely-etl:dev --help
docker run --rm --entrypoint sh wremotely-etl:dev \
  -c 'test "$(id -u)" = "10001"'
```

After changing wremotely dbt models, rebuild the local dbt image used by
`DATA_PLATFORM_DBT_IMAGE` before clearing `dbt_build`:

```bash
cd /var/home/kevinesg/dev/github/data-platform

docker build --pull=false --tag data-platform-dbt:dev dbt
```

The incremental models inspect existing target columns before applying a
watermark filter. When an older target lacks the required source/dbt watermark
columns, the next ordinary build processes all candidates once, appends the
columns, and backfills their values. Do not retry a downstream publication task
against an older dbt image: rebuild the image and clear `dbt_build` plus its
downstream tasks.

If `publication_hold` already completed in that DAG run before the dbt image
was corrected, preserve its replay artifact before clearing the downstream
tasks. The corrected candidate hash may not match the completed artifact:

```bash
export WREMOTELY_REPAIR_BASE_RUN_ID="<logical-date-as-YYYYMMDDTHHMMSSZ>-wremotely-repair"
export COMPLETED_REPAIR_HOLD_DIR="$WREMOTELY_ETL_ARTIFACTS_DIR/$WREMOTELY_REPAIR_BASE_RUN_ID-publication-hold/publication_hold"
export COMPLETED_REPAIR_HOLD_BACKUP="${COMPLETED_REPAIR_HOLD_DIR}.pre-dbt-watermark-migration"

test -d "$COMPLETED_REPAIR_HOLD_DIR"
test ! -e "$COMPLETED_REPAIR_HOLD_BACKUP"
sudo mv -- "$COMPLETED_REPAIR_HOLD_DIR" "$COMPLETED_REPAIR_HOLD_BACKUP"
```

For a pre-split producer run, manually trigger `publish__wremotely_serving` with
the same base run ID so it reuses those artifacts:

```json
{"publication_run_id": "<logical-date-as-YYYYMMDDTHHMMSSZ>-wremotely-repair"}
```

For a post-split run, clear `trigger_publication` in the producer or clear the
failed tasks directly in its linked publication DAG run.

Do not clear lifecycle tasks in an old `etl__wremotely` run after deploying this
split. Trigger `maintenance__wremotely_lifecycle` instead; its run IDs and
artifacts are independent from ingestion. A successful lifecycle run continues
through `trigger_publication`; the linked `publish__wremotely_serving` run must
then succeed through `signal_publication` so deletion-state changes reach the
serving database. Clearing a producer trigger resets and replays its same
deterministic publication DAG run instead of creating an overlapping run.

For one intentional dev integration check, trigger the lifecycle DAG with
`recheck_limit=12`. This bounds only that manual run. Scheduled production runs
retain the default `0` and process every row in their selected bucket.

To validate exact repair orchestration, first unpause
`publish__wremotely_serving`, then trigger `repair__wremotely_job_urls` in
the Airflow UI and enter one exact URL per line in **Job URLs to reprocess**.
Start with one known current-handoff URL. The DAG intentionally has no schedule,
does not crawl the registry, and fails in `select` if any requested identity is
missing. A successful producer run must reach `trigger_publication`, and its
linked publication run must continue through `signal_publication`. Do not
manually patch BigQuery or PostgreSQL for the repaired row.

After changing Airflow DAG code or the external Airflow environment file,
validate the environment file and recreate the local Airflow containers so they
read the updated values and DAG source:

```bash
cd /var/home/kevinesg/dev/github/data-platform/airflow

python validate_config.py --env-file "$DATA_PLATFORM_ENV_FILE"

docker compose --env-file "$DATA_PLATFORM_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.dev.yml \
  up -d --force-recreate --remove-orphans

docker compose --env-file "$DATA_PLATFORM_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.dev.yml \
  ps

docker compose --env-file "$DATA_PLATFORM_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.dev.yml \
  exec scheduler airflow dags list-import-errors

docker compose --env-file "$DATA_PLATFORM_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.dev.yml \
  exec scheduler airflow pools list

docker compose --env-file "$DATA_PLATFORM_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.dev.yml \
  exec scheduler airflow dags list
```

Verify that `wremotely_network` and `wremotely_warehouse` each have one slot,
and that all six wremotely DAG IDs are listed. New DAGs are paused on creation;
unpause only the DAGs needed for each dev smoke. Keep artifact cleanup paused
until its dry-run report has been reviewed. In the Airflow UI:

1. Trigger `repair__wremotely_job_urls` with one known URL from the current
   source-crawl handoff table. Confirm the producer reaches
   `trigger_publication` and its linked publication DAG succeeds through
   `signal_publication`.
2. Trigger `maintenance__wremotely_lifecycle` with `recheck_limit=12`. Confirm
   the prepared metadata records seven buckets, the logical-date-selected bucket
   index, and no more than 12 rows. Confirm the linked publication DAG succeeds
   through `signal_publication` even when no row becomes deleted.
3. Review a dry-run cleanup report, then trigger
   `maintenance__wremotely_artifacts`. Confirm only verified-safe artifacts
   older than three days were deleted and all evidence-required/recent runs were
   retained.
4. Confirm `etl__wremotely` no longer contains the five lifecycle tasks.
