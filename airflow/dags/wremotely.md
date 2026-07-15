# wremotely Airflow DAGs

`etl__wremotely` ingests newly crawled jobs,
`maintenance__wremotely_lifecycle` rechecks one stable active-job bucket, and
`repair__wremotely_job_urls` performs bounded exact-URL repairs. Each producer
loads raw data and triggers `publish__wremotely_serving`, which serializes the
tested dbt build, publication hold, current serving snapshot, and publication
signal. Airflow owns dependency order, retries, and timeouts only; the private
runtime owns extract/load behavior and dbt owns transformation and blocking
tests.

## Runtime inputs

All paths below must be outside this repository and readable by the host user
running Docker:

- `WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS`: service-account JSON for the
  private extract/load runtime.
- `DBT_GOOGLE_APPLICATION_CREDENTIALS`: service-account JSON for dbt.
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
- `WREMOTELY_APPROVED_SOURCES_FILE`: approved source snapshot JSONL file.
- `WREMOTELY_APPROVED_SOURCES_SHA256`: SHA-256 checksum of the approved source
  snapshot. Scheduled runs should pin this so a source-file edit cannot
  silently change a run.
- `WREMOTELY_PUBLICATION_HOLD_POLICY`: private policy file for the
  pre-publication hold step. Keep the file outside Git.
- `WREMOTELY_LIFECYCLE_SCHEDULE`: required only when `ENVIRONMENT=prod`;
  configure `15 */12 * * *` so seven half-day runs cover the active catalog in
  3.5 days. Keep dev/QA lifecycle runs manual.

The private runtime image is configured with `DATA_PLATFORM_WREMOTELY_ETL_IMAGE`.
Dev may keep this value in its external development environment file. QA and
prod keep it in the external deployment `images.env` manifest using the exact
immutable form
`ghcr.io/kevinesg/wremotely-etl:sha-<full-40-character-commit-sha>`.
`deploy-qa` validates the explicit QA image variable, while `deploy-prod`
promotes the same manifest entry. The private GHCR package must grant
`kevinesg/data-platform` read access under **Manage Actions access** so the
deployment workflows can verify and pull it without making the package public.

## Handoff dataset and publication topic

The wremotely DAG uses a private BigQuery handoff dataset for current-state
handoff and serving publication tables. This dataset is not raw
warehouse history or a dbt target. It is the private exchange boundary between
pipeline steps and the future serving publication worker; no table is public.

Use a descriptive dataset name such as `handoff_<developer>` for local dev and
`handoff` for QA and prod. The environment-specific GCP projects already separate
QA and prod, so deployed dataset names do not need a developer suffix. Avoid
`temp` because these tables are replaceable but still persistent handoff state;
the name can be confused with BigQuery temporary tables or data that may be
deleted at any moment. Avoid the shorthand `ops` in resource names unless a
broader operations dataset is created later with a documented ownership
contract.

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
  for newly ingested jobs or lifecycle changes;
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

The approved source registry remains file-based and reviewed in the private
source repository. Do not upload the approved registry to BigQuery as the source
of truth. The runtime should continue to receive the reviewed snapshot through
`WREMOTELY_APPROVED_SOURCES_FILE` and `WREMOTELY_APPROVED_SOURCES_SHA256`.

Create or verify the runtime's BigQuery access and handoff dataset as a platform
maintainer before enabling a DAG or private runtime command that references
them. The ETL service account needs project-level job creation, dataset-level
read/write access on the raw dataset, and dataset-level read/write access on the
handoff dataset. The same account reads raw history during `select`, writes raw
tables during `load`, and reads tested dbt candidate relations after
`dbt_build`. `roles/bigquery.dataEditor` is intentional on raw and handoff;
`roles/bigquery.dataViewer` is sufficient on the dbt dataset. The setup uses
`gcloud` and `bq`, installed with Google Cloud CLI.

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

bq show \
  --project_id="$PROJECT_ID" \
  "$PROJECT_ID:$WREMOTELY_DBT_MART_DATASET"

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
crawl
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
```

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

The normal scheduled path does not acquire new sources. It starts from the
approved source snapshot, selects unseen job URLs, loads raw BigQuery tables,
and only then builds the dbt serving snapshot.
The intended production ingestion cadence is every 12 hours (`0 */12 * * *`).
Lifecycle runs every 12 hours at minute 15 (`15 */12 * * *`) with seven stable
buckets and 16 internal workers. Each scheduled run owns one complete bucket,
so seven successful runs cover the active catalog in 3.5 days. Bucket size grows
with the active catalog; monitor actual run duration, network-pool queue delay,
retries, and completion before the next lifecycle interval. Dev and QA remain
manually triggered; repair and publication are unscheduled in every environment.

These DAGs include every implemented step required to create and refresh the
BigQuery serving publication. They intentionally do not run search-provider
`discover`, offline crawl merging, classifier benchmarks, or destructive local
artifact cleanup. Those are source-acquisition, evaluation, or maintenance
workflows with different budgets and cadences. The DAG publishes the exact
committed `READY` publication ID to Pub/Sub. The VPS publication worker consumes
that signal and applies the bounded serving snapshot to PostgreSQL.

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

`publication_hold` reads `serving_jobs` from the generated wremotely mart
dataset (`${DBT_DATASET}_mart_wremotely` in dev and `mart_wremotely` in
QA/prod).
Before invoking the local model, it keeps only candidates
whose trimmed, uppercased structured title contains `DATA ENGINEER`,
`ANALYTICS ENGINEER`, `SQL DEVELOPER`, or `ETL ENGINEER`. Its checkpoint is
bound to the complete candidate-set hash, policy hash, and title-filter contract.
Across completed runs it reuses a current hold decision only when the candidate
content hash, policy, evaluator, prompt, runtime, and model identities all
match; changed candidates are evaluated again.

The lifecycle DAG reads the current serving handoff plus raw selected-job
metadata and lifecycle history. The private selector removes `is_deleted=true`
rows before assigning active `job_id` values to seven stable hash buckets.
Airflow derives one bucket index from the 12-hour logical date and processes the
complete bucket with no scheduled row cap. Seven successful runs therefore
cover every currently active serving job over 3.5 days without one growing
all-at-once fetch burst. It completes successfully with an empty bucket.
Explicit closed-page evidence sets `is_deleted`; terminal HTTP outcomes require
two consecutive rechecks. The workflow loads lifecycle events before triggering
the serialized publication DAG, and dbt retains rows with `is_deleted = true`
and advances `_updated_at` instead of removing them.
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
The task also passes the mounted approved-source snapshot and its configured
SHA-256 checksum to the private runtime. The runtime counts distinct enabled,
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

The topic has no subscription in this PR. The VPS worker PR creates one
environment-specific pull subscription with its own least-privilege subscriber
identity. Pub/Sub does not retain topic messages for a future subscription, so
after that subscription is created, clear `signal_publication` once to send the
latest ready publication ID.

## Successful task clear and replay behavior

Idempotency is defined against the same declared run ID, configuration, and
input artifacts. It does not mean an old DAG run can safely replace newer
current-state handoff tables, nor does it provide disaster recovery after
someone manually deletes verified external data.

- `crawl`, `select`, `extract`, `job_facts`, `classify`, `evaluate`,
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
  run-pinned historical reconstruction.
- `publish_serving_snapshot` is content-addressed. Replaying the same completed
  run verifies the pinned approved-source checksum and source-coverage aggregate
  before returning its recorded publication ID. Recreating the same serving and
  source-coverage snapshot through a new run resolves to the same immutable
  publication ID.
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
- `WREMOTELY_LOCAL_LLM_*` configures the local inference endpoint used by the
  pre-publication hold step. Keep the selected model/runtime value in the
  external environment file, not in this repository.

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
and that all four wremotely DAG IDs are listed. New DAGs are paused on creation;
unpause the repair, lifecycle, and publication DAGs for the dev smoke. In the
Airflow UI:

1. Trigger `repair__wremotely_job_urls` with one known URL from the current
   source-crawl handoff table. Confirm the producer reaches
   `trigger_publication` and its linked publication DAG succeeds through
   `signal_publication`.
2. Trigger `maintenance__wremotely_lifecycle` with `recheck_limit=12`. Confirm
   the prepared metadata records seven buckets, the logical-date-selected bucket
   index, and no more than 12 rows. Confirm the linked publication DAG succeeds
   through `signal_publication` even when no row becomes deleted.
3. Confirm `etl__wremotely` no longer contains the five lifecycle tasks.
