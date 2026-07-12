# wremotely Airflow DAG

`etl__wremotely` orchestrates the private wremotely extract/load runtime and
then builds the wremotely dbt serving graph. Airflow owns dependency order,
retries, and timeouts only; the private runtime owns extract/load behavior and
dbt owns transformation and blocking tests.

## Runtime inputs

All paths below must be outside this repository and readable by the host user
running Docker:

- `WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS`: service-account JSON for the
  private extract/load runtime.
- `DBT_GOOGLE_APPLICATION_CREDENTIALS`: service-account JSON for dbt.
- `WREMOTELY_ETL_ARTIFACTS_DIR`: durable local artifact directory mounted into
  private runtime containers. It must be writable by the private runtime
  container user.
- `WREMOTELY_HANDOFF_DATASET`: private BigQuery dataset for replaceable
  current-state handoff tables and immutable versioned serving snapshots, for
  example `handoff_<developer>` in dev and `handoff` in QA/prod.
- `WREMOTELY_APPROVED_SOURCES_FILE`: approved source snapshot JSONL file.
- `WREMOTELY_APPROVED_SOURCES_SHA256`: SHA-256 checksum of the approved source
  snapshot. Scheduled runs should pin this so a source-file edit cannot
  silently change a run.
- `WREMOTELY_PUBLICATION_HOLD_POLICY`: private policy file for the
  pre-publication hold step. Keep the file outside Git.

The private runtime image is configured with `DATA_PLATFORM_WREMOTELY_ETL_IMAGE`.
Use an immutable image tag in QA and prod.

## Handoff and publication dataset

The wremotely DAG uses a private BigQuery handoff dataset for current-state
handoff tables and versioned publication snapshots. This dataset is not raw
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
- publication hold replaces one current decision table only after its local
  checkpoint completes;
- final serving snapshot tables are immutable by publication ID so the serving
  worker can replay a specific ready version;
- durable raw tables that dbt reads remain separate and are updated only by the
  core load step.

This keeps long-running discovery and crawl work from blocking manual runs that
only need the latest already-published handoff table. It also keeps handoff
replacement single-writer: only `publish_handoff` replaces the handoff table
after `crawl` has published a complete canonical crawl artifact.

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
```

After creating or verifying the dataset, add or update
`WREMOTELY_HANDOFF_DATASET` in the external Airflow environment file. Use
`handoff_<developer>` in dev and `handoff` in QA/prod unless a
component-specific runbook documents a different name.

```bash
python -c 'import os
from pathlib import Path

env_file = Path(os.environ["DATA_PLATFORM_ENV_FILE"])
lines = env_file.read_text().splitlines()
required_values = {
    "WREMOTELY_HANDOFF_DATASET": os.environ["WREMOTELY_HANDOFF_DATASET"],
    "WREMOTELY_PLATFORM_WORKER_COUNT": "2",
    "WREMOTELY_RECHECK_LIMIT": "100",
    "WREMOTELY_RECHECK_MIN_AGE_HOURS": "72",
}
for name, value in required_values.items():
    updated = False
    for index, line in enumerate(lines):
        if line.startswith(f"{name}="):
            if name == "WREMOTELY_HANDOFF_DATASET":
                lines[index] = f"{name}={value}"
            updated = True
            break
    if not updated:
        lines.append(f"{name}={value}")
env_file.write_text("\n".join(lines) + "\n")
'
```

## Task order

The DAG loads new-job data, then selects lifecycle work from that stable raw
boundary before one dbt build:

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
  -> prepare_recheck
  -> recheck
  -> stage_recheck
  -> upload_recheck
  -> load_recheck
  -> dbt_build
  -> publication_hold
  -> publish_serving_snapshot
```

The normal scheduled path does not acquire new sources. It starts from the
approved source snapshot, selects unseen job URLs, loads raw BigQuery tables,
and only then builds the dbt serving snapshot.
The intended production cadence is every 12 hours (`0 */12 * * *`); dev and QA
remain manually triggered unless their environment policy explicitly differs.

This DAG includes every implemented step required to create and refresh the
BigQuery serving publication. It intentionally does not run search-provider
`discover`, offline crawl merging, classifier benchmarks, or destructive local
artifact cleanup. Those are source-acquisition, evaluation, or maintenance
workflows with different budgets and cadences. Pub/Sub signaling and the VPS
publication worker are not implemented in this DAG yet and remain the next
serving-delivery boundary after the BigQuery `READY` publication.

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

The lifecycle branch reads raw selected-job and prior lifecycle history, chooses
the oldest due active rows, and is bounded by `WREMOTELY_RECHECK_LIMIT`. It
completes successfully with an empty batch. Explicit closed-page evidence sets
`is_deleted`; terminal HTTP outcomes require two consecutive rechecks. The
branch loads lifecycle events before dbt, and dbt retains tombstone rows with
`is_deleted = true` and advances `_updated_at` instead of removing them.
`prepare_recheck` depends on the core `load` task so its BigQuery query sees a
deterministic current-run raw boundary rather than racing a concurrent load.

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

After the local artifact completes, the task atomically replaces
`wremotely__publication_holds_current` in the handoff dataset. Each decision is
bound to the dbt candidate row hash. `publish_serving_snapshot` anti-joins only
matching `held` and `review_hold` decisions, then transactionally appends
immutable versioned jobs, companies, and country-eligibility rows plus a
`READY` publication-control row. Candidates without a hold decision pass.
The handoff dataset tables are
`wremotely__serving_jobs_versions`,
`wremotely__serving_companies_versions`,
`wremotely__serving_job_country_eligibility_versions`, and
`wremotely__serving_publications`.

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
- `WREMOTELY_RECHECK_LIMIT` bounds lifecycle requests per DAG run and must be
  between `1` and `1000`. The default is `100`.
- `WREMOTELY_RECHECK_MIN_AGE_HOURS` controls when an active row becomes due
  after its latest selection or lifecycle check. The default is `72`; use `0`
  only for an intentional dev integration run.
- `WREMOTELY_DOCKER_NETWORK_MODE=host` lets a container reach a local
  host-bound inference endpoint on Linux. Use another Docker network mode only
  if the configured endpoint is reachable from child containers.
- `WREMOTELY_LOCAL_LLM_*` configures the local inference endpoint used by the
  pre-publication hold step. Keep the selected model/runtime value in the
  external environment file, not in this repository.

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

For an existing dev DAG run that already completed the core `load` task before
the lifecycle branch was added, do not clear crawl, select, extract, or the core
load chain. After rebuilding both images and recreating Airflow, run/clear only
`prepare_recheck` through `load_recheck`. Once `load_recheck` succeeds, clear
`dbt_build` with downstream tasks selected and upstream tasks unselected. This
rebuilds the mart with lifecycle tombstones, reruns publication hold against the
correct generated mart dataset, and publishes the serving snapshot without
repeating the expensive core EL work.

For one intentional dev integration check, set
`WREMOTELY_RECHECK_MIN_AGE_HOURS=0` before recreating Airflow so the selector
chooses up to `WREMOTELY_RECHECK_LIMIT` existing rows. Restore the normal age
after the branch has been proven.

```bash
python -c 'import os
from pathlib import Path

env_file = Path(os.environ["DATA_PLATFORM_ENV_FILE"])
updates = {
    "WREMOTELY_RECHECK_LIMIT": "100",
    "WREMOTELY_RECHECK_MIN_AGE_HOURS": "0",
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
```
