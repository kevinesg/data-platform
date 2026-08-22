# wremotely Airflow runbook

The scheduled production DAG is `etl__wremotely`. A separate
`crawl__wremotely_onprem` DAG prepares six deterministic source shards every
two hours, runs those shards concurrently in the dedicated `wremotely_crawl`
pool, merges them atomically, and publishes the latest complete crawl
generation. `etl__wremotely` pins that generation before local filesystem
landing and ClickHouse raw loading, then
hands the run to a serialized publication DAG for the isolated ClickHouse dbt
project, immutable publication snapshot, and final Pub/Sub signal containing
only the publication identifier.

BigQuery and GCS are no longer part of the scheduled wremotely path. The
personal-finance DAG remains a separate BigQuery workload. The former GCP
ingestion, publication, and Wremotely repair DAGs are removed from the packaged
runtime. Historical recovery uses the private ETL's repository-owned replay
commands against retained local artifacts; it does not restore a cloud Airflow
path.

`maintenance__wremotely_lifecycle` is the native maintenance path. It selects
due jobs from ClickHouse, rechecks their source pages, lands the lifecycle
facts under the local warehouse root, and loads ClickHouse raw relations before
handing the run to the serialized publication DAG. The publication DAG runs
the ClickHouse dbt project, publishes a local snapshot, and sends only the
content-addressed publication ID through Pub/Sub.

Both scheduled paths hand off after raw loading to the serialized
`publish__wremotely_serving` DAG. That DAG has one active-run boundary and runs
the ClickHouse dbt, snapshot, review-export, and Pub/Sub signal sequence.
Lifecycle selection requires the ClickHouse dbt assignment model to have
completed first. That model keeps append-only, source-stratified assignments
across seven logical buckets. It assigns known dates only after they are at least
21 days old and also assigns jobs with no posting date so they can eventually be
closed; the selector applies the known-date age gate at selection time. The
maintenance DAG rotates one logical bucket every
12-hour schedule slot; it does not require pre-created bucket directories.
There is no legacy publication mode. Do not add a second publication path for
the same publication ID.

## Required external configuration

Keep the environment file and credentials outside Git. The Airflow image and
the private ETL image must be immutable published images. The ClickHouse dbt
image must be published from `dbt/wremotely_clickhouse/` and recorded in the
environment image manifest as
`DATA_PLATFORM_WREMOTELY_CLICKHOUSE_DBT_IMAGE`.

Required values for the scheduled path include:

- `WREMOTELY_CRAWL_SCHEDULE` (production default: `0 */2 * * *`)
- `WREMOTELY_SOURCE_CRAWL_SHARD_COUNT` (production default: `6`)

The production crawl commands use `--source-crawl-max-job-urls 0` (no
artificial per-run URL cap), `--source-crawl-max-pages-per-source 15`, and
`--source-crawl-max-links-per-page 1000`. The private ETL applies narrower
reviewed limits for source families that do not need all 15 pages, while
preserving explicit smaller smoke-test limits.

- `DATA_PLATFORM_WREMOTELY_CLICKHOUSE_DBT_IMAGE`
- `DATA_PLATFORM_WREMOTELY_ETL_IMAGE`
- `WREMOTELY_ETL_ARTIFACTS_DIR`
- `WREMOTELY_WAREHOUSE_ROOT`
- `WREMOTELY_CLICKHOUSE_URL`, `WREMOTELY_CLICKHOUSE_DATABASE`,
  `WREMOTELY_CLICKHOUSE_HOST`, `WREMOTELY_CLICKHOUSE_PORT`, and
  `WREMOTELY_CLICKHOUSE_USER`
- `WREMOTELY_PUBLICATION_TOPIC`
- `WREMOTELY_PUBLICATION_STATUS_SUBSCRIPTION`, a dedicated pull subscription
  for worker post-commit receipts
- `WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS` for Pub/Sub only
- `PROJECT_ID` for Pub/Sub only

`WREMOTELY_CLICKHOUSE_PASSWORD` is passed as a private DockerOperator
environment value and must never appear in a visible task environment, Git,
or an image layer. The ClickHouse server should bind to loopback or a private
network; it must not be exposed through Airflow's public port.

The warehouse root contains the local storage and control contracts:

```text
$WREMOTELY_WAREHOUSE_ROOT/
  storage/
    raw/
    curated/
  control/
    source-crawl/latest.json
    landing/<landing-run-id>/_SUCCESS
    clickhouse-publication/<snapshot-run-id>/manifest.json
```

The ETL artifact root is separate from the warehouse root and is mounted at
`/artifacts/wremotely-etl` inside task containers. It contains crawl, extract,
classification, and dbt target artifacts needed for replay and diagnosis.

## Run and validate

The crawl generation DAG is scheduled in production from
`WREMOTELY_CRAWL_SCHEDULE`; the ingestion DAG is scheduled from
`ETL__WREMOTELY_SCHEDULE`, the
lifecycle DAG from `WREMOTELY_LIFECYCLE_SCHEDULE`, and artifact cleanup from
`WREMOTELY_ARTIFACT_CLEANUP_SCHEDULE`; all three remain manual in dev and QA.
Before enabling them, validate the ClickHouse service and
the database/user/password contract using the commands in
[`../clickhouse/README.md`](../../clickhouse/README.md), then validate Airflow:

```bash
cd airflow
python validate_config.py --env-file "$DATA_PLATFORM_ENV_FILE"
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml config --quiet
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml \
  exec -T scheduler airflow dags list-import-errors
```

The expected result for import errors is `No data found`. The packaged
validator also checks that the canonical task commands contain no GCP or GCS
arguments; the only task allowed to use a GCP project is the final Pub/Sub
signal.

For a bounded manual smoke, use Airflow's UI or CLI to trigger
`etl__wremotely` in dev/QA after the ClickHouse service is healthy. Do not run
the legacy GCP DAGs to validate the new path. The first normal ingestion run
requires one successful `crawl__wremotely_onprem` generation so that
`control/source-crawl/latest.json` exists; inspect that pointer and its merged
run marker before enabling the normal ingestion schedule.

## Failure and replay boundaries

Every crawl shard and EL task derives its run identity from the Airflow logical
date. Retries and
task clears therefore reuse the same artifact identity. Clear only the failed
task and its downstream tasks after checking the corresponding `_SUCCESS` or
manifest marker; rerun from crawl when the source registry or crawl inputs
changed.

Normal ingestion pins the latest complete generation. An explicit full refresh
from the `crawl` boundary still runs the crawl task in the ingestion DAG and
does not advance the detached-generation pointer until the separate generation
publish step succeeds.

The ClickHouse dbt task writes a run-specific target directory. A failed dbt
build does not publish a snapshot, and the Pub/Sub signal cannot run unless the
snapshot manifest is READY and content-addressed. The publication worker on
the VPS consumes the identifier from Pub/Sub and reads the bounded ClickHouse
publication contract through its private serving boundary.

## Retention and lifecycle

The former GCS cleanup and BigQuery lifecycle paths are not part of normal
operation. The artifact cleanup DAG is filesystem-only, manual in dev/QA, and
deletes only exact eligible local run directories described by the ETL cleanup
manifest. Filesystem retention and ClickHouse lifecycle runs must preserve the
latest successful publication, its control manifest, and the crawl generation
referenced by `control/source-crawl/latest.json`.

## Monitoring

`monitor__wremotely` runs hourly in production and is manual in dev/QA. It
checks Airflow run freshness, ClickHouse reachability and latest READY
publication freshness, local publication-manifest agreement, and warehouse or
artifact filesystem headroom. Its final task consumes a dedicated Pub/Sub
status subscription and verifies that the latest ClickHouse READY publication
has a worker receipt written after the PostgreSQL transaction committed. The
checker keeps the latest accepted receipt in the mounted local control root so
a quiet monitor interval does not erase the last verified state. The
authenticated Airflow DAG run page is the operator-facing monitoring link.
