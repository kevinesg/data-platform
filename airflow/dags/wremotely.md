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
- `WREMOTELY_APPROVED_SOURCES_FILE`: approved source snapshot JSONL file.
- `WREMOTELY_APPROVED_SOURCES_SHA256`: SHA-256 checksum of the approved source
  snapshot. Scheduled runs should pin this so a source-file edit cannot
  silently change a run.
- `WREMOTELY_PUBLICATION_HOLD_POLICY`: private policy file for the
  pre-publication hold step. Keep the file outside Git.

The private runtime image is configured with `DATA_PLATFORM_WREMOTELY_ETL_IMAGE`.
Use an immutable image tag in QA and prod.

## Task order

The DAG runs the core publication path in this order:

```text
crawl
  -> select
  -> extract
  -> job_facts
  -> classify
  -> publication_hold
  -> evaluate
  -> stage
  -> upload
  -> load
  -> dbt_build
```

The normal scheduled path does not acquire new sources. It starts from the
approved source snapshot, selects unseen job URLs, loads raw BigQuery tables,
and only then builds the dbt serving snapshot.

## Configuration notes

- `WREMOTELY_SOURCE_CRAWL_SHARD_COUNT` and
  `WREMOTELY_SOURCE_CRAWL_SHARD_INDEX` can partition the approved source
  snapshot across multiple scheduled DAG configurations later. The default
  `1`/`0` crawls every approved source in one run.
- `WREMOTELY_EXTRACT_WORKER_COUNT` controls cross-domain extraction
  concurrency. The private runtime serializes requests per source domain.
- `WREMOTELY_DOCKER_NETWORK_MODE=host` lets a container reach a local
  host-bound inference endpoint on Linux. Use another Docker network mode only
  if the configured endpoint is reachable from child containers.
- `WREMOTELY_LOCAL_LLM_*` configures the local inference endpoint used by the
  pre-publication hold step. Keep the selected model/runtime value in the
  external environment file, not in this repository.

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
