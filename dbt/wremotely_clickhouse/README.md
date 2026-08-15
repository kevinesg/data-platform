# wremotely ClickHouse dbt harness

This directory is an isolated compatibility harness for the wremotely
ClickHouse migration. It is intentionally separate from the shared dbt
runtime in `dbt/`, which continues to serve the existing BigQuery
`personal_finance` and Wremotely projects.

The harness uses the dbt-clickhouse-supported dbt Core 1.10 line. Do not
downgrade the shared dbt 1.12 runtime to make this adapter fit. The harness is
not yet an Airflow task or a production cutover path.

## Local setup

Start the development ClickHouse instance and confirm the stable raw relation
has been loaded by the private ETL loader. Then run:

```bash
cd dbt/wremotely_clickhouse
cp profiles.yml.example profiles.yml
uv sync --locked

export WREMOTELY_CLICKHOUSE_DATABASE=wremotely_dev
export WREMOTELY_CLICKHOUSE_HOST=127.0.0.1
export WREMOTELY_CLICKHOUSE_PORT=8123
export WREMOTELY_CLICKHOUSE_USER=wremotely_dev
# Set WREMOTELY_CLICKHOUSE_PASSWORD only when the local user has a password.

uv run dbt debug --profiles-dir .
uv run dbt build --profiles-dir . --select stg_wremotely__job_facts
```

`profiles.yml` is local-only and must not be committed. The model reads the
stable `wremotely_dev.wremotely__job_facts` raw relation and materializes a
typed staging table in the same ClickHouse database.

## Scope and next boundary

This slice proves the separate runtime, source contract, JSON payload
projection, candidate-title projection, and dbt tests against real raw
relations. It deliberately defers the lifecycle-recheck staging model until a
representative raw lifecycle relation is loaded, as well as the remaining
intermediate/mart models, ClickHouse grants, Airflow orchestration, and
production cutover until each compatibility step is validated with
representative data. The latest-per-candidate and candidate-title projections
use replay-safe full-table materialization; incremental merge behavior is
deferred until the full graph and source-history semantics are validated.
