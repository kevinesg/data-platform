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
projection, latest-per-candidate source projections, candidate-title
projection, prepared country-eligibility inputs, exact country-eligibility
matches, candidate country-eligibility rollup, and serving-country bridge, and
dbt tests against real raw relations.
The prepared country relation aligns evidence to the latest classification and
assigns deterministic direction and match-mode fields. Exact matching covers
normalized country, group, reviewed location, and subdivision aliases, and
marks evidence that resolves to multiple countries as ambiguous rather than
selecting one silently. Phrase substring matching and country bridge expansion
are represented in the candidate rollup and serving-country bridge. Lifecycle
recheck inputs remain deferred until a representative capture includes a
stable lifecycle raw relation. The remaining candidate, publication,
intermediate, and mart models, ClickHouse grants, Airflow orchestration, and
production cutover remain deferred until each compatibility step is validated
with representative data. The latest-per-candidate and prepared evidence
projections use replay-safe full-table materialization; incremental merge
behavior is deferred until the full graph and source-history semantics are
validated.
