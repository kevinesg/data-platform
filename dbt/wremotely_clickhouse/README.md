# wremotely ClickHouse dbt harness

This directory is an isolated compatibility harness for the wremotely
ClickHouse migration. It is intentionally separate from the shared dbt
runtime in `dbt/`, which continues to serve the existing BigQuery
`personal_finance` and Wremotely projects.

The harness uses the dbt-clickhouse-supported dbt Core 1.10 line. Do not
downgrade the shared dbt 1.12 runtime to make this adapter fit. The harness is
used by the manual on-prem Airflow DAG; it is not a production cutover path by
itself. Its container image is published separately from the shared BigQuery
dbt image so the two adapter/runtime contracts can evolve independently.

## Container image

Build the isolated runtime from this directory:

```bash
docker build \
  --file Dockerfile \
  --tag data-platform-wremotely-clickhouse-dbt:dev \
  .

docker run --rm data-platform-wremotely-clickhouse-dbt:dev --version
```

The CI workflow builds the same Dockerfile without pushing it. Successful dbt
CI on `main` publishes the immutable
`ghcr.io/kevinesg/data-platform-wremotely-clickhouse-dbt:sha-<commit-sha>` tag.
The image currently contains only the isolated dbt project and profile
template; credentials and the real profile must be supplied at runtime.

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
uv run dbt build --profiles-dir .
```

`profiles.yml` is local-only and must not be committed. The model reads the
stable `wremotely_dev.wremotely__*` raw relations and materializes the full
typed staging, intermediate, and mart graph in the same ClickHouse database.
For a replay check, run the same build a second time and compare the
`wremotely__publication_manifest` row and the serving/company row-hash
fingerprints; identical declared input should produce identical fingerprints.

## Scope and next boundary

This slice proves the separate runtime, source contract, JSON payload
projection, latest-per-candidate source projections, candidate-title
projection, prepared country-eligibility inputs, exact country-eligibility
matches, candidate country-eligibility rollup, lifecycle latest-observation
semantics, current candidate facts, publication eligibility, publishable job
facts, search facets, serving jobs, companies, the serving-country bridge, and
a deterministic publication manifest. The model and seed tests run against
real raw relations.
The prepared country relation aligns evidence to the latest classification and
assigns deterministic direction and match-mode fields. Exact matching covers
normalized country, group, reviewed location, and subdivision aliases, and
marks evidence that resolves to multiple countries as ambiguous rather than
selecting one silently. Phrase substring matching and country bridge expansion
are represented in the candidate rollup and serving-country bridge. Lifecycle
closure requires one latest `CLOSED` observation or two ordered `TERMINAL`
observations; missing lifecycle history remains open and is never treated as
closure. The on-prem Airflow DAG runs this graph after loading local raw
relations, then exports a READY ClickHouse publication artifact and signals its
content-addressed publication ID over the existing Pub/Sub topic. ClickHouse
grants, the private VPS snapshot-read route, worker cutover, and production
authority remain separate operational work. The five latest-per-candidate
source projections use replay-safe ClickHouse `delete_insert` incremental
materialization. Each run identifies candidates whose source watermark
advanced (plus candidates not yet present in the target), ranks only those
candidates against immutable source history, and replaces their target rows
by `candidate_id`. The classification, extraction, job-facts, and selected-URL
projections rank a narrow key relation first, then join the winning
`ingest_key` back to the full source row; large payload columns therefore do
not participate in the window sort. The profile enables the adapter's
lightweight-delete support; model query settings spill large sort/group
operations and cap per-query memory. Use `--full-refresh` when a source
contract or model schema changes.

## Contract parity

The ClickHouse graph is intentionally not required to have the same dbt
resource count as the BigQuery graph. The country-evidence implementation is
split into prepared-input and exact-match relations, while adapter-specific
SQL is kept out of the shared project. Parity means preserving the same
publication, lifecycle, country-eligibility, company, taxonomy, and identity
guarantees—not duplicating BigQuery syntax or forcing identical model names.

The ClickHouse project includes the complete country taxonomy seeds, expanded
column-level contracts for the latest/source and serving models, and
adapter-compatible invariant tests for URL-to-facts alignment, source-backed
company identity, timestamp consistency, bounded titles, lifecycle closure,
publication readiness and status, country bridges, taxonomies, publication
manifest integrity, and the country-evidence boundary. The evidence checks
also verify latest-classification alignment, global-scope preservation,
reviewed platform-location consumption, restrictive evidence values, atomic
match unambiguity, and taxonomy alias hygiene. BigQuery-specific dbt unit tests
remain in the shared BigQuery project until an equivalent ClickHouse unit-test
implementation is available and validated.
