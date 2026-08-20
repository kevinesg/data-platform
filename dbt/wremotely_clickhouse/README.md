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

The Airflow publication task invokes the image's retained-result runner rather
than calling `dbt` directly. A successful build atomically replaces the
ClickHouse `run_results.json`; a failed build leaves the previous successful
result untouched and retains the temporary dbt target directory under the
artifacts mount. The image keeps only the five newest failed targets.

To retry only the failed nodes from a retained target, run the same image with
the wrapper entrypoint and the exact retained target path:

```bash
docker run --rm \
  --entrypoint python \
  -v /srv/data/services/etl-artifacts:/artifacts/wremotely-etl \
  ghcr.io/kevinesg/data-platform-wremotely-clickhouse-dbt:sha-<commit-sha> \
  /app/run_and_retain_results.py \
  --output /artifacts/wremotely-etl/baseline/clickhouse-dbt/run_results.json \
  --retry-target-path \
  /artifacts/wremotely-etl/dbt-failures/clickhouse/<retained-target>
```

The retry path uses dbt's native `retry` selection from the retained
`manifest.json` and `run_results.json`; it does not rebuild successful nodes.
The target directory is not deleted after retry so the operator can inspect the
result and rerun it if a transient ClickHouse failure recurs.

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
The profile template uses a 900-second ClickHouse HTTP receive timeout because
wide incremental models can legitimately run longer than the adapter's
300-second default. It also disables connection reuse so a client-side timeout
cannot leave a still-running query's dbt session locked for the next model.
Keep these settings aligned in any environment-specific profile; do not solve
the issue by raising the server-wide memory limit.
For a replay check, run the same build a second time and compare the
`wremotely__publication_manifest` row and the serving/company row-hash
fingerprints; identical declared input should produce identical fingerprints.

## Scope and next boundary

This slice proves the separate runtime, source contract, JSON payload
projection, latest-per-candidate source projections, candidate-title
projection, prepared country-eligibility inputs, bounded country-match stages,
candidate country-eligibility rollup, lifecycle latest-observation semantics,
current candidate facts, publication eligibility, publishable job facts,
search facets, serving jobs, companies, the serving-country bridge, and a
deterministic publication manifest. The model and seed tests run against real
raw relations.
The current-candidate-facts model filters each wide source projection through
the bounded changed-candidate key set before joining the retained rows, so an
incremental run does not sort or materialize unrelated source history.
The prepared country relation aligns evidence to the latest classification and
assigns deterministic direction and match-mode fields. The latest-state country
evidence graph is rebuilt atomically as tables on each dbt build: evidence IDs
include classification run identity, and a landing-run watermark cannot remove
old rows when a candidate is reclassified or produces no current evidence.
Raw source history and the latest-per-candidate source projections remain
incremental; only this derived latest-state graph uses a full table rebuild.
Country matching first materializes a narrow evidence projection, then runs
equality, country phrase, country-group phrase, and subdivision phrase stages
independently before the compatibility exact-match relation applies ambiguity
annotation. This keeps large text scans bounded without changing the downstream
relation contract.
Exact matching covers normalized country, group, reviewed location, and
subdivision aliases, and marks evidence that resolves to multiple countries as
ambiguous rather than selecting one silently. Phrase substring matching and
country bridge expansion are represented in the candidate rollup and
serving-country bridge. The intermediate country bridge is a read-only view;
the serving mart materializes only active bridge rows. Country-eligibility
joins use one thread, bounded semi-joins, and external sort/group spill
thresholds; the wide current-candidate query has an 8 GiB per-query ceiling so
it can use additional headroom without changing the server-wide limit. Lifecycle
closure requires one latest `CLOSED` observation or two ordered `TERMINAL`
observations; missing lifecycle history remains open and is never treated as
closure. The on-prem Airflow DAG runs this graph after loading local raw
relations, then exports a READY ClickHouse publication artifact and signals its
content-addressed publication ID over the existing Pub/Sub topic. Lifecycle
recheck assignments are append-only and source-stratified across seven logical
buckets. The assignment model initially backfills jobs with a known posting date
at least 21 days old as well as jobs with no posting date, then adds newly
eligible jobs on later incremental builds without moving existing candidates
between buckets. Unknown dates are retained so lifecycle checks can eventually
close those jobs; known dates remain subject to the 21-day minimum at selection
time. Replaying the same input is idempotent because existing candidate
assignments are preserved by their unique key. The maintenance selector joins
these assignments rather than deriving a transient bucket from the job ID.
ClickHouse grants, the private VPS snapshot-read route, worker cutover, and production
authority remain separate operational work. The five latest-per-candidate
source projections use replay-safe ClickHouse `delete_insert` incremental
materialization. Each run identifies candidates whose source watermark
advanced (plus candidates not yet present in the target), ranks only those
candidates against immutable source history, and replaces their target rows
by `candidate_id`. The classification, extraction, job-facts, and selected-URL
projections rank a narrow key relation first, then join the winning
`ingest_key` back to the full source row; large payload columns therefore do
not participate in the window sort. The current-candidate model also removes
the unused `raw_payload` column before its merge-sort joins, so the wide
candidate assembly does not decompress or sort evidence payloads that are not
part of its output contract. The profile enables the adapter's lightweight-
delete support; model query settings spill large sort/group operations and cap
per-query memory. Use `--full-refresh` when a source contract, model schema, or
title-cleaning rule changes.

The publication graph also consumes the versioned
`wremotely__publication_review` control relation. The private ETL export writes
review candidates as JSONL and Parquet; an operator records content-bound
`held` or `released` decisions in the warehouse control file. dbt treats
`unreviewed`, `pending`, and `held` candidates as not publishable, while
released candidates continue through the normal publication checks. Review
timestamps participate
in the incremental watermark so a decision change reprocesses the affected
candidate without rebuilding unrelated history. The export and decision sync
steps are replay-safe and run before the ClickHouse publication signal.
At the publishable-job boundary, dbt removes bounded repeated trailing company
suffixes from titles across the supported dash and pipe separators. Active rows
must use a supported work-arrangement scope; retained deletion tombstones may
preserve `UNKNOWN` source history because they are emitted only as deletions.

## Contract parity

The ClickHouse graph is intentionally not required to have the same dbt
resource count as the BigQuery graph. The country-evidence implementation is
split into a narrow prepared-input relation, independent match stages, and a
compatibility exact-match relation, while adapter-specific SQL is kept out of
the shared project. Parity means preserving the same
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

The equality-based country-evidence boundary is physically split into four
bounded table relations: country aliases, country-group aliases, reviewed
platform locations, and subdivisions. `int_wremotely__country_eligibility_atomic_matches`
remains as the compatibility union consumed by downstream models. The
cross-stage union remains a read-only view, while compact evidence-level
country-match counts are persisted before the final ambiguity annotation. This
avoids writing or aggregating the wide cross-stage union in one materialization
while preserving the existing downstream relation and match-source contract.
