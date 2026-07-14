# wremotely Mart Models

This directory owns publication-oriented wremotely marts.

`wremotely__serving_jobs` contains the tested pre-publication job candidates.
It excludes raw artifacts, internal
page paths, evidence blobs, provider values, and classifier implementation
details. Company links are nullable and appear only when dbt can derive a stable
company identity from conservative source evidence. The serving contract includes
full extracted job descriptions when available, salary payloads when available,
employment type, declared language, and source validity timestamps. The private
publication gate applies current hold decisions after dbt tests this relation.
Closed jobs remain in the relation as retained `is_deleted = true` rows so a
publisher can update an existing serving row instead of inferring deletion from
absence. `source_updated_at` is the latest relevant pipeline event.
`dbt_updated_at` is one stable dbt run timestamp assigned to rows selected by
the incremental merge, and `_updated_at` remains its current-publication
compatibility alias. Explicit
closed-page evidence deletes immediately; terminal HTTP evidence requires two
consecutive lifecycle checks.
`publication_hold_content_sha256` hashes policy-relevant job content separately
from `serving_row_sha256`, so lifecycle-only `_updated_at` changes do not force
private model reevaluation.

`wremotely__serving_jobs` is incremental by `job_id`. Incremental models verify
that an existing target has the required source and dbt watermark columns before
using its watermark. A target created by an older contract is processed in full
once, the missing columns are appended, and every existing row receives the
current dbt run timestamp. Later normal builds merge only new jobs or jobs whose
`source_updated_at` advanced. Taxonomy or transformation changes that must
reprocess unchanged source rows still require an explicit full refresh.

`wremotely__companies` contains the public-safe company rows that support
company pages. It includes only companies with currently publishable jobs and a
stable `company_id`. Missing or unknown companies remain missing on job rows
rather than being guessed.

`wremotely__job_country_eligibility` contains the compact country bridge for
explicit eligible countries and explicit exclusions. Global jobs stay compact on
`wremotely__serving_jobs.country_eligibility_scope`; they are not exploded to
one row per country.

`wremotely__publication_manifest` summarizes the current candidate snapshot for
jobs, companies, and country eligibility with a deterministic publication ID and
checksum. Airflow writes the final versioned serving snapshot and ready control
row only after dbt, publication hold, and their blocking checks succeed.

## Validate

From the `dbt/` component directory:

```bash
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$DATA_PLATFORM_SECRETS_DIR/.env}"

test -f "$DATA_PLATFORM_ENV_FILE"
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a

export DATA_PLATFORM_DBT_PROFILES_DIR="${DATA_PLATFORM_DBT_PROFILES_DIR:-$DATA_PLATFORM_SECRETS_DIR/dbt}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$DATA_PLATFORM_DBT_PROFILES_DIR}"
test -s "$DBT_GOOGLE_APPLICATION_CREDENTIALS"

WREMOTELY_DBT_SELECTOR="path:seeds/wremotely"
WREMOTELY_DBT_SELECTOR="$WREMOTELY_DBT_SELECTOR path:models/staging/wremotely"
WREMOTELY_DBT_SELECTOR="$WREMOTELY_DBT_SELECTOR path:models/intermediate/wremotely"
WREMOTELY_DBT_SELECTOR="$WREMOTELY_DBT_SELECTOR path:models/marts/wremotely"
WREMOTELY_DBT_SELECTOR="$WREMOTELY_DBT_SELECTOR path:tests/wremotely"
export WREMOTELY_DBT_SELECTOR

uv run dbt build \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select $WREMOTELY_DBT_SELECTOR
```

Use an explicit full refresh when a taxonomy or transformation change must
reprocess rows whose source watermark did not advance:

```bash
uv run dbt build \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --full-refresh \
  --select $WREMOTELY_DBT_SELECTOR
```

Run the ordinary build once afterward. With no newer source rows, the existing
serving-job `dbt_updated_at` values must remain unchanged.
