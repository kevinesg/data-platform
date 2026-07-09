# wremotely Mart Models

This directory owns publication-oriented wremotely marts.

`wremotely__serving_jobs` contains the bounded public-safe job rows that a
publisher can copy into the serving store. It excludes raw artifacts, internal
page paths, evidence blobs, provider values, and classifier implementation
details. Company links are nullable and appear only when dbt can derive a stable
company identity from conservative source evidence. The serving contract includes
full extracted job descriptions when available, salary payloads when available,
employment type, declared language, and source validity timestamps.

`wremotely__companies` contains the public-safe company rows that support
company pages. It includes only companies with currently publishable jobs and a
stable `company_id`. Missing or unknown companies remain missing on job rows
rather than being guessed.

`wremotely__job_country_eligibility` contains the compact country bridge for
explicit eligible countries and explicit exclusions. Global jobs stay compact on
`wremotely__serving_jobs.country_eligibility_scope`; they are not exploded to
one row per country.

`wremotely__publication_manifest` summarizes the current serving snapshot for
jobs, companies, and country eligibility with a deterministic publication ID and
checksum. Airflow must write the environment publication-control row only after
dbt build and blocking tests succeed; this manifest is the dbt-modeled input to
that later control row, not the Pub/Sub signal itself.

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
export WREMOTELY_RAW_DATASET="${WREMOTELY_RAW_DATASET:-wremotely_raw_dev}"
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
