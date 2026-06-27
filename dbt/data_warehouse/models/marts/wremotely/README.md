# wremotely Mart Models

This directory owns publication-oriented wremotely marts.

`wremotely__serving_jobs` contains the bounded public-safe job rows that a
publisher can copy into the serving store. It excludes raw artifacts, internal
page paths, evidence blobs, provider values, and classifier implementation
details.

`wremotely__publication_manifest` summarizes the current serving snapshot with a
deterministic publication ID and checksum. Airflow must write the environment
publication-control row only after dbt build and blocking tests succeed; this
manifest is the dbt-modeled input to that later control row, not the Pub/Sub
signal itself.

## Validate

From the `dbt/` component directory:

```bash
set -a
. "$DATA_PLATFORM_ENV_FILE"
set +a

export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
if [ -z "${DATA_PLATFORM_DBT_PROFILES_DIR:-}" ]; then
  DATA_PLATFORM_DBT_PROFILES_DIR="$DATA_PLATFORM_SECRETS_DIR/dbt"
fi
export DATA_PLATFORM_DBT_PROFILES_DIR
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$DATA_PLATFORM_DBT_PROFILES_DIR}"
export WREMOTELY_RAW_DATASET="wremotely_raw_dev"
WREMOTELY_DBT_SELECTOR="path:models/staging/wremotely"
WREMOTELY_DBT_SELECTOR="$WREMOTELY_DBT_SELECTOR path:models/intermediate/wremotely"
WREMOTELY_DBT_SELECTOR="$WREMOTELY_DBT_SELECTOR path:models/marts/wremotely"
WREMOTELY_DBT_SELECTOR="$WREMOTELY_DBT_SELECTOR path:tests/wremotely"
export WREMOTELY_DBT_SELECTOR

uv run dbt build \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select $WREMOTELY_DBT_SELECTOR
```
