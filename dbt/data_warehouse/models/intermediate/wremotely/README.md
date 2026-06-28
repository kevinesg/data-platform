# wremotely Intermediate Models

This directory turns staged wremotely run history into current candidate facts.

The models keep one latest record per `candidate_id` for each fact type:

- discovery candidate
- extraction page result
- classification
- lifecycle recheck

`int_wremotely__current_candidate_facts` joins those latest records together.
It does not decide what to publish or how to publish it.

`int_wremotely__publishable_job_facts` applies the current public-serving
eligibility rules once so downstream serving marts can share the same job grain.
It also derives nullable conservative company identity fields from source
company name plus source domain. Publication-control contracts still belong in
marts after current facts are stable.

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

uv run dbt build \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select path:models/staging/wremotely path:models/intermediate/wremotely
```
