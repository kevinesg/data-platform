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
absence. Incremental builds use the same tombstone signal when a previously
served job becomes non-publishable. The intermediate publication-status model
keeps classification suppression distinct from confirmed lifecycle closure in
the warehouse and also suppresses elapsed source `validThrough` boundaries.
`source_updated_at` is the latest relevant pipeline event.
`dbt_updated_at` is one stable dbt run timestamp assigned to rows selected by
the incremental merge, and `_updated_at` remains its current-publication
compatibility alias. Explicit
closed-page evidence deletes immediately; terminal HTTP evidence requires two
consecutive lifecycle checks.
`publication_hold_content_sha256` hashes policy-relevant job content separately
from `serving_row_sha256`, so lifecycle-only `_updated_at` changes do not force
private model reevaluation.

`wremotely__serving_jobs` is incremental by `job_id`. Incremental models verify
that an existing target has the columns required for content comparison. A
target created by an older contract is processed in full once and receives the
missing columns. Later builds hash the complete public serving content and merge
new, changed, closed, suppressed, or reactivated rows. This detects taxonomy and
transformation changes even when the source watermark does not advance.
Publication-status changes can also suppress or reactivate a row without a new
source event.

The model sets `full_refresh: false` so a command-level `--full-refresh` cannot
erase the prior serving state needed to emit suppression tombstones. Do not
override this protection or drop the target as a rebuild shortcut. If the target
is missing or damaged, recover its prior state before publishing a replacement
snapshot.

`wremotely__companies` contains the public-safe company rows that support
company pages. It includes only companies with currently publishable jobs and a
stable `company_id`. Missing or unknown companies remain missing on job rows
rather than being guessed. Public company fields aggregate only active jobs,
while `dbt_updated_at` includes changes from linked tombstones so removing a job
advances the company publication watermark.

`wremotely__job_country_eligibility` contains the compact country bridge for
explicit eligible countries and explicit exclusions for active jobs. Deleted
job tombstones remain only in `wremotely__serving_jobs`; they do not retain
country bridge rows. Global jobs stay compact on
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

Historical classification reconciliation must use this ordinary incremental
build. Do not add `--full-refresh`: the existing serving rows are the state
against which the model emits suppression tombstones.

Broad upstream rebuilds may use `--full-refresh`; the protected serving-jobs
model ignores that command-level flag and still reconciles against its prior
state. Run the ordinary build once more afterward:

```bash
uv run dbt build \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select $WREMOTELY_DBT_SELECTOR
```

With no intervening source or transformation changes, the second serving-jobs
merge must process zero rows and existing `dbt_updated_at` values must remain
unchanged.
