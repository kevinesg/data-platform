# wremotely Intermediate Models

This directory turns staged wremotely run history into current candidate facts.

The models keep one latest record per `candidate_id` for each fact type:

- discovery candidate
- selected job URL
- extraction page result
- extracted job facts
- classification
- country eligibility extraction
- pre-publication hold decision
- lifecycle recheck

`int_wremotely__current_candidate_facts` joins those latest records together.
It prefers extracted job facts for public job title, company, description,
salary, employment type, source dates, and language metadata when those facts
are available. It does not decide what to publish or how to publish it.

`int_wremotely__country_eligibility_evidence` maps raw country and region
evidence to reviewed country and group taxonomy where possible. It keeps
unknown, invalid, and unmatched evidence visible for QA/RCA instead of silently
promoting it to a country. Physical job-location evidence stays non-restrictive;
applicant eligibility must come from applicant-location or role-level
eligibility evidence.

`int_wremotely__candidate_country_eligibility` keeps the validated eligibility
contract compact at candidate grain. Global jobs are represented by scope and
exclusions rather than expanded to every country.

`int_wremotely__job_country_eligibility` is the compact bridge for explicit
eligible countries and explicit exclusions.

`int_wremotely__publishable_job_facts` applies the current public-serving
eligibility rules once so downstream serving marts can share the same job grain.
It also derives nullable conservative company identity fields from source
company name plus source domain. Known non-English rows are excluded from the
serving set for MVP, while unknown-language rows remain eligible. Full job
descriptions are passed through when available and are not truncated.
Jobs must also have a current matching pre-publication release decision before
they can enter this serving-prepared set. Held and review-held jobs stay visible
in intermediate QA models but are excluded from serving marts.

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

uv run dbt build \
  --project-dir data_warehouse \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --select path:seeds/wremotely path:models/staging/wremotely path:models/intermediate/wremotely
```
