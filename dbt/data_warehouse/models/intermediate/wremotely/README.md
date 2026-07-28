# wremotely Intermediate Models

This directory turns staged wremotely run history into current candidate facts.

The models keep one latest record per `candidate_id` for each fact type:

- selected job URL
- extraction page result
- extracted job facts
- classification
- country eligibility extraction
- lifecycle recheck

Those five latest-record models are incremental merges keyed by `candidate_id`.
An incremental run first identifies candidates with a newer source event, then
rereads each changed candidate's complete source history before ranking. This
preserves lifecycle predecessor checks and deterministic tie-breaking. New
candidates are included even when their source timestamp predates the current
global watermark.

`int_wremotely__current_candidate_facts` joins those latest records together
and incrementally merges changed candidates.
It prefers extracted job facts for public job title, company, description,
salary, employment type, source dates, and language metadata when those facts
are available. It does not decide what to publish or how to publish it.

`int_wremotely__country_eligibility_evidence` maps raw country and region
evidence from the matching latest classification run to reviewed country and
group taxonomy where possible. Structured atomic values require a complete,
unambiguous alias match; bounded role-level text may contain an alias. Unicode
normalization, unique ISO short-name derivation, and pinned English Unicode CLDR
country display names cover taxonomy variants. Versioned CLDR subdivision names
map bounded role-level location evidence only when the same text identifies the
parent country. A complete ISO subdivision code may map without separate parent
context; standalone code suffixes are not interpreted. Country and subdivision
codes are matched case-sensitively and country codes are accepted only from
atomic fields, not as substrings in free text. A longer subdivision phrase may
embed an otherwise conflicting country name, as in `New Mexico` or `Northern
Ireland`. Standalone subdivision names remain unmatched because CLDR uniqueness
does not distinguish an administrative division from an identically named city,
street, timezone, or colloquial region. Unknown, invalid, ambiguous,
context-conflicting, and unmatched evidence remains visible for QA/RCA instead
of being silently promoted. Physical job-location evidence stays
non-restrictive; applicant eligibility must come from applicant-location or
role-level evidence.
Structured country fields are used only to disambiguate those existing
eligibility signals. An alias observed as a subcountry location under a
different structured country more often than it is observed with its own
country context is left unmatched unless the candidate has structured context
supporting the country interpretation.

`int_wremotely__candidate_country_eligibility` keeps the validated eligibility
contract compact at candidate grain. Global jobs are represented by scope and
exclusions rather than expanded to every country. Explicit restrictive country
evidence takes precedence when a source also contains contradictory global
prose.

`int_wremotely__job_country_eligibility` is the compact bridge for explicit
eligible countries and explicit exclusions.

`int_wremotely__job_publication_status` records whether each current candidate
is publishable, lifecycle-closed, or non-publishable and preserves a bounded
reason for that decision. Source `validThrough` values suppress a job only
after their declared boundary: date-only values remain valid through that UTC
calendar date, while timestamp values use their exact instant.
`int_wremotely__publishable_job_facts` applies that status once so downstream
candidate marts can share the same job grain. It retains lifecycle-closed rows
with `is_deleted` and `_updated_at` metadata; it does not drop them from the
current-state contract.
It also derives nullable conservative company identity fields from source
company name plus source domain. Known non-English rows are excluded from the
serving set for MVP, while unknown-language rows remain eligible. Full job
descriptions are passed through when available and are not truncated. Private
publication holds are evaluated after this dbt graph passes its blocking tests.

`int_wremotely__job_search_facets` normalizes all available source employment
values into a sorted array and matches title/company/description text against a
reviewed tag taxonomy. Unmappable employment values remain available upstream
and produce no public category rather than a vague `OTHER` value.

Country-evidence rollups, publishable filtering, companies, country bridges,
and publication manifests remain complete table calculations because taxonomy,
aggregate, and removal semantics require complete-set reconciliation. The
latest-record, current-candidate, and final serving-job entities are the safe
keyed incremental boundaries in this slice.

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
