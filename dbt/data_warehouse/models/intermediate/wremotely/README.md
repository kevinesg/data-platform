# wremotely Intermediate Models

This directory turns staged wremotely run history into current candidate facts.

The models keep one latest record per `candidate_id` for each fact type:

- selected job URL
- extraction page result
- extracted job facts
- classification
- country eligibility extraction
- lifecycle recheck

Those six latest-record models are incremental merges keyed by `candidate_id`.
An incremental run first identifies candidates with a newer source event, then
rereads each changed candidate's complete source history before ranking. This
preserves lifecycle predecessor checks and deterministic tie-breaking. New
candidates are included even when their source timestamp predates the current
global watermark.

`int_wremotely__candidate_job_titles` selects one trustworthy bounded title per
candidate. Precedence is JSON-LD, Open Graph title, Twitter title, then a crawl
link title that the producer explicitly accepted. Every selected value is
limited to 500 characters. Historic rows created before typed crawl-title
fields existed may use their link text only when all new producer fields are
absent and the same 500-character bound passes. New rows require explicit
producer acceptance. Unbounded link text and generic HTML document titles stay
available for diagnosis but are never promoted. A candidate without trustworthy
title evidence remains untitled and fails publication closed.

`int_wremotely__current_candidate_facts` joins the latest records and resolved
title together and incrementally merges changed candidates. It prefers
extracted job facts for company, description, salary, employment type, source
dates, and language metadata when those facts are available. The validated job
identity URL is taken from extraction/job facts and falls back to the originally
selected job URL for historic rows; an arbitrary redirect target is not a job
identity. The first incremental run after these quality columns are introduced
re-merges every current candidate once, then resumes watermark and derived-value
change detection. This model does not decide what to publish or how to publish
it.

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
of being silently promoted. Generic physical job-location evidence remains
non-restrictive for remote and hybrid jobs. For an explicitly classified onsite
job, a structured country or region job location may establish the default
office country when the value is a validated country or subdivision match.
Reviewed ATS platform work-location evidence may restrict any arrangement only
when the producer marks it as restricting; canonical platform roles require a
reviewed platform identity and legacy parser-specific roles require an exact
role/source-system match. Explicitly classified onsite rows may use a mappable
generic structured job location even when an older producer did not set that
flag. Lever and Workday remote or hybrid JSON-LD job locations are the reviewed
generic-role exceptions and still require producer restriction
evidence. A city-only value remains unknown. Other remote and hybrid applicant
eligibility must still come from applicant-location or role-level evidence.
Reviewed exact-location aliases cover measured ATS formats such as full
subdivision names, `city, subdivision-code` composites, and explicitly reviewed
city labels. They match the complete normalized location value, may be scoped
to one ATS platform, and never make standalone code fragments such as `NY`
globally meaningful. Globally unambiguous subdivision names also map through
the complete subdivision taxonomy when they are a terminal component preceded
by another location component in explicitly restricting evidence. Bare
subdivision names and country-colliding names remain unmatched unless reviewed
as exact aliases. Raw location text remains available for later city and region
serving fields. Structured country fields are used only to disambiguate those
existing eligibility signals. An alias observed as a subcountry location under a
different structured country more often than it is observed with its own
country context is left unmatched unless the candidate has structured context
supporting the country interpretation.
Explicit reviewed global location labels are matched separately and produce
global eligibility rather than a synthetic country.

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
Its canonical URL is the validated job identity URL, with the selected source
URL as the only fallback.
When a formerly served row becomes non-publishable, the serving mart retains
its previous descriptive fields on the tombstone and changes lifecycle state
plus watermarks. Active-row title quality gates therefore do not rewrite
historical tombstone content.
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
