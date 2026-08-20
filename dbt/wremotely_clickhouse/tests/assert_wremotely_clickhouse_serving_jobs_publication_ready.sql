select
    job_id
    , canonical_url
    , title
    , remote_scope
    , country_eligibility_scope
    , eligible_country_codes
    , excluded_country_codes
    , lifecycle_status
    , is_deleted
from {{ ref('wremotely__serving_jobs') }}
where empty(trim(ifNull(canonical_url, '')))
    or empty(trim(ifNull(source_url, '')))
    or empty(trim(ifNull(title, '')))
    or (
        not is_deleted
        and lengthUTF8(trim(ifNull(title, ''))) > 500
    )
    or (
        not is_deleted
        and remote_scope not in ('REMOTE', 'HYBRID', 'ONSITE')
    )
    or country_eligibility_scope not in ('GLOBAL', 'GLOBAL_EXCEPT', 'SPECIFIC')
    or (
        country_eligibility_scope = 'SPECIFIC'
        and empty(eligible_country_codes)
    )
