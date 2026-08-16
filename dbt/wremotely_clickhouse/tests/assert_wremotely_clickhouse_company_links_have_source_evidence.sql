select
    job_id
    , company_id
    , company_name
    , source_domain
from {{ ref('wremotely__serving_jobs') }}
where notEmpty(ifNull(company_id, ''))
    and (
        empty(trim(ifNull(company_name, '')))
        or empty(trim(ifNull(source_domain, '')))
    )
