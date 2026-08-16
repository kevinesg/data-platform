select
    bridge.job_id
    , bridge.country_code
    , bridge.eligibility_status
    , serving.country_eligibility_scope
from {{ ref('wremotely__job_country_eligibility') }} as bridge
inner join {{ ref('wremotely__serving_jobs') }} as serving
    on bridge.job_id = serving.job_id
where bridge.eligibility_status = 'EXCLUDED'
    and serving.country_eligibility_scope = 'GLOBAL'
