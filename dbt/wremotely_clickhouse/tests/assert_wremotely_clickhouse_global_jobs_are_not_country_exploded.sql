select
    serving.job_id
    , bridge.country_code
    , bridge.eligibility_status
from {{ ref('wremotely__serving_jobs') }} as serving
inner join {{ ref('wremotely__job_country_eligibility') }} as bridge
    on serving.job_id = bridge.job_id
where serving.country_eligibility_scope = 'GLOBAL'
    and bridge.eligibility_status = 'ELIGIBLE'
