select
    bridge.job_id
    , bridge.country_code
    , bridge.eligibility_status
from {{ ref('wremotely__job_country_eligibility') }} as bridge
inner join {{ ref('wremotely__serving_jobs') }} as serving
    on bridge.job_id = serving.job_id
where serving.is_deleted
