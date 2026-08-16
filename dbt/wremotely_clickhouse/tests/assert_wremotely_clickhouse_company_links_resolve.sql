select
    serving.job_id
    , serving.company_id
from {{ ref('wremotely__serving_jobs') }} as serving
left join {{ ref('wremotely__companies') }} as companies
    on serving.company_id = companies.company_id
where serving.company_id is not null
    and not serving.is_deleted
    and companies.company_id is null
