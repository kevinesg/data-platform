select
    companies.company_id
    , companies.open_job_count
    , countIf(
        notEmpty(ifNull(serving.job_id, ''))
        and not serving.is_deleted
    ) as expected_open_job_count
from {{ ref('wremotely__companies') }} as companies
left join {{ ref('wremotely__serving_jobs') }} as serving
    on companies.company_id = serving.company_id
group by
    companies.company_id
    , companies.open_job_count
having companies.open_job_count != expected_open_job_count
    or expected_open_job_count = 0
