with specific_jobs as (
    select job_id
    from {{ ref('wremotely__serving_jobs') }}
    where country_eligibility_scope = 'SPECIFIC'
        and not is_deleted
), eligible_bridge_rows as (
    select distinct job_id
    from {{ ref('wremotely__job_country_eligibility') }}
    where eligibility_status = 'ELIGIBLE'
)
select specific_jobs.job_id
from specific_jobs
left join eligible_bridge_rows
    on specific_jobs.job_id = eligible_bridge_rows.job_id
where eligible_bridge_rows.job_id is null
