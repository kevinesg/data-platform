select
    serving.job_id
    , serving.is_deleted
    , status.publication_status
    , status.publication_status_reason
from {{ ref('wremotely__serving_jobs') }} as serving
left join {{ ref('int_wremotely__job_publication_status') }} as status
    on serving.job_id = status.candidate_id
where status.candidate_id is null
    or (status.publication_status = 'PUBLISHABLE' and serving.is_deleted)
    or (
        status.publication_status in ('CLOSED', 'NOT_PUBLISHABLE')
        and not serving.is_deleted
    )
