select
    candidate_id
    , latest_lifecycle_status
    , previous_lifecycle_status
from {{ ref('int_wremotely__job_publication_status') }}
where publication_status = 'CLOSED'
    and not (
        latest_lifecycle_status = 'CLOSED'
        or (
            latest_lifecycle_status = 'TERMINAL'
            and previous_lifecycle_status = 'TERMINAL'
        )
    )
