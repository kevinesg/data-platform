select
    candidate_id
    , publication_review_status
    , publication_status
    , publication_status_reason
from {{ ref('int_wremotely__job_publication_status') }}
where publication_review_status in ('pending', 'held')
    and publication_status = 'PUBLISHABLE'
