select
    publishable.job_id
    , publishable.canonical_url
    , status.job_identity_url
    , status.url as selected_url
    , status.latest_final_url
from {{ ref('int_wremotely__publishable_job_facts') }} as publishable
inner join {{ ref('int_wremotely__job_publication_status') }} as status
    on publishable.job_id = status.candidate_id
where ifNull(publishable.canonical_url, '') != ifNull(
    coalesce(nullIf(trim(status.job_identity_url), ''), status.url)
    , ''
)
