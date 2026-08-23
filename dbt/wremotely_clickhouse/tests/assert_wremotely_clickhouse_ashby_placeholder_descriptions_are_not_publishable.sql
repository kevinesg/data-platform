select
    candidate_id
    , latest_job_fact_source_platform_guess
    , job_description
    , publication_status
    , publication_status_reason
from {{ ref('int_wremotely__job_publication_status') }}
where lowerUTF8(ifNull(latest_job_fact_source_platform_guess, '')) = 'ashby'
    and (
        empty(trim(replaceRegexpAll(
            ifNull(job_description, '')
            , '<[^>]*>'
            , ''
        )))
        or match(
            trim(replaceRegexpAll(
                ifNull(job_description, '')
                , '<[^>]*>'
                , ''
            ))
            , '^https?://[^[:space:]]+$'
        )
    )
    and publication_status = 'PUBLISHABLE'
