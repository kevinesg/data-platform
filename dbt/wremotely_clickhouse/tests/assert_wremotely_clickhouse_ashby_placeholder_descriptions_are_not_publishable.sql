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
        or lowerUTF8(replaceAll(trim(replaceRegexpAll(
            ifNull(job_description, '')
            , '<[^>]*>'
            , ''
        )), '&amp;', '&')) = lowerUTF8(trim(ifNull(title, '')))
        or lowerUTF8(trim(replaceRegexpAll(
            ifNull(job_description, '')
            , '<[^>]*>'
            , ''
        ))) in ('test', 'test only', 'testing', 'tbd', 'coming soon')
    )
    and publication_status = 'PUBLISHABLE'
