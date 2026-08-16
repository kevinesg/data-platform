select
    serving.job_id
    , search_tag
from (
    select
        job_id
        , arrayJoin(search_tags) as search_tag
    from {{ ref('wremotely__serving_jobs') }}
) as serving
left join {{ ref('wremotely__search_tags') }} as taxonomy
    on search_tag = taxonomy.tag_code
where taxonomy.tag_code is null
