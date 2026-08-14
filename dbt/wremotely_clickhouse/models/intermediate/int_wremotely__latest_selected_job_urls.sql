{{ config(materialized='table') }}

with selected_with_result_context as (
    select
        selected.*
        , results.link_text as source_link_text
        , results.link_text_char_count as source_link_text_char_count
        , results.link_title_candidate as source_link_title_candidate
        , results.link_title_candidate_status as source_link_title_candidate_status
        , results.link_rel as source_link_rel
        , results.discovery_reason as source_job_url_discovery_reason
        , results.selection_status
        , results.selection_reason
        , results.known_url_match
        , results.duplicate_url_identity
    from {{ ref('stg_wremotely__selected_job_urls') }} as selected
    left join {{ ref('stg_wremotely__job_url_selection_results') }} as results
        on selected.selection_run_id = results.selection_run_id
        and selected.source_job_url_id = results.job_url_id
    where selected.candidate_id is not null
),

ranked as (
    select
        *
        , row_number() over (
            partition by candidate_id
            order by
                if(selected_at is null, 1, 0)
                , selected_at desc
                , stage_run_id desc
                , selection_run_id desc
                , source_record_index desc
        ) as selected_job_url_rank
    from selected_with_result_context
)

select
    * except (selected_job_url_rank)
    , selected_at as source_updated_at
    , now64(3) as dbt_updated_at
from ranked
where selected_job_url_rank = 1
