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
    candidate_id
    , url
    , normalized_url
    , source_job_url_id
    , source_candidate_id
    , source_url
    , source_domain
    , source_crawl_run_id
    , source_url_identity
    , source_type_guess
    , source_platform_guess
    , source_review_status
    , source_default_work_arrangement
    , source_link_text
    , source_link_text_char_count
    , source_link_title_candidate
    , source_link_title_candidate_status
    , source_link_rel
    , source_job_url_discovery_reason
    , selection_status
    , selection_reason
    , known_url_match
    , duplicate_url_identity
    , selected_at as latest_selected_at
    , selector_version as latest_selector_version
    , stage_run_id as latest_selection_stage_run_id
    , selection_run_id as latest_selection_run_id
    , source_record_index as latest_selection_source_record_index
    , source_artifact_sha256 as latest_selection_artifact_sha256
    , raw_payload
    , selected_at as source_updated_at
    , now64(3) as dbt_updated_at
from ranked
where selected_job_url_rank = 1
