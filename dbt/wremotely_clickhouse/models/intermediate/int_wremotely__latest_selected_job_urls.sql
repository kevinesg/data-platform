{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='candidate_id',
    on_schema_change='append_new_columns',
    query_settings={
        'max_bytes_before_external_sort': 268435456,
        'max_bytes_before_external_group_by': 268435456,
        'max_memory_usage': 4294967296,
        'max_threads': 2
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_updated_at', 'dbt_updated_at']) %}

with selected_job_url_keys as (
    select
        ingest_key
        , candidate_id
        , selected_at
        , stage_run_id
        , selection_run_id
        , source_record_index
    from {{ ref('stg_wremotely__selected_job_urls') }}
),

selection_results as (
    select *
    from {{ ref('stg_wremotely__job_url_selection_results') }}
),

changed_candidates as (
    select distinct source.candidate_id
    from selected_job_url_keys as source
    where source.candidate_id is not null
    {% if incremental_watermark_ready %}
        and (
            source.selected_at > (
                select coalesce(max(latest_selected_at), toDateTime('1970-01-01 00:00:00'))
                from {{ this }}
            )
            or not exists (
                select 1
                from {{ this }} as current_candidate
                where current_candidate.candidate_id = source.candidate_id
            )
        )
    {% endif %}
),

ranked_keys as (
    select
        source.ingest_key
        , row_number() over (
            partition by source.candidate_id
            order by
                if(source.selected_at is null, 1, 0)
                , source.selected_at desc
                , source.stage_run_id desc
                , source.selection_run_id desc
                , source.source_record_index desc
        ) as selected_job_url_rank
    from selected_job_url_keys as source
    inner join changed_candidates as changed
        on source.candidate_id = changed.candidate_id
),

latest_selected_job_urls as (
    select source.*
    from {{ ref('stg_wremotely__selected_job_urls') }} as source
    inner join ranked_keys as latest
        on source.ingest_key = latest.ingest_key
    where latest.selected_job_url_rank = 1
),

selected_with_result_context as (
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
    from latest_selected_job_urls as selected
    left join selection_results as results
        on selected.selection_run_id = results.selection_run_id
        and selected.source_job_url_id = results.job_url_id
    where selected.candidate_id is not null
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
from selected_with_result_context
