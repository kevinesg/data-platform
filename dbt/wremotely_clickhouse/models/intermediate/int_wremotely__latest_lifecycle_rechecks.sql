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

with source_lifecycle_results as (
    select *
    from {{ ref('stg_wremotely__recheck_lifecycle_results') }}
),

changed_candidates as (
    select distinct source.candidate_id
    from source_lifecycle_results as source
    where source.candidate_id is not null
    {% if incremental_watermark_ready %}
        and (
            source.checked_at > (
                select coalesce(max(source_updated_at), toDateTime('1970-01-01 00:00:00'))
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

ranked as (
    select
        *
        , row_number() over (
            partition by candidate_id
            order by
                if(checked_at is null, 1, 0)
                , checked_at desc
                , stage_run_id desc
                , recheck_run_id desc
                , source_record_index desc
        ) as latest_rank
        , lag(lifecycle_status) over (
            partition by candidate_id
            order by
                if(checked_at is null, 1, 0) desc
                , checked_at
                , stage_run_id
                , recheck_run_id
                , source_record_index
        ) as previous_lifecycle_status
    from source_lifecycle_results as source
    inner join changed_candidates as changed
        using (candidate_id)
)

select
    candidate_id
    , url
    , source_domain
    , checked_at as latest_lifecycle_checked_at
    , checker_version as latest_lifecycle_checker_version
    , page_status as latest_lifecycle_page_status
    , lifecycle_status as latest_lifecycle_status
    , previous_lifecycle_status
    , lifecycle_signal as latest_lifecycle_signal
    , http_status as latest_lifecycle_http_status
    , final_url as latest_lifecycle_final_url
    , redirect_chain_json as latest_lifecycle_redirect_chain_json
    , content_type as latest_lifecycle_content_type
    , attempt_count as latest_lifecycle_attempt_count
    , extractor as latest_lifecycle_extractor
    , robots_txt_allowed as latest_lifecycle_robots_txt_allowed
    , robots_txt_status as latest_lifecycle_robots_txt_status
    , robots_txt_http_status as latest_lifecycle_robots_txt_http_status
    , robots_txt_url as latest_lifecycle_robots_txt_url
    , robots_txt_error as latest_lifecycle_robots_txt_error
    , error_type as latest_lifecycle_error_type
    , error as latest_lifecycle_error
    , content_sha256 as latest_lifecycle_content_sha256
    , raw_html_path as latest_lifecycle_raw_html_path
    , normalized_text_path as latest_lifecycle_normalized_text_path
    , normalized_text_sha256 as latest_lifecycle_normalized_text_sha256
    , jsonld_path as latest_lifecycle_jsonld_path
    , jsonld_sha256 as latest_lifecycle_jsonld_sha256
    , evidence_json as latest_lifecycle_evidence_json
    , stage_run_id as latest_lifecycle_stage_run_id
    , recheck_run_id as latest_lifecycle_recheck_run_id
    , source_record_index as latest_lifecycle_source_record_index
    , source_artifact_sha256 as latest_lifecycle_artifact_sha256
    , checked_at as source_updated_at
    , now64(3) as dbt_updated_at
from ranked
where latest_rank = 1
