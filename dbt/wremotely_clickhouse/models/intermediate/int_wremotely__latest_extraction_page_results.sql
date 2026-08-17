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

with source_page_result_keys as (
    select
        ingest_key
        , candidate_id
        , retrieved_at
        , stage_run_id
        , extraction_run_id
        , source_record_index
    from {{ ref('stg_wremotely__extraction_page_results') }}
),

changed_candidates as (
    select distinct source.candidate_id
    from source_page_result_keys as source
    where source.candidate_id is not null
    {% if incremental_watermark_ready %}
        and (
            source.retrieved_at > (
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

ranked_keys as (
    select
        source.ingest_key
        , row_number() over (
            partition by source.candidate_id
            order by
                if(source.retrieved_at is null, 1, 0)
                , source.retrieved_at desc
                , source.stage_run_id desc
                , source.extraction_run_id desc
                , source.source_record_index desc
        ) as page_result_rank
    from source_page_result_keys as source
    inner join changed_candidates as changed
        on source.candidate_id = changed.candidate_id
),

latest_source_rows as (
    select source.*
    from {{ ref('stg_wremotely__extraction_page_results') }} as source
    inner join ranked_keys as latest
        on source.ingest_key = latest.ingest_key
    where latest.page_result_rank = 1
)

select
    candidate_id
    , url
    , source_domain
    , page_status as latest_page_status
    , retrieved_at as latest_retrieved_at
    , http_status as latest_http_status
    , final_url as latest_final_url
    , job_identity_url as latest_job_identity_url
    , final_url_identity_status as latest_final_url_identity_status
    , redirect_chain_json as latest_redirect_chain_json
    , content_type as latest_content_type
    , attempt_count as latest_attempt_count
    , extractor as latest_extractor
    , primary_extractor as latest_primary_extractor
    , primary_result_json as latest_primary_result_json
    , robots_txt_allowed as latest_robots_txt_allowed
    , robots_txt_status as latest_robots_txt_status
    , robots_txt_http_status as latest_robots_txt_http_status
    , robots_txt_url as latest_robots_txt_url
    , robots_txt_error as latest_robots_txt_error
    , robots_txt_json as latest_robots_txt_json
    , error_type as latest_error_type
    , error as latest_error
    , content_sha256 as latest_content_sha256
    , raw_html_path as latest_raw_html_path
    , normalized_text_path as latest_normalized_text_path
    , normalized_text_sha256 as latest_normalized_text_sha256
    , normalized_text_char_count as latest_normalized_text_char_count
    , jsonld_path as latest_jsonld_path
    , jsonld_sha256 as latest_jsonld_sha256
    , jsonld_document_count as latest_jsonld_document_count
    , jsonld_parse_error_count as latest_jsonld_parse_error_count
    , stage_run_id as latest_extraction_stage_run_id
    , extraction_run_id as latest_extraction_run_id
    , source_record_index as latest_extraction_source_record_index
    , source_artifact_sha256 as latest_extraction_artifact_sha256
    , raw_payload
    , retrieved_at as source_updated_at
    , now64(3) as dbt_updated_at
from latest_source_rows
