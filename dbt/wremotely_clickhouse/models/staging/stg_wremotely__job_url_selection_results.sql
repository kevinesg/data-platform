{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='ingest_key',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''), ingest_key)"
) }}

{% set selected_url_expr = "coalesce(nullIf(JSONExtractString(payload, 'normalized_url'), ''), nullIf(JSONExtractString(payload, 'url'), ''))" %}
{% set selected_platform_expr = "nullIf(JSONExtractString(payload, 'source_platform_guess'), '')" %}

select
    ingest_key
    , landing_run_id
    , landing_file
    , row_number
    , contract_version as raw_contract_version
    , stage_run_id
    , source_step
    , source_run_id as selection_run_id
    , source_artifact
    , source_artifact_sha256
    , source_record_index
    , nullIf(JSONExtractString(payload, 'job_url_id'), '') as job_url_id
    , {{ wremotely_canonical_candidate_id("nullIf(JSONExtractString(payload, 'candidate_id'), '')", selected_url_expr, selected_platform_expr) }} as candidate_id
    , {{ wremotely_canonical_candidate_url("nullIf(JSONExtractString(payload, 'url'), '')", selected_platform_expr) }} as url
    , nullIf(JSONExtractString(payload, 'raw_url'), '') as raw_url
    , {{ wremotely_canonical_candidate_url("nullIf(JSONExtractString(payload, 'normalized_url'), '')", selected_platform_expr) }} as normalized_url
    , nullIf(JSONExtractString(payload, 'source_candidate_id'), '') as source_candidate_id
    , nullIf(JSONExtractString(payload, 'source_url'), '') as source_url
    , nullIf(JSONExtractString(payload, 'source_domain'), '') as source_domain
    , nullIf(upper(JSONExtractString(payload, 'source_candidate_kind')), '')
        as source_candidate_kind
    , nullIf(JSONExtractString(payload, 'source_url_identity'), '') as source_url_identity
    , nullIf(upper(JSONExtractString(payload, 'source_type_guess')), '') as source_type_guess
    , nullIf(JSONExtractString(payload, 'source_platform_guess'), '')
        as source_platform_guess
    , nullIf(upper(JSONExtractString(payload, 'source_review_status')), '')
        as source_review_status
    , nullIf(upper(JSONExtractString(payload, 'source_default_work_arrangement')), '')
        as source_default_work_arrangement
    , nullIf(JSONExtractString(payload, 'source_page_final_url'), '') as source_page_final_url
    , nullIf(JSONExtractInt(payload, 'link_index'), 0) as link_index
    , nullIf(JSONExtractString(payload, 'link_text'), '') as link_text
    , JSONExtractInt(payload, 'link_text_char_count') as link_text_char_count
    , nullIf(JSONExtractString(payload, 'link_title_candidate'), '') as link_title_candidate
    , nullIf(upper(JSONExtractString(payload, 'link_title_candidate_status')), '')
        as link_title_candidate_status
    , nullIf(JSONExtractString(payload, 'link_rel'), '') as link_rel
    , nullIf(upper(JSONExtractString(payload, 'discovery_reason')), '') as discovery_reason
    , nullIf(JSONExtractString(payload, 'crawler_version'), '') as crawler_version
    , nullIf(JSONExtractString(payload, 'source_crawl_run_id'), '') as source_crawl_run_id
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'selected_at')) as selected_at
    , nullIf(JSONExtractString(payload, 'selector_version'), '') as selector_version
    , nullIf(upper(JSONExtractString(payload, 'selection_status')), '') as selection_status
    , coalesce(
        nullIf(upper(JSONExtractString(payload, 'selection_reason')), '')
        , case upper(JSONExtractString(payload, 'selection_status'))
            when 'SELECTED' then 'NEW_URL_SELECTED'
            when 'SKIPPED_KNOWN_URL' then 'KNOWN_URL_IDENTITY'
            when 'SKIPPED_DUPLICATE_URL_IDENTITY' then 'DUPLICATE_WITHIN_SOURCE_CRAWL'
            when 'SKIPPED_SELECTION_LIMIT' then 'SELECTION_LIMIT_REACHED'
            else 'UNKNOWN_SELECTION_STATUS'
        end
    ) as selection_reason
    , JSONExtractBool(payload, 'known_url_match') as known_url_match
    , JSONExtractBool(payload, 'duplicate_url_identity') as duplicate_url_identity
    , nullIf(JSONExtractString(payload, 'duplicate_of_job_url_id'), '')
        as duplicate_of_job_url_id
    , payload as raw_payload
from {{ source('wremotely', 'job_url_selection_results') }}
{% if is_incremental() %}
where landing_run_id > (
    select coalesce(max(landing_run_id), '')
    from {{ this }}
)
{% endif %}
