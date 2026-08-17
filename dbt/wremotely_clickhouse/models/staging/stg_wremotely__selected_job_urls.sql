{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='ingest_key',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''), ingest_key)"
) }}

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
    , nullIf(JSONExtractString(payload, 'candidate_id'), '') as candidate_id
    , nullIf(JSONExtractString(payload, 'url'), '') as url
    , nullIf(JSONExtractString(payload, 'normalized_url'), '') as normalized_url
    , nullIf(JSONExtractString(payload, 'source_job_url_id'), '') as source_job_url_id
    , nullIf(JSONExtractString(payload, 'source_candidate_id'), '') as source_candidate_id
    , nullIf(JSONExtractString(payload, 'source_url'), '') as source_url
    , nullIf(JSONExtractString(payload, 'source_domain'), '') as source_domain
    , nullIf(upper(JSONExtractString(payload, 'source_type_guess')), '') as source_type_guess
    , nullIf(JSONExtractString(payload, 'source_platform_guess'), '')
        as source_platform_guess
    , nullIf(upper(JSONExtractString(payload, 'source_review_status')), '')
        as source_review_status
    , nullIf(upper(JSONExtractString(payload, 'source_default_work_arrangement')), '')
        as source_default_work_arrangement
    , nullIf(JSONExtractString(payload, 'source_crawl_run_id'), '') as source_crawl_run_id
    , nullIf(JSONExtractString(payload, 'source_url_identity'), '') as source_url_identity
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'selected_at')) as selected_at
    , nullIf(JSONExtractString(payload, 'selector_version'), '') as selector_version
    , payload as raw_payload
from {{ source('wremotely', 'selected_job_urls') }}
{% if is_incremental() %}
where landing_run_id > (
    select coalesce(max(landing_run_id), '')
    from {{ this }}
)
{% endif %}
