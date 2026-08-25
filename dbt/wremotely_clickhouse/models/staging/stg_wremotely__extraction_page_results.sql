{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='ingest_key',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''), ingest_key)"
) }}

{% set extraction_url_expr = "nullIf(JSONExtractString(payload, 'url'), '')" %}

select
    ingest_key
    , landing_run_id
    , landing_file
    , row_number
    , contract_version as raw_contract_version
    , stage_run_id
    , source_step
    , source_run_id as extraction_run_id
    , source_artifact
    , source_artifact_sha256
    , source_record_index
    , {{ wremotely_canonical_candidate_id("nullIf(JSONExtractString(payload, 'candidate_id'), '')", extraction_url_expr) }} as candidate_id
    , {{ wremotely_canonical_candidate_url(extraction_url_expr) }} as url
    , nullIf(JSONExtractString(payload, 'source_domain'), '') as source_domain
    , nullIf(upper(JSONExtractString(payload, 'status')), '') as page_status
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'retrieved_at')) as retrieved_at
    , nullIf(JSONExtractInt(payload, 'http_status'), 0) as http_status
    , {{ wremotely_canonical_candidate_url("nullIf(JSONExtractString(payload, 'final_url'), '')") }} as final_url
    , {{ wremotely_canonical_candidate_url("nullIf(JSONExtractString(payload, 'job_identity_url'), '')") }} as job_identity_url
    , nullIf(upper(JSONExtractString(payload, 'final_url_identity_status')), '')
        as final_url_identity_status
    , JSONExtractArrayRaw(payload, 'redirect_chain') as redirect_chain_json
    , nullIf(JSONExtractString(payload, 'content_type'), '') as content_type
    , nullIf(JSONExtractInt(payload, 'attempt_count'), 0) as attempt_count
    , nullIf(JSONExtractString(payload, 'extractor'), '') as extractor
    , nullIf(JSONExtractString(payload, 'primary_extractor'), '') as primary_extractor
    , JSONExtractRaw(payload, 'primary_result') as primary_result_json
    , JSONExtractRaw(payload, 'robots_txt') as robots_txt_json
    , nullIf(upper(JSONExtractString(payload, 'robots_txt_status')), '') as robots_txt_status
    , nullIf(JSONExtractInt(payload, 'robots_txt_http_status'), 0)
        as robots_txt_http_status
    , JSONExtractBool(payload, 'robots_txt_allowed') as robots_txt_allowed
    , nullIf(JSONExtractString(payload, 'robots_txt_url'), '') as robots_txt_url
    , nullIf(JSONExtractString(payload, 'robots_txt_error'), '') as robots_txt_error
    , nullIf(upper(JSONExtractString(payload, 'error_type')), '') as error_type
    , nullIf(JSONExtractString(payload, 'error'), '') as error
    , nullIf(JSONExtractString(payload, 'content_sha256'), '') as content_sha256
    , nullIf(JSONExtractString(payload, 'raw_html_path'), '') as raw_html_path
    , nullIf(JSONExtractString(payload, 'normalized_text_path'), '') as normalized_text_path
    , nullIf(JSONExtractString(payload, 'normalized_text_sha256'), '')
        as normalized_text_sha256
    , nullIf(JSONExtractInt(payload, 'normalized_text_char_count'), 0)
        as normalized_text_char_count
    , nullIf(JSONExtractString(payload, 'jsonld_path'), '') as jsonld_path
    , nullIf(JSONExtractString(payload, 'jsonld_sha256'), '') as jsonld_sha256
    , nullIf(JSONExtractInt(payload, 'jsonld_document_count'), 0)
        as jsonld_document_count
    , nullIf(JSONExtractInt(payload, 'jsonld_parse_error_count'), 0)
        as jsonld_parse_error_count
    , payload as raw_payload
from {{ source('wremotely', 'extraction_page_results') }}
{% if is_incremental() %}
where landing_run_id > (
    select coalesce(max(landing_run_id), '')
    from {{ this }}
)
{% endif %}
