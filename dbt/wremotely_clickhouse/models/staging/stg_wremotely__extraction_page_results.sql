{{ config(
    materialized='table',
    order_by='(source_artifact_sha256, source_record_index, ingest_key)'
) }}

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
    , nullIf(JSONExtractString(payload, 'candidate_id'), '') as candidate_id
    , nullIf(JSONExtractString(payload, 'url'), '') as url
    , nullIf(JSONExtractString(payload, 'source_domain'), '') as source_domain
    , nullIf(upper(JSONExtractString(payload, 'status')), '') as page_status
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'retrieved_at')) as retrieved_at
    , nullIf(JSONExtractString(payload, 'final_url'), '') as final_url
    , nullIf(JSONExtractString(payload, 'job_identity_url'), '') as job_identity_url
    , nullIf(upper(JSONExtractString(payload, 'final_url_identity_status')), '')
        as final_url_identity_status
    , JSONExtractArrayRaw(payload, 'redirect_chain') as redirect_chain_json
    , nullIf(JSONExtractString(payload, 'content_type'), '') as content_type
    , nullIf(JSONExtractString(payload, 'extractor'), '') as extractor
    , nullIf(JSONExtractString(payload, 'primary_extractor'), '') as primary_extractor
    , JSONExtractRaw(payload, 'primary_result') as primary_result_json
    , JSONExtractRaw(payload, 'robots_txt') as robots_txt_json
    , nullIf(upper(JSONExtractString(payload, 'robots_txt_status')), '') as robots_txt_status
    , nullIf(JSONExtractString(payload, 'robots_txt_url'), '') as robots_txt_url
    , nullIf(JSONExtractString(payload, 'robots_txt_error'), '') as robots_txt_error
    , nullIf(upper(JSONExtractString(payload, 'error_type')), '') as error_type
    , nullIf(JSONExtractString(payload, 'error'), '') as error
    , nullIf(JSONExtractString(payload, 'content_sha256'), '') as content_sha256
    , nullIf(JSONExtractString(payload, 'raw_html_path'), '') as raw_html_path
    , nullIf(JSONExtractString(payload, 'normalized_text_path'), '') as normalized_text_path
    , nullIf(JSONExtractString(payload, 'normalized_text_sha256'), '')
        as normalized_text_sha256
    , nullIf(JSONExtractString(payload, 'jsonld_path'), '') as jsonld_path
    , nullIf(JSONExtractString(payload, 'jsonld_sha256'), '') as jsonld_sha256
    , payload as raw_payload
from {{ source('wremotely', 'extraction_page_results') }}
