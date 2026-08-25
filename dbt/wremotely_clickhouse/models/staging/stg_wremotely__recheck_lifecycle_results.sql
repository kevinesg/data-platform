{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='ingest_key',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''), ingest_key)"
) }}

{% set recheck_url_expr = "nullIf(JSONExtractString(payload, 'url'), '')" %}

select
    ingest_key
    , landing_run_id
    , landing_file
    , row_number
    , contract_version as raw_contract_version
    , stage_run_id
    , source_step
    , source_run_id as recheck_run_id
    , source_artifact
    , source_artifact_sha256
    , source_record_index
    , {{ wremotely_canonical_candidate_id("nullIf(JSONExtractString(payload, 'candidate_id'), '')", recheck_url_expr) }} as candidate_id
    , {{ wremotely_canonical_candidate_url(recheck_url_expr) }} as url
    , nullIf(JSONExtractString(payload, 'source_domain'), '') as source_domain
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'checked_at')) as checked_at
    , nullIf(JSONExtractString(payload, 'checker_version'), '') as checker_version
    , nullIf(upper(JSONExtractString(payload, 'page_status')), '') as page_status
    , nullIf(upper(JSONExtractString(payload, 'lifecycle_status')), '') as lifecycle_status
    , nullIf(upper(JSONExtractString(payload, 'lifecycle_signal')), '') as lifecycle_signal
    , if(JSONHas(payload, 'http_status'), JSONExtractInt(payload, 'http_status'), NULL)
        as http_status
    , {{ wremotely_canonical_candidate_url("nullIf(JSONExtractString(payload, 'final_url'), '')") }} as final_url
    , nullIf(JSONExtractRaw(payload, 'redirect_chain'), '') as redirect_chain_json
    , nullIf(JSONExtractString(payload, 'content_type'), '') as content_type
    , if(JSONHas(payload, 'attempt_count'), JSONExtractInt(payload, 'attempt_count'), NULL)
        as attempt_count
    , nullIf(upper(JSONExtractString(payload, 'extractor')), '') as extractor
    , if(JSONHas(payload, 'robots_txt_allowed'), JSONExtractBool(payload, 'robots_txt_allowed'), NULL)
        as robots_txt_allowed
    , nullIf(upper(JSONExtractString(payload, 'robots_txt_status')), '') as robots_txt_status
    , if(JSONHas(payload, 'robots_txt_http_status'), JSONExtractInt(payload, 'robots_txt_http_status'), NULL)
        as robots_txt_http_status
    , nullIf(JSONExtractString(payload, 'robots_txt_url'), '') as robots_txt_url
    , nullIf(JSONExtractString(payload, 'robots_txt_error'), '') as robots_txt_error
    , nullIf(upper(JSONExtractString(payload, 'error_type')), '') as error_type
    , nullIf(JSONExtractString(payload, 'error'), '') as error
    , nullIf(JSONExtractString(payload, 'content_sha256'), '') as content_sha256
    , nullIf(JSONExtractString(payload, 'raw_html_path'), '') as raw_html_path
    , nullIf(JSONExtractString(payload, 'normalized_text_path'), '') as normalized_text_path
    , nullIf(JSONExtractString(payload, 'normalized_text_sha256'), '') as normalized_text_sha256
    , nullIf(JSONExtractString(payload, 'jsonld_path'), '') as jsonld_path
    , nullIf(JSONExtractString(payload, 'jsonld_sha256'), '') as jsonld_sha256
    , nullIf(JSONExtractRaw(payload, 'evidence'), '') as evidence_json
    , payload as raw_payload
from {{ source('wremotely', 'recheck_lifecycle_results') }}
{% if is_incremental() %}
where landing_run_id > (
    select coalesce(max(landing_run_id), '')
    from {{ this }}
)
{% endif %}
