{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='ingest_key',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''), ingest_key)"
) }}

{% set classification_url_expr = "nullIf(JSONExtractString(payload, 'url'), '')" %}

select
    ingest_key
    , landing_run_id
    , landing_file
    , row_number
    , contract_version as raw_contract_version
    , stage_run_id
    , source_step
    , source_run_id as classification_run_id
    , source_artifact
    , source_artifact_sha256
    , source_record_index
    , {{ wremotely_canonical_candidate_id("nullIf(JSONExtractString(payload, 'candidate_id'), '')", classification_url_expr) }} as candidate_id
    , {{ wremotely_canonical_candidate_url(classification_url_expr) }} as url
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'classified_at')) as classified_at
    , nullIf(JSONExtractString(payload, 'classifier_version'), '') as classifier_version
    , nullIf(JSONExtractString(payload, 'model'), '') as model
    , nullIf(upper(JSONExtractString(payload, 'classification_status')), '')
        as classification_status
    , nullIf(upper(JSONExtractString(payload, 'job_posting_type')), '') as job_posting_type
    , nullIf(upper(JSONExtractString(payload, 'job_status')), '') as job_status
    , nullIf(upper(JSONExtractString(payload, 'remote_scope')), '') as remote_scope
    , case upper(JSONExtractString(payload, 'country_eligibility_scope'))
        when 'TARGET_COUNTRY' then 'SPECIFIC'
        when 'RESTRICTED' then 'SPECIFIC'
        when '' then 'UNKNOWN'
        else upper(JSONExtractString(payload, 'country_eligibility_scope'))
    end as country_eligibility_scope
    , nullIf(JSONExtractString(payload, 'target_country'), '') as target_country
    , nullIf(upper(JSONExtractString(payload, 'target_country_code')), '') as target_country_code
    , nullIf(upper(JSONExtractString(payload, 'target_country_eligibility')), '')
        as target_country_eligibility
    , nullIf(upper(JSONExtractString(payload, 'serving_decision')), '') as serving_decision
    , nullIf(JSONExtractString(payload, 'source_candidate_id'), '') as source_candidate_id
    , nullIf(JSONExtractString(payload, 'source_url'), '') as source_url
    , nullIf(JSONExtractString(payload, 'source_url_identity'), '') as source_url_identity
    , nullIf(upper(JSONExtractString(payload, 'source_type_guess')), '') as source_type_guess
    , nullIf(JSONExtractString(payload, 'source_platform_guess'), '')
        as source_platform_guess
    , nullIf(upper(JSONExtractString(payload, 'source_review_status')), '')
        as source_review_status
    , nullIf(upper(JSONExtractString(payload, 'source_default_work_arrangement')), '')
        as source_default_work_arrangement
    , nullIf(JSONExtractString(payload, 'source_content_sha256'), '') as source_content_sha256
    , nullIf(JSONExtractString(payload, 'normalized_text_sha256'), '') as normalized_text_sha256
    , nullIf(JSONExtractString(payload, 'jsonld_sha256'), '') as jsonld_sha256
    , nullIf(JSONExtractString(payload, 'declared_language_raw'), '') as declared_language_raw
    , nullIf(JSONExtractString(payload, 'declared_language_tag'), '') as declared_language_tag
    , nullIf(JSONExtractString(payload, 'declared_language_source'), '')
        as declared_language_source
    , JSONExtractArrayRaw(payload, 'evidence') as evidence_json
    , payload as raw_payload
from {{ source('wremotely', 'classification_classifications') }}
{% if is_incremental() %}
where landing_run_id > (
    select coalesce(max(landing_run_id), '')
    from {{ this }}
)
{% endif %}
