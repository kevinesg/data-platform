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
    , source_run_id as classification_run_id
    , source_artifact
    , source_artifact_sha256
    , source_record_index
    , nullIf(JSONExtractString(payload, 'candidate_id'), '') as candidate_id
    , nullIf(JSONExtractString(payload, 'url'), '') as url
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'extracted_at')) as extracted_at
    , nullIf(JSONExtractString(payload, 'country_eligibility_extractor_version'), '')
        as country_eligibility_extractor_version
    , nullIf(JSONExtractString(payload, 'classifier_version'), '') as classifier_version
    , nullIf(JSONExtractString(payload, 'target_country'), '') as target_country
    , nullIf(upper(JSONExtractString(payload, 'target_country_code')), '') as target_country_code
    , nullIf(upper(JSONExtractString(payload, 'target_country_eligibility')), '')
        as target_country_eligibility
    , nullIf(upper(JSONExtractString(payload, 'country_eligibility_scope')), '')
        as raw_country_eligibility_scope
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
    , nullIf(JSONExtractString(payload, 'page_extractor'), '') as page_extractor
    , nullIf(upper(JSONExtractString(payload, 'country_field_source')), '') as country_field_source
    , nullIf(upper(JSONExtractString(payload, 'country_field_role')), '') as country_field_role
    , nullIf(upper(JSONExtractString(payload, 'country_field_source_system')), '')
        as country_field_source_system
    , nullIf(JSONExtractString(payload, 'country_field_source_identifier'), '')
        as country_field_source_identifier
    , nullIf(upper(JSONExtractString(payload, 'extraction_method')), '') as extraction_method
    , nullIf(upper(JSONExtractString(payload, 'raw_value_field')), '') as raw_value_field
    , nullIf(JSONExtractString(payload, 'raw_value'), '') as raw_value
    , nullIf(JSONExtractString(payload, 'json_path'), '') as json_path
    , nullIf(JSONExtractString(payload, 'quote'), '') as quote
    , nullIf(upper(JSONExtractString(payload, 'rule')), '') as rule
    , if(JSONHas(payload, 'can_restrict'), JSONExtractBool(payload, 'can_restrict'), NULL)
        as can_restrict
    , nullIf(upper(JSONExtractString(payload, 'llm_runtime')), '') as llm_runtime
    , nullIf(JSONExtractString(payload, 'llm_model'), '') as llm_model
    , nullIf(JSONExtractString(payload, 'llm_prompt_version'), '') as llm_prompt_version
    , nullIf(JSONExtractString(payload, 'llm_response'), '') as llm_response
    , payload as raw_payload
from {{ source('wremotely', 'country_eligibility_extractions') }}
