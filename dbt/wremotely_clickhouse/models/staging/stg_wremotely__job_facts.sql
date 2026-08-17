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
    , source_run_id as job_facts_run_id
    , source_artifact
    , source_artifact_sha256
    , source_record_index
    , nullIf(JSONExtractString(payload, 'candidate_id'), '') as candidate_id
    , nullIf(JSONExtractString(payload, 'url'), '') as url
    , nullIf(JSONExtractString(payload, 'final_url'), '') as final_url
    , nullIf(JSONExtractString(payload, 'job_identity_url'), '') as job_identity_url
    , nullIf(upper(JSONExtractString(payload, 'final_url_identity_status')), '')
        as final_url_identity_status
    , nullIf(JSONExtractString(payload, 'source_domain'), '') as source_domain
    , nullIf(JSONExtractString(payload, 'source_candidate_id'), '') as source_candidate_id
    , nullIf(JSONExtractString(payload, 'source_url'), '') as source_url
    , nullIf(JSONExtractString(payload, 'source_url_identity'), '') as source_url_identity
    , nullIf(upper(JSONExtractString(payload, 'source_type_guess')), '') as source_type_guess
    , nullIf(JSONExtractString(payload, 'source_platform_guess'), '')
        as source_platform_guess
    , nullIf(upper(JSONExtractString(payload, 'source_review_status')), '')
        as source_review_status
    , nullIf(upper(JSONExtractString(payload, 'job_fact_status')), '') as job_fact_status
    , nullIf(upper(JSONExtractString(payload, 'page_status')), '') as page_status
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'retrieved_at')) as retrieved_at
    , parseDateTimeBestEffortOrNull(
        JSONExtractString(payload, 'job_fact_extracted_at')
    ) as job_fact_extracted_at
    , nullIf(JSONExtractString(payload, 'job_fact_extractor_version'), '')
        as job_fact_extractor_version
    , nullIf(JSONExtractInt(payload, 'http_status'), 0) as http_status
    , nullIf(JSONExtractString(payload, 'content_type'), '') as content_type
    , nullIf(JSONExtractString(payload, 'source_content_sha256'), '')
        as source_content_sha256
    , nullIf(JSONExtractString(payload, 'raw_html_path'), '') as raw_html_path
    , nullIf(JSONExtractString(payload, 'normalized_text_path'), '')
        as normalized_text_path
    , nullIf(JSONExtractString(payload, 'normalized_text_sha256'), '')
        as normalized_text_sha256
    , nullIf(JSONExtractString(payload, 'jsonld_path'), '') as jsonld_path
    , nullIf(JSONExtractString(payload, 'jsonld_sha256'), '') as jsonld_sha256
    , nullIf(JSONExtractInt(payload, 'job_posting_count'), 0) as job_posting_count
    , nullIf(JSONExtractInt(payload, 'jsonld_document_count'), 0) as jsonld_document_count
    , nullIf(JSONExtractInt(payload, 'jsonld_parse_error_count'), 0)
        as jsonld_parse_error_count
    , nullIf(JSONExtractString(payload, 'declared_language_raw'), '') as declared_language_raw
    , nullIf(lowerUTF8(JSONExtractString(payload, 'declared_language_tag')), '')
        as declared_language_tag
    , nullIf(JSONExtractString(payload, 'declared_language_source'), '')
        as declared_language_source
    , JSONExtractArrayRaw(payload, 'raw_title_values') as raw_title_values
    , nullIf(JSONExtractString(payload, 'raw_title_values', 1, 'value'), '') as raw_title
    , JSONExtractArrayRaw(payload, 'raw_company_name_values') as raw_company_name_values
    , nullIf(JSONExtractString(payload, 'raw_company_name_values', 1, 'value'), '')
        as raw_company_name
    , JSONExtractArrayRaw(payload, 'raw_description_values') as raw_description_values
    , nullIf(JSONExtractString(payload, 'raw_description_values', 1, 'value'), '')
        as raw_description
    , JSONExtractArrayRaw(payload, 'raw_base_salary_values') as raw_base_salary_values
    , nullIf(JSONExtractRaw(payload, 'raw_base_salary_values', 1, 'value'), '')
        as raw_base_salary_json
    , JSONExtractArrayRaw(payload, 'raw_estimated_salary_values')
        as raw_estimated_salary_values
    , nullIf(JSONExtractRaw(payload, 'raw_estimated_salary_values', 1, 'value'), '')
        as raw_estimated_salary_json
    , JSONExtractArrayRaw(payload, 'raw_employment_type_values')
        as raw_employment_type_values
    , nullIf(JSONExtractString(payload, 'raw_employment_type_values', 1, 'value'), '')
        as raw_employment_type
    , JSONExtractArrayRaw(payload, 'raw_date_posted_values') as raw_date_posted_values
    , parseDateTimeBestEffortOrNull(
        JSONExtractString(payload, 'raw_date_posted_values', 1, 'value')
    ) as raw_date_posted_at
    , JSONExtractArrayRaw(payload, 'raw_valid_through_values') as raw_valid_through_values
    , parseDateTimeBestEffortOrNull(
        JSONExtractString(payload, 'raw_valid_through_values', 1, 'value')
    ) as raw_valid_through_at
    , JSONExtractArrayRaw(payload, 'raw_job_location_type_values')
        as raw_job_location_type_values
    , nullIf(JSONExtractString(payload, 'raw_job_location_type_values', 1, 'value'), '')
        as raw_job_location_type
    , JSONExtractArrayRaw(payload, 'raw_job_location_values') as raw_job_location_values
    , arrayStringConcat(
        arrayFilter(
            value -> notEmpty(value)
            , arrayDistinct(arrayMap(
                item -> JSONExtractString(item, 'value')
                , JSONExtractArrayRaw(payload, 'raw_job_location_values')
            ))
        )
        , ', '
    ) as raw_job_location_text
    , JSONExtractArrayRaw(payload, 'raw_applicant_location_requirement_values')
        as raw_applicant_location_requirement_values
    , arrayStringConcat(
        arrayFilter(
            value -> notEmpty(value)
            , arrayDistinct(arrayMap(
                item -> JSONExtractString(item, 'value')
                , JSONExtractArrayRaw(payload, 'raw_applicant_location_requirement_values')
            ))
        )
        , ', '
    ) as raw_applicant_location_requirement_text
    , nullIf(upper(JSONExtractString(payload, 'raw_work_arrangement')), '')
        as raw_work_arrangement
    , JSONExtractArrayRaw(payload, 'raw_work_arrangement_evidence')
        as raw_work_arrangement_evidence
    , nullIf(upper(JSONExtractString(payload, 'source_default_work_arrangement')), '')
        as source_default_work_arrangement
    , nullIf(upper(JSONExtractString(payload, 'source_default_country_eligibility_scope')), '')
        as source_default_country_eligibility_scope
    , JSONExtractArrayRaw(payload, 'source_default_country_eligibility_values')
        as source_default_country_eligibility_values
    , JSONExtractArrayRaw(payload, 'source_default_country_eligibility_evidence')
        as source_default_country_eligibility_evidence
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'record_updated_at'))
        as record_updated_at
    , nullIf(upper(JSONExtractString(payload, 'record_updated_by_step')), '')
        as record_updated_by_step
    , payload as raw_payload
from {{ source('wremotely', 'job_facts') }}
{% if is_incremental() %}
where landing_run_id > (
    select coalesce(max(landing_run_id), '')
    from {{ this }}
)
{% endif %}
