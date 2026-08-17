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

with job_fact_keys as (
    select
        ingest_key
        , candidate_id
        , record_updated_at
        , job_fact_extracted_at
        , retrieved_at
        , stage_run_id
        , job_facts_run_id
        , source_record_index
    from {{ ref('stg_wremotely__job_facts') }}
),

changed_candidates as (
    select distinct source.candidate_id
    from job_fact_keys as source
    where source.candidate_id is not null
    {% if incremental_watermark_ready %}
        and (
            coalesce(source.record_updated_at, source.job_fact_extracted_at, source.retrieved_at) > (
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
                if(source.record_updated_at is null, 1, 0)
                , source.record_updated_at desc
                , if(source.job_fact_extracted_at is null, 1, 0)
                , source.job_fact_extracted_at desc
                , source.stage_run_id desc
                , source.job_facts_run_id desc
                , source.source_record_index desc
        ) as job_fact_rank
    from job_fact_keys as source
    inner join changed_candidates as changed
        on source.candidate_id = changed.candidate_id
),

latest_source_rows as (
    select source.*
    from {{ ref('stg_wremotely__job_facts') }} as source
    inner join ranked_keys as latest
        on source.ingest_key = latest.ingest_key
    where latest.job_fact_rank = 1
)

select
    candidate_id
    , url
    , final_url as latest_job_fact_final_url
    , job_identity_url as latest_job_fact_job_identity_url
    , final_url_identity_status as latest_job_fact_final_url_identity_status
    , source_domain as latest_job_fact_source_domain
    , source_candidate_id as latest_job_fact_source_candidate_id
    , source_url as latest_job_fact_source_url
    , source_url_identity as latest_job_fact_source_url_identity
    , source_type_guess as latest_job_fact_source_type_guess
    , source_platform_guess as latest_job_fact_source_platform_guess
    , source_review_status as latest_job_fact_source_review_status
    , job_fact_status as latest_job_fact_status
    , page_status as latest_job_fact_page_status
    , retrieved_at as latest_job_fact_retrieved_at
    , job_fact_extracted_at as latest_job_fact_extracted_at
    , job_fact_extractor_version as latest_job_fact_extractor_version
    , http_status as latest_job_fact_http_status
    , content_type as latest_job_fact_content_type
    , source_content_sha256 as latest_job_fact_source_content_sha256
    , raw_html_path as latest_job_fact_raw_html_path
    , normalized_text_path as latest_job_fact_normalized_text_path
    , normalized_text_sha256 as latest_job_fact_normalized_text_sha256
    , jsonld_path as latest_job_fact_jsonld_path
    , jsonld_sha256 as latest_job_fact_jsonld_sha256
    , job_posting_count as latest_job_fact_job_posting_count
    , jsonld_document_count as latest_job_fact_jsonld_document_count
    , jsonld_parse_error_count as latest_job_fact_jsonld_parse_error_count
    , declared_language_raw as latest_job_fact_declared_language_raw
    , declared_language_tag as latest_job_fact_declared_language_tag
    , declared_language_source as latest_job_fact_declared_language_source
    , raw_title_values as latest_job_fact_raw_title_values
    , raw_title as latest_job_fact_raw_title
    , raw_company_name_values as latest_job_fact_raw_company_name_values
    , raw_company_name as latest_job_fact_raw_company_name
    , raw_description_values as latest_job_fact_raw_description_values
    , raw_description as latest_job_fact_raw_description
    , raw_base_salary_values as latest_job_fact_raw_base_salary_values
    , raw_base_salary_json as latest_job_fact_raw_base_salary_json
    , raw_estimated_salary_values as latest_job_fact_raw_estimated_salary_values
    , raw_estimated_salary_json as latest_job_fact_raw_estimated_salary_json
    , raw_employment_type_values as latest_job_fact_raw_employment_type_values
    , raw_employment_type as latest_job_fact_raw_employment_type
    , raw_date_posted_values as latest_job_fact_raw_date_posted_values
    , raw_date_posted_at as latest_job_fact_raw_date_posted_at
    , raw_valid_through_values as latest_job_fact_raw_valid_through_values
    , raw_valid_through_at as latest_job_fact_raw_valid_through_at
    , raw_job_location_type_values as latest_job_fact_raw_job_location_type_values
    , raw_job_location_type as latest_job_fact_raw_job_location_type
    , raw_job_location_values as latest_job_fact_raw_job_location_values
    , raw_job_location_text as latest_job_fact_raw_job_location_text
    , raw_applicant_location_requirement_values
        as latest_job_fact_raw_applicant_location_requirement_values
    , raw_applicant_location_requirement_text
        as latest_job_fact_raw_applicant_location_requirement_text
    , raw_work_arrangement as latest_job_fact_raw_work_arrangement
    , raw_work_arrangement_evidence as latest_job_fact_raw_work_arrangement_evidence
    , source_default_work_arrangement as latest_job_fact_source_default_work_arrangement
    , source_default_country_eligibility_scope
        as latest_job_fact_source_default_country_eligibility_scope
    , source_default_country_eligibility_values
        as latest_job_fact_source_default_country_eligibility_values
    , source_default_country_eligibility_evidence
        as latest_job_fact_source_default_country_eligibility_evidence
    , record_updated_at as latest_job_fact_record_updated_at
    , record_updated_by_step as latest_job_fact_record_updated_by_step
    , stage_run_id as latest_job_fact_stage_run_id
    , job_facts_run_id as latest_job_facts_run_id
    , source_record_index as latest_job_fact_source_record_index
    , source_artifact_sha256 as latest_job_fact_artifact_sha256
    , raw_payload
    , coalesce(record_updated_at, job_fact_extracted_at, retrieved_at) as source_updated_at
    , now64(3) as dbt_updated_at
from latest_source_rows
