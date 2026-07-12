{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="candidate_id"
    )
}}

WITH job_facts AS (
    SELECT *
    FROM {{ ref('stg_wremotely__job_facts') }}
),

changed_candidates AS (
    SELECT DISTINCT source.candidate_id
    FROM job_facts AS source
    WHERE source.candidate_id IS NOT NULL
    {% if is_incremental() %}
        AND (
            COALESCE(
                source.record_updated_at
                , source.job_fact_extracted_at
                , source.retrieved_at
            ) > (
                SELECT COALESCE(MAX(source_updated_at), TIMESTAMP '1970-01-01 00:00:00+00')
                FROM {{ this }}
            )
            OR NOT EXISTS (
                SELECT 1
                FROM {{ this }} AS current_candidate
                WHERE current_candidate.candidate_id = source.candidate_id
            )
        )
    {% endif %}
),

ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY candidate_id
            ORDER BY
                CASE WHEN record_updated_at IS NULL THEN 1 ELSE 0 END
                , record_updated_at DESC
                , CASE WHEN job_fact_extracted_at IS NULL THEN 1 ELSE 0 END
                , job_fact_extracted_at DESC
                , stage_run_id DESC
                , job_facts_run_id DESC
                , source_record_index DESC
        ) AS job_fact_rank
    FROM job_facts
    INNER JOIN changed_candidates
        USING (candidate_id)
),

final AS (
    SELECT
        candidate_id
        , url
        , final_url AS latest_job_fact_final_url
        , source_domain AS latest_job_fact_source_domain
        , source_candidate_id AS latest_job_fact_source_candidate_id
        , source_url AS latest_job_fact_source_url
        , source_url_identity AS latest_job_fact_source_url_identity
        , source_type_guess AS latest_job_fact_source_type_guess
        , source_platform_guess AS latest_job_fact_source_platform_guess
        , source_review_status AS latest_job_fact_source_review_status
        , job_fact_status AS latest_job_fact_status
        , page_status AS latest_job_fact_page_status
        , retrieved_at AS latest_job_fact_retrieved_at
        , job_fact_extracted_at AS latest_job_fact_extracted_at
        , job_fact_extractor_version AS latest_job_fact_extractor_version
        , http_status AS latest_job_fact_http_status
        , content_type AS latest_job_fact_content_type
        , source_content_sha256 AS latest_job_fact_source_content_sha256
        , raw_html_path AS latest_job_fact_raw_html_path
        , normalized_text_path AS latest_job_fact_normalized_text_path
        , normalized_text_sha256 AS latest_job_fact_normalized_text_sha256
        , jsonld_path AS latest_job_fact_jsonld_path
        , jsonld_sha256 AS latest_job_fact_jsonld_sha256
        , job_posting_count AS latest_job_fact_job_posting_count
        , jsonld_document_count AS latest_job_fact_jsonld_document_count
        , jsonld_parse_error_count AS latest_job_fact_jsonld_parse_error_count
        , declared_language_raw AS latest_job_fact_declared_language_raw
        , declared_language_tag AS latest_job_fact_declared_language_tag
        , declared_language_source AS latest_job_fact_declared_language_source
        , raw_title_values AS latest_job_fact_raw_title_values
        , raw_title AS latest_job_fact_raw_title
        , raw_company_name_values AS latest_job_fact_raw_company_name_values
        , raw_company_name AS latest_job_fact_raw_company_name
        , raw_description_values AS latest_job_fact_raw_description_values
        , raw_description AS latest_job_fact_raw_description
        , raw_base_salary_values AS latest_job_fact_raw_base_salary_values
        , raw_base_salary_json AS latest_job_fact_raw_base_salary_json
        , raw_estimated_salary_values AS latest_job_fact_raw_estimated_salary_values
        , raw_estimated_salary_json AS latest_job_fact_raw_estimated_salary_json
        , raw_employment_type_values AS latest_job_fact_raw_employment_type_values
        , raw_employment_type AS latest_job_fact_raw_employment_type
        , raw_date_posted_values AS latest_job_fact_raw_date_posted_values
        , raw_date_posted_at AS latest_job_fact_raw_date_posted_at
        , raw_valid_through_values AS latest_job_fact_raw_valid_through_values
        , raw_valid_through_at AS latest_job_fact_raw_valid_through_at
        , raw_job_location_type_values AS latest_job_fact_raw_job_location_type_values
        , raw_job_location_type AS latest_job_fact_raw_job_location_type
        , raw_job_location_values AS latest_job_fact_raw_job_location_values
        , raw_job_location_text AS latest_job_fact_raw_job_location_text
        , raw_applicant_location_requirement_values
            AS latest_job_fact_raw_applicant_location_requirement_values
        , raw_applicant_location_requirement_text
            AS latest_job_fact_raw_applicant_location_requirement_text
        , raw_work_arrangement AS latest_job_fact_raw_work_arrangement
        , raw_work_arrangement_evidence AS latest_job_fact_raw_work_arrangement_evidence
        , source_default_work_arrangement AS latest_job_fact_source_default_work_arrangement
        , source_default_country_eligibility_scope
            AS latest_job_fact_source_default_country_eligibility_scope
        , source_default_country_eligibility_values
            AS latest_job_fact_source_default_country_eligibility_values
        , source_default_country_eligibility_evidence
            AS latest_job_fact_source_default_country_eligibility_evidence
        , record_updated_at AS latest_job_fact_record_updated_at
        , record_updated_by_step AS latest_job_fact_record_updated_by_step
        , stage_run_id AS latest_job_fact_stage_run_id
        , job_facts_run_id AS latest_job_facts_run_id
        , source_record_index AS latest_job_fact_source_record_index
        , source_artifact_sha256 AS latest_job_fact_artifact_sha256
        , COALESCE(record_updated_at, job_fact_extracted_at, retrieved_at)
            AS source_updated_at
        , TIMESTAMP('{{ run_started_at.isoformat() }}') AS dbt_updated_at
    FROM ranked
    WHERE job_fact_rank = 1
)

SELECT *
FROM final
